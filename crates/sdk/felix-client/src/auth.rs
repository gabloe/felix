//! Token sources for stream authentication.
//!
//! Each stream authenticates when it opens, and a client keeps opening streams
//! after connect (subscriptions, watches, group requests, reconnects). Access
//! tokens last 900s by default, so the client asks a [`TokenProvider`] for the
//! current token each time instead of holding the one it connected with.
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;

/// The future a [`TokenProvider`] answers with.
pub type TokenFuture<'a> = Pin<Box<dyn Future<Output = Result<String>> + Send + 'a>>;

/// Supplies the token a stream authenticates with.
///
/// Called on every stream open, so implementations should cache.
/// [`RefreshingToken`] is the usual one.
pub trait TokenProvider: Send + Sync {
    /// The token to present now.
    fn token(&self) -> TokenFuture<'_>;

    /// The broker refused `rejected`. The client then asks for a token again
    /// and retries once if it gets a different one, so a caching provider
    /// should drop `rejected` here.
    ///
    /// The token is passed in because several streams can be refused at once,
    /// and a late call shouldn't throw away a token fetched since.
    fn invalidate(&self, rejected: &str) {
        let _ = rejected;
    }
}

/// The fixed [`crate::ClientConfig::auth_token`].
pub(crate) struct StaticToken(pub(crate) String);

impl TokenProvider for StaticToken {
    fn token(&self) -> TokenFuture<'_> {
        let token = self.0.clone();
        Box::pin(async move { Ok(token) })
    }
}

type Fetch = Box<dyn Fn() -> TokenFuture<'static> + Send + Sync>;

/// A [`TokenProvider`] that replaces its token before it expires.
///
/// `fetch` gets a new token, usually by calling the control plane's
/// `/token/refresh`. It runs on first use and again once two thirds of the
/// current token's life has passed, going by its `exp` claim. A token without
/// a readable `exp` is kept until the broker refuses it.
///
/// If a fetch fails while the current token is still valid, the current token
/// is returned and the fetch is tried again a few seconds later. Concurrent
/// callers wait on the same fetch.
pub struct RefreshingToken {
    fetch: Fetch,
    state: Mutex<Option<Held>>,
    fetching: tokio::sync::Mutex<()>,
}

struct Held {
    token: String,
    /// Unix seconds. `None` means the token did not say when it expires.
    expires_at: Option<i64>,
    /// When to stop answering from this token and fetch again.
    refresh_at: Option<i64>,
}

/// Delay before retrying a failed fetch, so every stream open doesn't trigger
/// one.
const RETRY_AFTER: Duration = Duration::from_secs(5);

impl RefreshingToken {
    /// Fetch tokens with `fetch`, starting with nothing held.
    pub fn new<F, Fut>(fetch: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String>> + Send + 'static,
    {
        Self {
            fetch: Box::new(move || Box::pin(fetch())),
            state: Mutex::new(None),
            fetching: tokio::sync::Mutex::new(()),
        }
    }

    /// Start from `initial`, a token the caller already has, and fetch its
    /// replacements with `fetch`.
    pub fn with_initial<F, Fut>(initial: impl Into<String>, fetch: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String>> + Send + 'static,
    {
        let token = Self::new(fetch);
        *token.state.lock().expect("token state poisoned") = Some(Held::new(initial.into()));
        token
    }

    /// The held token, if it is not yet due for replacement.
    fn fresh(&self, now: i64) -> Option<String> {
        let state = self.state.lock().expect("token state poisoned");
        let held = state.as_ref()?;
        match held.refresh_at {
            Some(at) if now >= at => None,
            _ => Some(held.token.clone()),
        }
    }

    async fn current(&self) -> Result<String> {
        if let Some(token) = self.fresh(now_secs()) {
            return Ok(token);
        }
        let _fetching = self.fetching.lock().await;
        // Another caller may have fetched while this one waited.
        if let Some(token) = self.fresh(now_secs()) {
            return Ok(token);
        }
        match (self.fetch)().await {
            Ok(token) => {
                *self.state.lock().expect("token state poisoned") = Some(Held::new(token.clone()));
                Ok(token)
            }
            Err(err) => {
                let now = now_secs();
                let mut state = self.state.lock().expect("token state poisoned");
                match state.as_mut() {
                    Some(held) if held.expires_at.is_some_and(|exp| now < exp) => {
                        tracing::warn!(
                            error = %err,
                            remaining_secs = held.expires_at.map(|exp| exp - now),
                            "could not refresh the client token; using the current one",
                        );
                        held.refresh_at = Some(now + RETRY_AFTER.as_secs() as i64);
                        Ok(held.token.clone())
                    }
                    _ => Err(err.context("fetch client token")),
                }
            }
        }
    }
}

impl TokenProvider for RefreshingToken {
    fn token(&self) -> TokenFuture<'_> {
        Box::pin(self.current())
    }

    fn invalidate(&self, rejected: &str) {
        let mut state = self.state.lock().expect("token state poisoned");
        if state.as_ref().is_some_and(|held| held.token == rejected) {
            *state = None;
        }
    }
}

impl Held {
    fn new(token: String) -> Self {
        let expires_at = read_exp(&token);
        let refresh_at = expires_at.map(|exp| refresh_at(now_secs(), exp));
        Self {
            token,
            expires_at,
            refresh_at,
        }
    }
}

/// Two thirds of the way to `exp`, leaving the last third for retries.
fn refresh_at(now: i64, exp: i64) -> i64 {
    now + exp.saturating_sub(now).max(0) * 2 / 3
}

/// A JWT's `exp` claim, unverified. It only decides when to refresh; the
/// broker does the real check.
fn read_exp(token: &str) -> Option<i64> {
    use base64::Engine;

    #[derive(serde::Deserialize)]
    struct Claims {
        exp: i64,
    }

    let mut parts = token.split('.');
    let (_header, payload, _signature) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    serde_json::from_slice::<Claims>(&decoded)
        .ok()
        .map(|claims| claims.exp)
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn jwt(exp: i64) -> String {
        let encode = |value: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value);
        format!(
            "{}.{}.sig",
            encode(r#"{"alg":"EdDSA"}"#),
            encode(&format!(r#"{{"exp":{exp},"tid":"t1"}}"#))
        )
    }

    /// A provider whose fetches are numbered, so a test can tell them apart.
    fn counting(
        make: impl Fn(usize) -> Result<String> + Send + Sync + 'static,
    ) -> (RefreshingToken, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let make = Arc::new(make);
        let provider = RefreshingToken::new(move || {
            let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
            let make = Arc::clone(&make);
            async move { make(n) }
        });
        (provider, calls)
    }

    #[test]
    fn exp_is_read_from_a_jwt_and_nothing_else() {
        assert_eq!(read_exp(&jwt(1_900_000_000)), Some(1_900_000_000));
        assert_eq!(read_exp("demo-token"), None);
        assert_eq!(read_exp("a.b"), None);
        assert_eq!(read_exp("a.b.c.d"), None);
    }

    #[test]
    fn refresh_is_due_two_thirds_through_the_remaining_life() {
        assert_eq!(refresh_at(1_000, 1_900), 1_600);
        // Already expired: due now.
        assert_eq!(refresh_at(1_000, 900), 1_000);
    }

    /// A token that is still valid but past the point where it should be
    /// replaced.
    fn due(token: String) -> Held {
        Held {
            token,
            expires_at: Some(now_secs() + 60),
            refresh_at: Some(now_secs() - 1),
        }
    }

    #[tokio::test]
    async fn a_token_with_life_left_is_reused() -> Result<()> {
        let (provider, calls) = counting(|_| Ok(jwt(now_secs() + 900)));
        let first = provider.token().await?;
        let second = provider.token().await?;
        assert_eq!(first, second);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_token_past_two_thirds_of_its_life_is_replaced() -> Result<()> {
        let (provider, calls) = counting(|n| Ok(format!("{}-{n}", jwt(now_secs() + 900))));
        *provider.state.lock().unwrap() = Some(due(jwt(now_secs() + 60)));
        let token = provider.token().await?;
        assert!(
            token.ends_with("-1"),
            "expected a fetched token, got {token}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_failed_fetch_falls_back_to_a_token_that_is_still_valid() -> Result<()> {
        let (provider, calls) = counting(|_| Err(anyhow::anyhow!("control plane down")));
        let held = jwt(now_secs() + 60);
        *provider.state.lock().unwrap() = Some(due(held.clone()));
        assert_eq!(provider.token().await?, held);
        // And does not fetch again on the very next call.
        assert_eq!(provider.token().await?, held);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_failed_fetch_with_nothing_valid_held_is_an_error() {
        let (provider, _) = counting(|_| Err(anyhow::anyhow!("control plane down")));
        *provider.state.lock().unwrap() = Some(Held::new(jwt(now_secs() - 1)));
        assert!(provider.token().await.is_err());
    }

    #[tokio::test]
    async fn invalidate_drops_only_the_token_it_names() -> Result<()> {
        let (provider, calls) = counting(|n| Ok(format!("opaque-{n}")));
        let first = provider.token().await?;
        provider.invalidate(&first);
        let second = provider.token().await?;
        assert_ne!(first, second);
        // A late report about the first token must not discard the second.
        provider.invalidate(&first);
        assert_eq!(provider.token().await?, second);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn an_initial_token_is_used_before_any_fetch() -> Result<()> {
        let provider = RefreshingToken::with_initial("seed", || async {
            anyhow::bail!("should not be called")
        });
        assert_eq!(provider.token().await?, "seed");
        Ok(())
    }
}
