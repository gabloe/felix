// Owned map keys plus borrowed `*Ref` twins used for allocation-free lookups.
// The `Equivalent` impls let `hashbrown` probe an owned-key map with borrowed data.

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct NamespaceKey {
    pub(crate) tenant_id: String,
    pub(crate) namespace: String,
}

impl NamespaceKey {
    pub fn new(tenant_id: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            namespace: namespace.into(),
        }
    }
}

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub(crate) struct NamespaceKeyRef<'a> {
    pub(crate) tenant_id: &'a str,
    pub(crate) namespace: &'a str,
}

impl<'a> NamespaceKeyRef<'a> {
    pub(crate) fn new(tenant_id: &'a str, namespace: &'a str) -> Self {
        Self {
            tenant_id,
            namespace,
        }
    }
}

impl<'a> hashbrown::Equivalent<NamespaceKey> for NamespaceKeyRef<'a> {
    fn equivalent(&self, key: &NamespaceKey) -> bool {
        self.tenant_id == key.tenant_id && self.namespace == key.namespace
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct StreamKey {
    pub(crate) tenant_id: String,
    pub(crate) namespace: String,
    pub(crate) stream: String,
}

impl StreamKey {
    pub fn new(
        tenant_id: impl Into<String>,
        namespace: impl Into<String>,
        stream: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            namespace: namespace.into(),
            stream: stream.into(),
        }
    }
}

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub(crate) struct StreamKeyRef<'a> {
    pub(crate) tenant_id: &'a str,
    pub(crate) namespace: &'a str,
    pub(crate) stream: &'a str,
}

impl<'a> StreamKeyRef<'a> {
    pub(crate) fn new(tenant_id: &'a str, namespace: &'a str, stream: &'a str) -> Self {
        Self {
            tenant_id,
            namespace,
            stream,
        }
    }
}

impl<'a> hashbrown::Equivalent<StreamKey> for StreamKeyRef<'a> {
    fn equivalent(&self, key: &StreamKey) -> bool {
        self.tenant_id == key.tenant_id
            && self.namespace == key.namespace
            && self.stream == key.stream
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CacheKey {
    pub(crate) tenant_id: String,
    pub(crate) namespace: String,
    pub(crate) cache: String,
}

impl CacheKey {
    pub fn new(
        tenant_id: impl Into<String>,
        namespace: impl Into<String>,
        cache: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            namespace: namespace.into(),
            cache: cache.into(),
        }
    }
}

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub(crate) struct CacheKeyRef<'a> {
    pub(crate) tenant_id: &'a str,
    pub(crate) namespace: &'a str,
    pub(crate) cache: &'a str,
}

impl<'a> CacheKeyRef<'a> {
    pub(crate) fn new(tenant_id: &'a str, namespace: &'a str, cache: &'a str) -> Self {
        Self {
            tenant_id,
            namespace,
            cache,
        }
    }
}

impl<'a> hashbrown::Equivalent<CacheKey> for CacheKeyRef<'a> {
    fn equivalent(&self, key: &CacheKey) -> bool {
        self.tenant_id == key.tenant_id
            && self.namespace == key.namespace
            && self.cache == key.cache
    }
}
