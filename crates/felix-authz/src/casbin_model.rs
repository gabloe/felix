use casbin::prelude::DefaultModel;

const MODEL: &str = r#"
[request_definition]
r = sub, dom, obj, act

[policy_definition]
p = sub, dom, obj, act

[role_definition]
g = _, _, _

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = g(r.sub, p.sub, r.dom) && r.dom == p.dom && keyMatch2(r.obj, p.obj) && r.act == p.act
"#;

pub fn casbin_model_string() -> &'static str {
    MODEL
}

pub async fn casbin_model() -> DefaultModel {
    DefaultModel::from_str(MODEL)
        .await
        .expect("casbin model must be valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use casbin::Model;

    #[test]
    fn model_string_contains_matcher() {
        let model = casbin_model_string();
        assert!(model.contains("keyMatch2"));
        assert!(model.contains("request_definition"));
    }

    #[tokio::test]
    async fn model_builds() {
        let model = casbin_model().await;
        let data = model.get_model();
        assert!(data.contains_key("r"));
        assert!(data.contains_key("p"));
        assert!(data.contains_key("g"));
    }
}
