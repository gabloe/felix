/// Authorization primitives for simple role-based checks. NOTE: This is a placeholder implementation and
/// should be replaced with a more robust solution in the future.
///
/// ```
/// use felix_authz::{Permission, Policy, Role};
///
/// let policy = Policy {
///     roles: vec![Role::new("writer", vec![Permission::Publish])],
/// };
/// assert!(policy.allows(Permission::Publish));
/// assert!(!policy.allows(Permission::Manage));
/// ```
// Authorization primitives for simple role-based checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Permission {
    Publish,
    Subscribe,
    Manage,
}

#[derive(Debug, Clone)]
pub struct Role {
    pub name: String,
    pub permissions: Vec<Permission>,
}

impl Role {
    // Collect permissions under a named role.
    pub fn new(name: impl Into<String>, permissions: Vec<Permission>) -> Self {
        Self {
            name: name.into(),
            permissions,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Policy {
    pub roles: Vec<Role>,
}

impl Policy {
    pub fn allows(&self, permission: Permission) -> bool {
        // Any role with the permission grants access.
        self.roles
            .iter()
            .any(|role| role.permissions.contains(&permission))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_allows_permission() {
        // Writer role should grant publish.
        let policy = Policy {
            roles: vec![Role::new("writer", vec![Permission::Publish])],
        };
        assert!(policy.allows(Permission::Publish));
    }

    #[test]
    fn policy_denies_missing_permission() {
        // Missing permission should remain denied.
        let policy = Policy {
            roles: vec![Role::new("reader", vec![Permission::Subscribe])],
        };
        assert!(!policy.allows(Permission::Manage));
    }

    #[test]
    fn policy_allows_when_any_role_matches() {
        let policy = Policy {
            roles: vec![
                Role::new("reader", vec![Permission::Subscribe]),
                Role::new("writer", vec![Permission::Publish]),
            ],
        };
        assert!(policy.allows(Permission::Publish));
    }
}
