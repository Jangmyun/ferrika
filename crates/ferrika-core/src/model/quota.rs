//! Client quota entities.

/// Who a client quota applies to: `(entity type, name)` pairs such as
/// `("user", Some("alice"))`; a `None` name is that type's default.
pub type QuotaEntity = Vec<(String, Option<String>)>;

/// `user=alice, client-id=<default>`.
pub fn quota_entity_label(entity: &QuotaEntity) -> String {
    entity
        .iter()
        .map(|(kind, name)| format!("{kind}={}", name.as_deref().unwrap_or("<default>")))
        .collect::<Vec<_>>()
        .join(", ")
}
