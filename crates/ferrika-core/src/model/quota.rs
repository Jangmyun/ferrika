//! Client quota entities.

/// Who a client quota applies to: `(entity type, name)` pairs such as
/// `("user", Some("alice"))`; a `None` name is that type's default.
pub type QuotaEntity = Vec<(String, Option<String>)>;

/// `entity` with its components in the usual order (user, client-id, ip,
/// then any other type by name). The broker lists them in its own map
/// order, so two descriptions of one entity may differ only in order.
pub fn canonical_quota_entity(mut entity: QuotaEntity) -> QuotaEntity {
    fn rank(kind: &str) -> usize {
        ["user", "client-id", "ip"]
            .iter()
            .position(|k| *k == kind)
            .unwrap_or(3)
    }
    entity.sort_by(|(a, _), (b, _)| rank(a).cmp(&rank(b)).then_with(|| a.cmp(b)));
    entity
}

/// Whether `a` and `b` name the same entity, whatever their order.
pub fn same_quota_entity(a: &QuotaEntity, b: &QuotaEntity) -> bool {
    canonical_quota_entity(a.clone()) == canonical_quota_entity(b.clone())
}

/// `user=alice, client-id=<default>`.
/// Components are in [`canonical_quota_entity`] order, so the label (also
/// the history resource key) is the same however the entity was listed.
pub fn quota_entity_label(entity: &QuotaEntity) -> String {
    canonical_quota_entity(entity.clone())
        .iter()
        .map(|(kind, name)| format!("{kind}={}", name.as_deref().unwrap_or("<default>")))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entity(parts: &[(&str, &str)]) -> QuotaEntity {
        parts
            .iter()
            .map(|(k, v)| (k.to_string(), Some(v.to_string())))
            .collect()
    }

    #[test]
    fn entities_compare_whatever_the_component_order() {
        let cli = entity(&[("user", "a"), ("client-id", "app")]);
        let broker = entity(&[("client-id", "app"), ("user", "a")]);
        assert!(same_quota_entity(&cli, &broker));
        assert_eq!(quota_entity_label(&broker), "user=a, client-id=app");
        assert_eq!(canonical_quota_entity(broker), cli);
        assert!(!same_quota_entity(&cli, &entity(&[("user", "a")])));
        assert!(!same_quota_entity(
            &cli,
            &entity(&[("user", "app"), ("client-id", "a")])
        ));
    }
}
