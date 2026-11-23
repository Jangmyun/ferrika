//! Ferrika's governance layer on top of `ferrika-core`: the signed company
//! policy that gates what a command may do ([`policy`]), the hash-chained
//! local audit log ([`audit`]), its OS anchors ([`audit_anchor`]) and the
//! SIEM forwarding ([`audit_forward`]).

pub mod audit;
pub mod audit_anchor;
pub mod audit_forward;
mod file_security;
pub mod policy;
mod policy_dir_perm;
// `test-util` is for tests: test_policy builds policies that claim to be
// verified without a signature. An optimized build must not carry it, so the
// feature fails to compile without debug assertions.
#[cfg(all(feature = "test-util", not(debug_assertions)))]
compile_error!(
    "the `test-util` feature is for tests and debug builds only; shipped (release) builds must not enable it"
);
#[cfg(any(test, feature = "test-util"))]
pub mod test_policy;
