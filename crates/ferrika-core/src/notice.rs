//! Warnings for the person running the tool that the library cannot return
//! as an error: a company policy about to expire, an audit entry kept only
//! locally. Without a handler they go to `tracing` at WARN level; an
//! application that shows them its own way installs one at start-up.

use std::sync::OnceLock;

type Handler = Box<dyn Fn(&str) + Send + Sync>;

static HANDLER: OnceLock<Handler> = OnceLock::new();

/// Sends every later warning to `handler`. Only the first call takes
/// effect; it returns `false` when a handler was already set.
pub fn set_handler(handler: impl Fn(&str) + Send + Sync + 'static) -> bool {
    HANDLER.set(Box::new(handler)).is_ok()
}

/// Shows `message` through the installed handler, or logs it. For the
/// library crates built on this one; applications install a handler.
pub fn warn(message: &str) {
    match HANDLER.get() {
        Some(handler) => handler(message),
        None => tracing::warn!(target: "ferrika::notice", "{message}"),
    }
}
