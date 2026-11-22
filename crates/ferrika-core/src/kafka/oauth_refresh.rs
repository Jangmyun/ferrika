//! Hands OAUTHBEARER tokens to librdkafka clients and keeps them fresh.
//!
//! The clients keep `DefaultClientContext`: right after a client is created
//! its first token is set with `rd_kafka_oauthbearer_set_token`, and an
//! [`OAuthRefresher`] thread sets a new one at 80 % of each token's life
//! (`rd_kafka_oauthbearer_set_token_failure` and a backoff when fetching
//! fails). A custom `ClientContext` was not used: rdkafka 0.39 does not run
//! its refresh callback for `AdminClient`, its `OAuthToken` has no SASL
//! extensions, and librdkafka's own OIDC support needs libcurl.
//!
//! librdkafka only waits for a token before it connects, so setting it right
//! after creation is early enough.
//!
//! A wrapper that owns a client and its refresher must declare the
//! refresher field *before* the client field, so it is dropped (and its
//! thread joined) while the client still exists.

use std::ffi::{c_char, CStr, CString};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use rdkafka::bindings as rdsys;
use rdkafka::client::{Client, ClientContext};

use crate::error::{KafkaError, KafkaResult};
use crate::kafka::config::KafkaClientConfig;
use crate::kafka::oauth::{self, now_ms, OAuthToken, TokenSource};

/// Shortest wait between two refresh attempts.
const MIN_WAIT: Duration = Duration::from_secs(1);
/// Longest backoff after failed refreshes.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Where refreshed tokens go: a librdkafka client, or a recorder in tests.
trait TokenSink: Send + 'static {
    fn set(&self, token: &OAuthToken) -> KafkaResult<()>;
    fn fail(&self, reason: &str);
}

/// A librdkafka client handle the refresher thread may use.
struct ClientSink(*mut rdsys::rd_kafka_t);

// SAFETY: librdkafka's oauthbearer token functions are thread-safe (they
// lock the SASL handle). The pointer stays valid because the owner of the
// client drops (and joins) the refresher before the client.
unsafe impl Send for ClientSink {}

impl TokenSink for ClientSink {
    fn set(&self, token: &OAuthToken) -> KafkaResult<()> {
        set_token(self.0, token)
    }

    fn fail(&self, reason: &str) {
        let text = CString::new(reason.replace('\0', " "))
            .unwrap_or_else(|_| c"OAuth token refresh failed".to_owned());
        // SAFETY: the client pointer is valid (see `ClientSink`), the string
        // is NUL-terminated and outlives the call; librdkafka copies it.
        let err = unsafe { rdsys::rd_kafka_oauthbearer_set_token_failure(self.0, text.as_ptr()) };
        if err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            tracing::warn!("librdkafka refused the OAuth token failure: {err:?}");
        }
    }
}

fn c_string(text: &str, what: &str) -> KafkaResult<CString> {
    CString::new(text)
        .map_err(|_| KafkaError::config(format!("The OAuth {what} contains a NUL byte")))
}

/// Sets `token` on the client behind `rk`.
fn set_token(rk: *mut rdsys::rd_kafka_t, token: &OAuthToken) -> KafkaResult<()> {
    let value = c_string(&token.value, "token")?;
    let principal = c_string(&token.principal, "principal")?;
    let mut owned = Vec::with_capacity(token.extensions.len() * 2);
    for (k, v) in &token.extensions {
        owned.push(c_string(k, "extension name")?);
        owned.push(c_string(v, "extension value")?);
    }
    let mut extensions: Vec<*const c_char> = owned.iter().map(|s| s.as_ptr()).collect();
    let mut errstr = [0 as c_char; 512];
    // SAFETY: `rk` is a live client (callers hold the client while calling),
    // every string is NUL-terminated and lives until the call returns
    // (librdkafka copies them), `extensions` holds `len` pointers in
    // key/value pairs, and `errstr` is writable for its full length.
    let err = unsafe {
        rdsys::rd_kafka_oauthbearer_set_token(
            rk,
            value.as_ptr(),
            token.expires_at_ms,
            principal.as_ptr(),
            extensions.as_mut_ptr(),
            extensions.len(),
            errstr.as_mut_ptr(),
            errstr.len(),
        )
    };
    if err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        return Ok(());
    }
    // SAFETY: librdkafka NUL-terminates `errstr` within its length; the last
    // byte is forced to NUL in case it did not.
    errstr[errstr.len() - 1] = 0;
    let message = unsafe { CStr::from_ptr(errstr.as_ptr()) }
        .to_string_lossy()
        .into_owned();
    Err(KafkaError::config(format!(
        "librdkafka refused the OAuth token ({err:?}): {message}"
    )))
}

/// The first token of a client, fetched before the client is created so a
/// bad token endpoint or missing AWS credentials fail up front.
pub(crate) struct OAuthSetup {
    source: Arc<dyn TokenSource>,
    token: OAuthToken,
    timeout: Duration,
}

/// The token setup for `config`, or `None` when it does not use OAUTHBEARER.
/// The first token request is bounded by the request timeout.
pub(crate) fn prepare(config: &KafkaClientConfig) -> KafkaResult<Option<OAuthSetup>> {
    if !config.uses_oauth() {
        return Ok(None);
    }
    let source = oauth::source_for(config)?;
    let token = oauth::blocking_token(source.as_ref())?;
    Ok(Some(OAuthSetup {
        source,
        token,
        timeout: config.request_timeout,
    }))
}

impl OAuthSetup {
    /// Sets the first token on `client` and starts its refresher. The
    /// caller keeps the refresher next to the client, declared before it.
    pub(crate) fn attach<C: ClientContext>(
        self,
        client: &Client<C>,
    ) -> KafkaResult<OAuthRefresher> {
        let sink = ClientSink(client.native_ptr());
        sink.set(&self.token)?;
        Ok(OAuthRefresher::start(
            sink,
            self.source,
            self.token,
            self.timeout,
        ))
    }
}

/// Prepares the token, then attaches it to the client `create` builds:
/// `with_oauth(config, || rd_config.create(), |c| c.client())`.
pub(crate) fn with_oauth<T, C: ClientContext>(
    config: &KafkaClientConfig,
    create: impl FnOnce() -> KafkaResult<T>,
    client_of: impl FnOnce(&T) -> &Client<C>,
) -> KafkaResult<(Option<OAuthRefresher>, T)> {
    let setup = prepare(config)?;
    let client = create()?;
    let refresher = match setup {
        Some(setup) => Some(setup.attach(client_of(&client))?),
        None => None,
    };
    Ok((refresher, client))
}

/// Stop flag the refresher thread waits on.
type StopSignal = Arc<(Mutex<bool>, Condvar)>;

/// Keeps one client's token fresh on a background thread. Dropping it stops
/// and joins the thread.
pub struct OAuthRefresher {
    stop: StopSignal,
    thread: Option<JoinHandle<()>>,
}

impl OAuthRefresher {
    fn start(
        sink: impl TokenSink,
        source: Arc<dyn TokenSource>,
        token: OAuthToken,
        timeout: Duration,
    ) -> Self {
        let stop: StopSignal = Arc::new((Mutex::new(false), Condvar::new()));
        let signal = stop.clone();
        let thread = std::thread::Builder::new()
            .name("ferrika-oauth".to_string())
            .spawn(move || refresh_loop(&sink, source.as_ref(), token, timeout, &signal))
            .map_err(|e| tracing::warn!("Cannot start the OAuth token refresher: {e}"))
            .ok();
        Self { stop, thread }
    }
}

impl Drop for OAuthRefresher {
    fn drop(&mut self) {
        let (lock, cvar) = &*self.stop;
        if let Ok(mut stopped) = lock.lock() {
            *stopped = true;
        }
        cvar.notify_all();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Waits up to `wait`; true when asked to stop.
fn wait_or_stop(signal: &StopSignal, wait: Duration) -> bool {
    let (lock, cvar) = &**signal;
    let Ok(guard) = lock.lock() else {
        return true;
    };
    match cvar.wait_timeout_while(guard, wait, |stopped| !*stopped) {
        Ok((stopped, _)) => *stopped,
        Err(_) => true,
    }
}

fn refresh_loop(
    sink: &dyn TokenSink,
    source: &dyn TokenSource,
    mut token: OAuthToken,
    timeout: Duration,
    signal: &StopSignal,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            sink.fail(&format!("Cannot start the OAuth refresher runtime: {e}"));
            return;
        }
    };
    let mut backoff = MIN_WAIT;
    let mut wait = until(token.refresh_at_ms());
    loop {
        if wait_or_stop(signal, wait) {
            return;
        }
        let fetched = runtime.block_on(async {
            tokio::time::timeout(timeout, oauth::cached_token(source))
                .await
                .unwrap_or_else(|_| {
                    Err(KafkaError::Timeout(format!(
                        "No OAuth token within {:.1}s",
                        timeout.as_secs_f32()
                    )))
                })
        });
        match fetched.and_then(|new| sink.set(&new).map(|()| new)) {
            Ok(new) => {
                token = new;
                backoff = MIN_WAIT;
                wait = until(token.refresh_at_ms());
            }
            Err(e) => {
                tracing::warn!("OAuth token refresh failed: {e}");
                sink.fail(&e.to_string());
                wait = backoff;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

/// Time from now until `at_ms`, at least [`MIN_WAIT`].
fn until(at_ms: i64) -> Duration {
    let ms = u64::try_from(at_ms - now_ms()).unwrap_or(0);
    Duration::from_millis(ms).max(MIN_WAIT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::oauth::BoxFuture;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Hands out tokens that live `lifetime_ms`, or fails.
    struct Source {
        key: String,
        lifetime_ms: i64,
        fail: bool,
        fetches: AtomicUsize,
    }

    impl TokenSource for Source {
        fn cache_key(&self) -> String {
            self.key.clone()
        }
        fn fetch(&self) -> BoxFuture<'_, KafkaResult<OAuthToken>> {
            Box::pin(async move {
                let n = self.fetches.fetch_add(1, Ordering::SeqCst);
                if self.fail {
                    return Err(KafkaError::config("idp down"));
                }
                let now = now_ms();
                Ok(OAuthToken {
                    value: format!("t{n}"),
                    principal: "p".into(),
                    issued_at_ms: now,
                    expires_at_ms: now + self.lifetime_ms,
                    extensions: BTreeMap::new(),
                })
            })
        }
    }

    #[derive(Clone, Default)]
    struct Recorder(Arc<Mutex<Vec<String>>>);

    impl TokenSink for Recorder {
        fn set(&self, token: &OAuthToken) -> KafkaResult<()> {
            self.0.lock().unwrap().push(format!("set {}", token.value));
            Ok(())
        }
        fn fail(&self, reason: &str) {
            self.0.lock().unwrap().push(format!("fail {reason}"));
        }
    }

    fn source(tag: &str, lifetime_ms: i64, fail: bool) -> Arc<Source> {
        Arc::new(Source {
            key: format!("refresh-{tag}-{}", std::process::id()),
            lifetime_ms,
            fail,
            fetches: AtomicUsize::new(0),
        })
    }

    fn first_token(lifetime_ms: i64) -> OAuthToken {
        let now = now_ms();
        OAuthToken {
            value: "first".into(),
            principal: "p".into(),
            issued_at_ms: now,
            expires_at_ms: now + lifetime_ms,
            extensions: BTreeMap::new(),
        }
    }

    #[test]
    fn refreshes_short_tokens_and_stops_on_drop() {
        let recorder = Recorder::default();
        // A 2 s token is always inside the 60 s floor: refreshed every MIN_WAIT.
        let src = source("short", 2_000, false);
        let refresher = OAuthRefresher::start(
            recorder.clone(),
            src.clone(),
            first_token(2_000),
            Duration::from_secs(5),
        );
        std::thread::sleep(Duration::from_millis(2_500));
        let started = std::time::Instant::now();
        drop(refresher);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "drop must not wait"
        );
        let seen = recorder.0.lock().unwrap().clone();
        assert!(seen.len() >= 2, "{seen:?}");
        assert!(seen.iter().all(|s| s.starts_with("set t")), "{seen:?}");
        let after = src.fetches.load(Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(1_200));
        assert_eq!(src.fetches.load(Ordering::SeqCst), after, "stopped");
    }

    #[test]
    fn failures_are_reported_to_librdkafka_with_backoff() {
        let recorder = Recorder::default();
        let src = source("failing", 2_000, true);
        let refresher = OAuthRefresher::start(
            recorder.clone(),
            src.clone(),
            first_token(2_000),
            Duration::from_secs(5),
        );
        // Attempts at about 1 s, then 1 s later, then 2 s after that.
        std::thread::sleep(Duration::from_millis(2_600));
        drop(refresher);
        let seen = recorder.0.lock().unwrap().clone();
        assert_eq!(seen.len(), 2, "{seen:?}");
        assert!(seen.iter().all(|s| s.contains("idp down")), "{seen:?}");
    }

    #[test]
    fn long_tokens_wait_until_80_percent() {
        let token = first_token(3_600_000);
        let wait = until(token.refresh_at_ms());
        assert!(wait > Duration::from_secs(2_870) && wait <= Duration::from_secs(2_880));
        assert_eq!(until(0), MIN_WAIT);
    }

    /// The FFI path on a real (unconnected) librdkafka client.
    #[test]
    fn set_token_reaches_librdkafka() {
        use rdkafka::producer::{BaseProducer, Producer};
        let mut config = rdkafka::ClientConfig::new();
        config.set("bootstrap.servers", "127.0.0.1:1");
        let oauth_capable = cfg!(feature = "ssl");
        if oauth_capable {
            config.set("security.protocol", "sasl_plaintext");
            config.set("sasl.mechanism", "OAUTHBEARER");
        }
        let producer: BaseProducer = config.create().unwrap();
        let mut token = first_token(60_000);
        token.extensions.insert("traceId".into(), "abc".into());
        let result = set_token(producer.client().native_ptr(), &token);
        if oauth_capable {
            result.unwrap();
            // An expired token is refused with librdkafka's message.
            let expired = OAuthToken {
                expires_at_ms: 1,
                ..first_token(1)
            };
            let err = set_token(producer.client().native_ptr(), &expired).unwrap_err();
            assert!(err.to_string().contains("librdkafka refused"), "{err}");
            ClientSink(producer.client().native_ptr()).fail("test failure");
        } else {
            // PLAINTEXT client: librdkafka refuses instead of crashing.
            let err = result.unwrap_err();
            assert!(err.to_string().contains("librdkafka refused"), "{err}");
        }
    }
}
