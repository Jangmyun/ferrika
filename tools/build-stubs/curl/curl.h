/*
 * Empty stand-in for <curl/curl.h>, used only while building the bundled
 * librdkafka (see .cargo/config.toml).
 *
 * librdkafka 2.12.1 (rdkafka-sys 4.10.0) includes <curl/curl.h> from
 * src/rdkafka_conf.c with `#ifdef WITH_OAUTHBEARER_OIDC`. Its CMake config
 * header writes `#define WITH_OAUTHBEARER_OIDC 0` when OIDC is off, so the
 * `#ifdef` is true and the header is needed even though libcurl is not used:
 * rdkafka-sys passes -DWITH_CURL=0 unless the `curl` feature is on, and
 * nothing is linked against libcurl. Without this file the build fails on any
 * machine that has no libcurl development package (for example a fresh
 * Ubuntu runner). With it, the build output is the same on every machine.
 *
 * If OIDC support is ever switched on (rdkafka `curl` feature + SSL), this
 * stub would hide the real header, so stop with a clear message instead.
 */
#if defined(WITH_OAUTHBEARER_OIDC) && WITH_OAUTHBEARER_OIDC
#error "tools/build-stubs/curl/curl.h hides the real libcurl header; remove the C_INCLUDE_PATH entry in .cargo/config.toml when enabling librdkafka's curl support"
#endif
