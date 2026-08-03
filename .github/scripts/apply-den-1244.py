#!/usr/bin/env python3
"""Apply the reviewed DEN-1244 source transformation exactly once.

Temporary branch helper. The helper and its workflow are removed after the
source commit lands; the final PR retains only src/kv.rs and its tests.
"""

from pathlib import Path

path = Path("src/kv.rs")
text = path.read_text()


def replace_once(old: str, new: str, label: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one source match, found {count}")
    text = text.replace(old, new, 1)


replace_once(
    "    http::{StatusCode, Uri},",
    "    http::{HeaderMap, StatusCode, Uri},",
    "HeaderMap import",
)
replace_once(
    "const VAULT_RESPONSE_MAX_BYTES: usize = 64 * 1024;\n",
    "const VAULT_RESPONSE_MAX_BYTES: usize = 64 * 1024;\n"
    'const TRUSTED_SCOPES_HEADER: &str = "x-fiducia-scopes";\n'
    'const ADMIN_WRITE_SCOPE: &str = "admin:write";\n'
    'const SECRET_KEYSPACE_PREFIX: &str = "secret/";\n',
    "plaintext policy constants",
)
replace_once(
    '''/// A client may opt a specific write out with `"plaintext": true`; that value
/// is stored verbatim.
''',
    '''/// A trusted caller may request a plaintext write only with the explicit
/// `admin:write` scope and never in the reserved `secret/` keyspace. Ordinary
/// `kv:write` and wildcard identities fail closed with HTTP 403.
''',
    "KV protection posture documentation",
)
replace_once(
    '''/// Seal a to-be-written value with the node's cipher unless the caller opted
/// out or encryption is disabled.
async fn seal_for_write(
''',
    '''/// Whether the trusted-hop identity may intentionally persist one plaintext KV
/// value. The load balancer strips all client-supplied `x-fiducia-*` headers and
/// injects one canonical space-separated scope header after authentication.
///
/// Fail closed when the header is absent, malformed, duplicated, or merely
/// carries `*`: plaintext is a separate administrative authority, not an
/// implication of broad data-plane access. Secret-delivery keys never permit an
/// opt-out, including for administrators.
fn plaintext_write_authorized(headers: &HeaderMap, caller_key: &str) -> bool {
    if caller_key == "secret" || caller_key.starts_with(SECRET_KEYSPACE_PREFIX) {
        return false;
    }

    let mut values = headers.get_all(TRUSTED_SCOPES_HEADER).iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    let Ok(value) = value.to_str() else {
        return false;
    };
    value
        .split_ascii_whitespace()
        .any(|scope| scope == ADMIN_WRITE_SCOPE)
}

/// Seal a to-be-written value with the node's cipher unless a separately
/// authorized plaintext request reached this point or encryption is disabled.
async fn seal_for_write(
''',
    "plaintext authorization helper",
)
replace_once(
    '''async fn put_key(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
''',
    '''async fn put_key(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    headers: HeaderMap,
    uri: Uri,
''',
    "PUT handler trusted headers",
)
replace_once(
    '''    let Some(key) = q.key else {
        return bad_request("missing `key`");
    };
    // Seal before the value enters the log, so ciphertext is what gets
''',
    '''    let Some(key) = q.key else {
        return bad_request("missing `key`");
    };
    if body.plaintext && !plaintext_write_authorized(&headers, &key) {
        return plaintext_write_forbidden();
    }
    // Seal before the value enters the log, so ciphertext is what gets
''',
    "plaintext PUT authorization gate",
)
replace_once(
    "fn crypto_failure(operation: &'static str, error: &KvCryptoError) -> Response {\n",
    '''fn plaintext_write_forbidden() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({
            "error": "plaintext_kv_forbidden",
            "detail": "plaintext:true requires admin:write and is never permitted for the secret keyspace"
        })),
    )
        .into_response()
}

fn crypto_failure(operation: &'static str, error: &KvCryptoError) -> Response {
''',
    "stable plaintext denial response",
)
replace_once(
    '''        .expect("valid test keyring")
    }

    #[test]
    fn seal_then_unseal_round_trips_with_protection_metadata() {
''',
    '''        .expect("valid test keyring")
    }

    fn scope_headers(values: &[&'static str]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for value in values {
            headers.append(
                TRUSTED_SCOPES_HEADER,
                axum::http::HeaderValue::from_static(value),
            );
        }
        headers
    }

    #[test]
    fn plaintext_write_requires_exact_admin_scope_and_non_secret_keyspace() {
        assert!(!plaintext_write_authorized(&HeaderMap::new(), "flags/a"));
        assert!(!plaintext_write_authorized(
            &scope_headers(&["kv:write"]),
            "flags/a"
        ));
        assert!(!plaintext_write_authorized(
            &scope_headers(&["*"]),
            "flags/a"
        ));
        assert!(plaintext_write_authorized(
            &scope_headers(&["kv:write admin:write"]),
            "flags/a"
        ));
        assert!(!plaintext_write_authorized(
            &scope_headers(&["admin:write", "kv:write"]),
            "flags/a"
        ));
        assert!(!plaintext_write_authorized(
            &scope_headers(&["admin:write"]),
            "secret/database-password"
        ));
        assert!(!plaintext_write_authorized(
            &scope_headers(&["admin:write"]),
            "secret"
        ));
        assert!(plaintext_write_authorized(
            &scope_headers(&["admin:write"]),
            "secrets/non-reserved-name"
        ));
    }

    #[test]
    fn seal_then_unseal_round_trips_with_protection_metadata() {
''',
    "plaintext policy unit tests",
)

path.write_text(text)
