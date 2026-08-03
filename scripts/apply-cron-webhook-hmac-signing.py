from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


source_path = Path("src/schedule_runner.rs")
source = source_path.read_text(encoding="utf-8")

source = replace_once(
    source,
    "use std::collections::HashSet;\nuse std::sync::{Arc, Mutex, OnceLock};",
    "use std::collections::HashSet;\nuse std::fmt::Write as _;\nuse std::sync::{Arc, Mutex, OnceLock};",
    "std imports",
)
source = replace_once(
    source,
    "use reqwest::header::{HeaderMap, HeaderName, HeaderValue, RETRY_AFTER};",
    "use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE, RETRY_AFTER};",
    "reqwest header imports",
)
source = replace_once(
    source,
    "use serde_json::json;\nuse tokio::sync::{OwnedSemaphorePermit, Semaphore};",
    "use serde_json::json;\nuse sha2::{Digest, Sha256};\nuse tokio::sync::{OwnedSemaphorePermit, Semaphore};",
    "sha2 imports",
)
source = replace_once(
    source,
    "const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);",
    "const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);\nconst MIN_WEBHOOK_SIGNING_SECRET_BYTES: usize = 32;\nconst HMAC_SHA256_BLOCK_BYTES: usize = 64;\nconst WEBHOOK_SIGNATURE_HEADER: &str = \"X-Fiducia-Signature-256\";",
    "signing constants",
)
source = replace_once(
    source,
    "struct RunnerConfig {\n    lambda_base_url: Option<Url>,\n    lambda_server_auth: Option<HeaderValue>,\n    max_in_flight: usize,\n}",
    "struct RunnerConfig {\n    lambda_base_url: Option<Url>,\n    lambda_server_auth: Option<HeaderValue>,\n    webhook_signing_secret: Option<Arc<[u8]>>,\n    max_in_flight: usize,\n}",
    "runner config fields",
)
source = replace_once(
    source,
    "        let lambda_server_auth = std::env::var(\"FIDUCIA_LAMBDA_SERVER_AUTH_SECRET\")\n            .ok()\n            .and_then(|value| HeaderValue::from_str(&value).ok());\n        Self {\n            lambda_base_url,\n            lambda_server_auth,\n            max_in_flight,\n        }\n    }\n}\n\nstruct CronMetrics",
    "        let lambda_server_auth = std::env::var(\"FIDUCIA_LAMBDA_SERVER_AUTH_SECRET\")\n            .ok()\n            .and_then(|value| HeaderValue::from_str(&value).ok());\n        let webhook_signing_secret = webhook_signing_secret_from_env();\n        Self {\n            lambda_base_url,\n            lambda_server_auth,\n            webhook_signing_secret,\n            max_in_flight,\n        }\n    }\n}\n\nfn webhook_signing_secret_from_env() -> Option<Arc<[u8]>> {\n    match std::env::var(\"FIDUCIA_CRON_WEBHOOK_SIGNING_SECRET\") {\n        Ok(value) if value.as_bytes().len() >= MIN_WEBHOOK_SIGNING_SECRET_BYTES => {\n            Some(Arc::<[u8]>::from(value.into_bytes()))\n        }\n        Ok(_) => panic!(\n            \"FIDUCIA_CRON_WEBHOOK_SIGNING_SECRET must contain at least {MIN_WEBHOOK_SIGNING_SECRET_BYTES} bytes\"\n        ),\n        Err(std::env::VarError::NotPresent) => None,\n        Err(std::env::VarError::NotUnicode(_)) => {\n            panic!(\"FIDUCIA_CRON_WEBHOOK_SIGNING_SECRET must be valid UTF-8\")\n        }\n    }\n}\n\nstruct CronMetrics",
    "runner config environment",
)
source = replace_once(
    source,
    "        cron.max_in_flight = config.max_in_flight,\n        cron.lambda_configured = config.lambda_base_url.is_some(),\n        \"cron runner configured\"",
    "        cron.max_in_flight = config.max_in_flight,\n        cron.lambda_configured = config.lambda_base_url.is_some(),\n        cron.webhook_signing_configured = config.webhook_signing_secret.is_some(),\n        \"cron runner configured\"",
    "runner startup telemetry",
)
source = replace_once(
    source,
    "    let body = json!({\n        \"schedule\": name,\n        \"fire_id\": fire_id_ms.to_string(),\n        \"fired_at_ms\": fire_id_ms,\n        \"target_kind\": target_kind(&schedule.target),\n    });\n    let max_attempts = schedule.max_retries.saturating_add(1);",
    "    let body = json!({\n        \"schedule\": name,\n        \"fire_id\": fire_id_ms.to_string(),\n        \"fired_at_ms\": fire_id_ms,\n        \"target_kind\": target_kind(&schedule.target),\n    });\n    // Sign the exact byte sequence sent on the wire. Computing this once before\n    // retries keeps the body, signature, and idempotency identity stable.\n    let body_bytes = serde_json::to_vec(&body).expect(\"schedule delivery body must serialize\");\n    let webhook_signature = webhook_signature_header(\n        &schedule.target,\n        config.webhook_signing_secret.as_deref(),\n        &body_bytes,\n    );\n    let max_attempts = schedule.max_retries.saturating_add(1);",
    "delivery body serialization",
)
source = replace_once(
    source,
    "            let mut request = http\n                .post(url.clone())\n                .headers(headers)\n                .header(\"Idempotency-Key\", &idempotency_key)\n                .header(\"X-Fiducia-Schedule\", name)\n                .json(&body);\n            if function_auth {",
    "            let mut request = http\n                .post(url.clone())\n                .headers(headers)\n                .header(CONTENT_TYPE, \"application/json\")\n                .header(\"Idempotency-Key\", &idempotency_key)\n                .header(\"X-Fiducia-Schedule\", name)\n                .body(body_bytes.clone());\n            if let Some(signature) = webhook_signature.clone() {\n                request = request.header(WEBHOOK_SIGNATURE_HEADER, signature);\n            }\n            if function_auth {",
    "delivery request body and signature",
)
source = replace_once(
    source,
    "fn record_retry(target: &ScheduleTarget, trigger: RunTrigger, reason: &'static str) {",
    "fn webhook_signature_header(\n    target: &ScheduleTarget,\n    signing_secret: Option<&[u8]>,\n    body: &[u8],\n) -> Option<HeaderValue> {\n    if !matches!(target, ScheduleTarget::Webhook { .. }) {\n        return None;\n    }\n    let signing_secret = signing_secret?;\n    let value = hmac_sha256_header_value(signing_secret, body);\n    Some(HeaderValue::from_str(&value).expect(\"HMAC-SHA256 signature is a valid header value\"))\n}\n\nfn hmac_sha256_header_value(secret: &[u8], body: &[u8]) -> String {\n    let mut key_block = [0_u8; HMAC_SHA256_BLOCK_BYTES];\n    if secret.len() > HMAC_SHA256_BLOCK_BYTES {\n        let digest = Sha256::digest(secret);\n        key_block[..digest.len()].copy_from_slice(&digest);\n    } else {\n        key_block[..secret.len()].copy_from_slice(secret);\n    }\n\n    let mut inner_pad = [0x36_u8; HMAC_SHA256_BLOCK_BYTES];\n    let mut outer_pad = [0x5c_u8; HMAC_SHA256_BLOCK_BYTES];\n    for (index, byte) in key_block.iter().copied().enumerate() {\n        inner_pad[index] ^= byte;\n        outer_pad[index] ^= byte;\n    }\n\n    let mut inner = Sha256::new();\n    inner.update(inner_pad);\n    inner.update(body);\n    let inner_digest = inner.finalize();\n\n    let mut outer = Sha256::new();\n    outer.update(outer_pad);\n    outer.update(inner_digest);\n    let digest = outer.finalize();\n\n    let mut value = String::with_capacity(\"sha256=\".len() + digest.len() * 2);\n    value.push_str(\"sha256=\");\n    for byte in digest {\n        write!(&mut value, \"{byte:02x}\").expect(\"writing hexadecimal to String cannot fail\");\n    }\n    value\n}\n\nfn record_retry(target: &ScheduleTarget, trigger: RunTrigger, reason: &'static str) {",
    "webhook HMAC helpers",
)
source = replace_once(
    source,
    "    #[test]\n    fn lambda_base_is_operator_controlled_and_normalized() {",
    "    #[test]\n    fn webhook_hmac_matches_rfc_4231_and_is_scoped_to_webhook_targets() {\n        let secret = vec![0x0b; 20];\n        let body = b\"Hi There\";\n        let expected =\n            \"sha256=b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7\";\n        assert_eq!(hmac_sha256_header_value(&secret, body), expected);\n\n        let webhook = ScheduleTarget::Webhook {\n            url: \"https://messaging-intel.example/internal/cron\".to_string(),\n        };\n        assert_eq!(\n            webhook_signature_header(&webhook, Some(&secret), body)\n                .unwrap()\n                .to_str()\n                .unwrap(),\n            expected,\n        );\n        assert!(webhook_signature_header(&webhook, None, body).is_none());\n\n        let function = ScheduleTarget::Function {\n            function_id: \"fn_1\".to_string(),\n        };\n        assert!(webhook_signature_header(&function, Some(&secret), body).is_none());\n    }\n\n    #[test]\n    fn lambda_base_is_operator_controlled_and_normalized() {",
    "webhook signing tests",
)
source = replace_once(
    source,
    "            lambda_server_auth: Some(HeaderValue::from_static(\"secret\")),\n            max_in_flight: 1,",
    "            lambda_server_auth: Some(HeaderValue::from_static(\"secret\")),\n            webhook_signing_secret: None,\n            max_in_flight: 1,",
    "test runner config",
)

source_path.write_text(source, encoding="utf-8")

doc = Path("docs/cron-webhook-signing.md")
doc.parent.mkdir(parents=True, exist_ok=True)
doc.write_text(
    """# Cron webhook authenticity\n\n`fiducia-node` can authenticate cron webhook deliveries with an HMAC-SHA256\nsignature. Configure the node process with:\n\n```text\nFIDUCIA_CRON_WEBHOOK_SIGNING_SECRET=<at least 32 bytes>\n```\n\nWhen configured, every `webhook` schedule delivery includes:\n\n```text\nX-Fiducia-Signature-256: sha256=<64 lowercase hexadecimal characters>\n```\n\nThe MAC covers the exact UTF-8 JSON bytes sent in the HTTP request body. The\nbody is serialized and signed once per durable fire, before retry processing, so\nretries preserve the same body, signature, `X-Fiducia-Schedule`, and\n`Idempotency-Key`. Receivers should verify the signature over the raw body before\nJSON parsing, compare the digest in constant time, validate the schedule name and\nidempotency key, and reject missing signatures.\n\nThe secret is operator configuration only. It is never placed in a schedule, the\nRaft log, delivery body, result history, metrics, or tracing fields. The setting\nis intentionally opt-in for backward compatibility; webhook receivers that\nrequire authentication should fail closed until every delivering node has the\nsame secret configured.\n\nThe secret currently defines one node-cluster webhook trust domain. Do not share\nit with untrusted receivers. Rotate it by first making receivers accept both the\nold and new secret, rolling the new value across all nodes, and then removing the\nold receiver secret.\n""",
    encoding="utf-8",
)
