//! Config KV with watches.
//!
//! A linearizable, versioned key/value store for configuration and feature
//! flags — the etcd/ZooKeeper-znode primitive. Writes are proposed to the owning
//! shard's Raft group via [`Node::propose`]; single-key reads go through
//! [`Node::query`]. `watch` streams change events so clients get live config
//! push instead of polling.
//!
//! **The key is a `?key=` query parameter, never a path segment.** That keeps it
//! free of any path grammar (it may contain slashes, dots, be empty, etc.) and
//! gives the load balancer one uniform place to find the routing key on every
//! request — the same reason etcd carries keys in the request, not the URL.
//!
//! Routes (mounted under `/v1/kv`):
//!   * `GET    /v1/kv?key=K`              — read a key (+ its revision)
//!   * `GET    /v1/kv?key=K&watch=true`   — SSE stream of changes for that key
//!   * `GET    /v1/kv?prefix=P&watch=true`— SSE stream for every key under prefix `P`
//!   * `GET    /v1/kv?prefix=P`           — list keys under a prefix
//!   * `PUT    /v1/kv?key=K`              — upsert `{ "value", "ttl_ms"? }`, optional CAS
//!   * `DELETE /v1/kv?key=K`              — delete a key

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use aes_gcm::aead::{Aead, KeyInit, OsRng, Payload};
use aes_gcm::{AeadCore, Aes256Gcm, Key, Nonce};
use axum::{
    extract::{Query, State},
    http::{header::CACHE_CONTROL, HeaderMap, HeaderValue, StatusCode, Uri},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::get,
    Json, Router,
};
use base64::Engine;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_stream::{wrappers::BroadcastStream, StreamExt, StreamMap};

use crate::consensus::{read_error_response, Node, ReadRequest, ReadResponse};
use crate::org_scope::OrgScope;
use crate::state::Command;

/// Marker prefix on a sealed KV value. Self-describing so a value can be
/// recognised as ciphertext on read without a schema flag, which is what lets
/// encrypted and plaintext-declared values coexist in the same keyspace.
const KV_ENVELOPE_V1_PREFIX: &str = "fcenc:v1:";
const KV_ENVELOPE_V2_PREFIX: &str = "fcenc:v2:";
const KV_VAULT_ENVELOPE_PREFIX: &str = "fcenc:vault:v1:";
const VAULT_RESPONSE_MAX_BYTES: usize = 64 * 1024;
const TRUSTED_SCOPES_HEADER: &str = "x-fiducia-scopes";
const ADMIN_WRITE_SCOPE: &str = "admin:write";
const SECRET_KEYSPACE_PREFIX: &str = "secret/";

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct KvProtection {
    pub at_rest: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_version: Option<u64>,
}

impl KvProtection {
    fn plaintext() -> Self {
        Self {
            at_rest: "plaintext",
            provider: None,
            key_id: None,
            key_version: None,
        }
    }
}

struct UnsealedValue {
    value: String,
    protection: KvProtection,
}

#[derive(Debug)]
pub struct KvCryptoError(String);

impl KvCryptoError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for KvCryptoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for KvCryptoError {}

/// KV value protection at rest.
///
/// **Default posture:** when Vault Transit or a local AES-256-GCM keyring is
/// configured, values are sealed *before* they enter the Raft log — so the
/// on-disk log, snapshots, and in-memory state machine all hold ciphertext.
/// A trusted caller may request a plaintext write only with the explicit
/// `admin:write` scope and never in the reserved `secret/` keyspace. Ordinary
/// `kv:write` and wildcard identities fail closed with HTTP 403.
///
/// Sealing happens once, on the node that receives the PUT, and the resulting
/// envelope string is what gets replicated — so every replica stores identical
/// bytes and the state machine stays deterministic despite the random nonce.
pub enum KvCipher {
    Local(LocalKeyring),
    Vault(VaultTransit),
}

pub struct LocalKeyring {
    active_key_id: String,
    keys: HashMap<String, Aes256Gcm>,
}

pub struct VaultTransit {
    address: Url,
    mount: String,
    key_name: String,
    token: String,
    namespace: Option<String>,
    client: reqwest::Client,
}

impl fmt::Debug for KvCipher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local(keyring) => f
                .debug_struct("KvCipher::Local")
                .field("active_key_id", &keyring.active_key_id)
                .field("key_count", &keyring.keys.len())
                .finish(),
            Self::Vault(vault) => f
                .debug_struct("KvCipher::Vault")
                .field("address", &vault.address)
                .field("mount", &vault.mount)
                .field("key_name", &vault.key_name)
                .field("namespace", &vault.namespace)
                .field("token", &"<redacted>")
                .finish(),
        }
    }
}

impl KvCipher {
    /// Load one fail-closed protection backend.
    ///
    /// `FIDUCIA_KV_VAULT_ADDR` + `FIDUCIA_KV_VAULT_TOKEN` +
    /// `FIDUCIA_KV_VAULT_KEY` selects HashiCorp Vault Transit. Vault owns key
    /// material and key-version rotation; Fiducia stores only Vault ciphertext.
    /// Otherwise a local keyring is loaded from
    /// `FIDUCIA_KV_ENCRYPTION_KEYS` (JSON object of key-id -> base64 key) and
    /// `FIDUCIA_KV_ENCRYPTION_ACTIVE_KEY_ID`. The legacy single-key variable is
    /// still accepted and is emitted as a v2 envelope with key id `legacy`.
    /// Partial or malformed configuration is an error, never a silent downgrade.
    pub fn from_env() -> Result<Option<Self>, KvCryptoError> {
        Self::from_lookup(nonempty_env)
    }

    fn from_lookup(
        mut lookup: impl FnMut(&str) -> Option<String>,
    ) -> Result<Option<Self>, KvCryptoError> {
        let vault_address = lookup("FIDUCIA_KV_VAULT_ADDR");
        let vault_token = lookup("FIDUCIA_KV_VAULT_TOKEN");
        let vault_key = lookup("FIDUCIA_KV_VAULT_KEY");
        let vault_mount = lookup("FIDUCIA_KV_VAULT_MOUNT");
        let vault_namespace = lookup("FIDUCIA_KV_VAULT_NAMESPACE");
        let versioned_keys = lookup("FIDUCIA_KV_ENCRYPTION_KEYS");
        let active_key_id = lookup("FIDUCIA_KV_ENCRYPTION_ACTIVE_KEY_ID");
        let legacy_key = lookup("FIDUCIA_KV_ENCRYPTION_KEY");
        let legacy_key_id = lookup("FIDUCIA_KV_ENCRYPTION_KEY_ID");

        let vault_present = [
            &vault_address,
            &vault_token,
            &vault_key,
            &vault_mount,
            &vault_namespace,
        ]
        .into_iter()
        .any(Option::is_some);
        let versioned_present = versioned_keys.is_some() || active_key_id.is_some();
        let legacy_present = legacy_key.is_some() || legacy_key_id.is_some();
        let local_present = versioned_present || legacy_present;

        if vault_present && local_present {
            return Err(KvCryptoError::new(
                "configure exactly one KV protection backend: Vault Transit or a local keyring",
            ));
        }

        if vault_present {
            let address = vault_address.ok_or_else(|| {
                KvCryptoError::new("FIDUCIA_KV_VAULT_ADDR is required for Vault Transit")
            })?;
            let token = vault_token.ok_or_else(|| {
                KvCryptoError::new("FIDUCIA_KV_VAULT_TOKEN is required for Vault Transit")
            })?;
            let key_name = vault_key.ok_or_else(|| {
                KvCryptoError::new("FIDUCIA_KV_VAULT_KEY is required for Vault Transit")
            })?;
            let address = validate_vault_address(&address)?;
            let mount = vault_mount.unwrap_or_else(|| "transit".to_string());
            validate_vault_segment("FIDUCIA_KV_VAULT_MOUNT", &mount)?;
            validate_vault_segment("FIDUCIA_KV_VAULT_KEY", &key_name)?;
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(10))
                .build()
                .map_err(|_| KvCryptoError::new("could not construct Vault Transit client"))?;
            return Ok(Some(Self::Vault(VaultTransit {
                address,
                mount,
                key_name,
                token,
                namespace: vault_namespace,
                client,
            })));
        }

        if versioned_present && legacy_present {
            return Err(KvCryptoError::new(
                "configure either FIDUCIA_KV_ENCRYPTION_KEYS or the legacy single key, not both",
            ));
        }

        if versioned_present {
            let raw = versioned_keys.ok_or_else(|| {
                KvCryptoError::new(
                    "FIDUCIA_KV_ENCRYPTION_KEYS is required with FIDUCIA_KV_ENCRYPTION_ACTIVE_KEY_ID",
                )
            })?;
            let encoded: HashMap<String, String> = serde_json::from_str(&raw).map_err(|_| {
                KvCryptoError::new(
                    "FIDUCIA_KV_ENCRYPTION_KEYS must be a JSON object of key ids to base64 keys",
                )
            })?;
            let active_key_id = active_key_id.ok_or_else(|| {
                KvCryptoError::new(
                    "FIDUCIA_KV_ENCRYPTION_ACTIVE_KEY_ID is required with the keyring",
                )
            })?;
            return Ok(Some(Self::Local(LocalKeyring::new(
                active_key_id,
                encoded,
            )?)));
        }

        if !legacy_present {
            return Ok(None);
        }
        let raw = legacy_key.ok_or_else(|| {
            KvCryptoError::new(
                "FIDUCIA_KV_ENCRYPTION_KEY is required with FIDUCIA_KV_ENCRYPTION_KEY_ID",
            )
        })?;
        let key_id = legacy_key_id.unwrap_or_else(|| "legacy".to_string());
        Ok(Some(Self::Local(LocalKeyring::new(
            key_id.clone(),
            HashMap::from([(key_id, raw)]),
        )?)))
    }

    pub fn posture(&self) -> String {
        match self {
            Self::Local(keyring) => format!(
                "local-keyring active_key_id={} readable_keys={}",
                keyring.active_key_id,
                keyring.keys.len()
            ),
            Self::Vault(vault) => format!(
                "vault-transit address={} mount={} key={}",
                vault.address, vault.mount, vault.key_name
            ),
        }
    }

    async fn seal(&self, storage_key: &str, plaintext: &str) -> Result<String, KvCryptoError> {
        match self {
            Self::Local(keyring) => keyring.seal(storage_key, plaintext),
            Self::Vault(vault) => vault.seal(storage_key, plaintext).await,
        }
    }

    async fn unseal(
        &self,
        storage_key: &str,
        stored: &str,
    ) -> Result<UnsealedValue, KvCryptoError> {
        match self {
            Self::Local(keyring) => keyring.unseal(storage_key, stored),
            Self::Vault(vault) => vault.unseal(storage_key, stored).await,
        }
    }
}

impl LocalKeyring {
    fn new(active_key_id: String, encoded: HashMap<String, String>) -> Result<Self, KvCryptoError> {
        validate_key_id(&active_key_id)?;
        if encoded.is_empty() {
            return Err(KvCryptoError::new(
                "FIDUCIA_KV_ENCRYPTION_KEYS must contain at least one key",
            ));
        }
        let mut keys = HashMap::new();
        for (key_id, value) in encoded {
            validate_key_id(&key_id)?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(value.trim())
                .map_err(|_| KvCryptoError::new(format!("KV key {key_id:?} is not base64")))?;
            let key: [u8; 32] = bytes.try_into().map_err(|_| {
                KvCryptoError::new(format!("KV key {key_id:?} must decode to exactly 32 bytes"))
            })?;
            keys.insert(key_id, Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key)));
        }
        if !keys.contains_key(&active_key_id) {
            return Err(KvCryptoError::new(
                "FIDUCIA_KV_ENCRYPTION_ACTIVE_KEY_ID is not present in the keyring",
            ));
        }
        Ok(Self {
            active_key_id,
            keys,
        })
    }

    fn seal(&self, storage_key: &str, plaintext: &str) -> Result<String, KvCryptoError> {
        let cipher = self
            .keys
            .get(&self.active_key_id)
            .ok_or_else(|| KvCryptoError::new("active KV key disappeared"))?;
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let aad = local_aad(&self.active_key_id, storage_key);
        let ciphertext = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext.as_bytes(),
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|_| KvCryptoError::new("KV encryption failed"))?;
        let mut blob = nonce.to_vec();
        blob.extend_from_slice(&ciphertext);
        Ok(format!(
            "{KV_ENVELOPE_V2_PREFIX}{}:{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&self.active_key_id),
            base64::engine::general_purpose::STANDARD.encode(blob)
        ))
    }

    fn unseal(&self, storage_key: &str, stored: &str) -> Result<UnsealedValue, KvCryptoError> {
        if let Some(rest) = stored.strip_prefix(KV_ENVELOPE_V2_PREFIX) {
            let (encoded_key_id, encoded_blob) = rest
                .split_once(':')
                .ok_or_else(|| KvCryptoError::new("malformed fcenc:v2 KV envelope"))?;
            let key_id = String::from_utf8(
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(encoded_key_id)
                    .map_err(|_| KvCryptoError::new("malformed fcenc:v2 key id"))?,
            )
            .map_err(|_| KvCryptoError::new("fcenc:v2 key id is not UTF-8"))?;
            let cipher = self.keys.get(&key_id).ok_or_else(|| {
                KvCryptoError::new(format!(
                    "KV ciphertext references unavailable key id {key_id:?}"
                ))
            })?;
            let blob = decode_cipher_blob(encoded_blob)?;
            let (nonce, ciphertext) = blob.split_at(12);
            let aad = local_aad(&key_id, storage_key);
            let plaintext = cipher
                .decrypt(
                    Nonce::from_slice(nonce),
                    Payload {
                        msg: ciphertext,
                        aad: aad.as_bytes(),
                    },
                )
                .map_err(|_| KvCryptoError::new("KV ciphertext authentication failed"))?;
            return decoded_plaintext(
                plaintext,
                KvProtection {
                    at_rest: "encrypted",
                    provider: Some("local_keyring"),
                    key_id: Some(key_id),
                    key_version: None,
                },
            );
        }

        if let Some(encoded_blob) = stored.strip_prefix(KV_ENVELOPE_V1_PREFIX) {
            let blob = decode_cipher_blob(encoded_blob)?;
            let (nonce, ciphertext) = blob.split_at(12);
            for (key_id, cipher) in &self.keys {
                if let Ok(plaintext) = cipher.decrypt(Nonce::from_slice(nonce), ciphertext) {
                    return decoded_plaintext(
                        plaintext,
                        KvProtection {
                            at_rest: "encrypted",
                            provider: Some("local_keyring_legacy"),
                            key_id: Some(key_id.clone()),
                            key_version: None,
                        },
                    );
                }
            }
            return Err(KvCryptoError::new(
                "legacy KV ciphertext did not authenticate with any configured key",
            ));
        }

        if is_fiducia_envelope(stored) {
            return Err(KvCryptoError::new(
                "KV ciphertext was created by a different protection provider",
            ));
        }
        Ok(UnsealedValue {
            value: stored.to_string(),
            protection: KvProtection::plaintext(),
        })
    }
}

impl VaultTransit {
    fn endpoint(&self, operation: &str) -> Result<Url, KvCryptoError> {
        let mut url = self.address.clone();
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|_| KvCryptoError::new("Vault address cannot hold path segments"))?;
            path.pop_if_empty();
            for segment in ["v1", self.mount.as_str(), operation, self.key_name.as_str()] {
                path.push(segment);
            }
        }
        Ok(url)
    }

    fn request(&self, url: Url) -> reqwest::RequestBuilder {
        let mut request = self
            .client
            .post(url)
            .header("x-vault-token", self.token.as_str());
        if let Some(namespace) = &self.namespace {
            request = request.header("x-vault-namespace", namespace);
        }
        request
    }

    async fn seal(&self, storage_key: &str, plaintext: &str) -> Result<String, KvCryptoError> {
        let response = self
            .request(self.endpoint("encrypt")?)
            .json(&json!({
                "plaintext": base64::engine::general_purpose::STANDARD.encode(plaintext),
                "context": base64::engine::general_purpose::STANDARD.encode(storage_key),
            }))
            .send()
            .await
            .map_err(|_| KvCryptoError::new("Vault Transit encrypt request failed"))?;
        if !response.status().is_success() {
            return Err(KvCryptoError::new(format!(
                "Vault Transit encrypt rejected with HTTP {}",
                response.status().as_u16()
            )));
        }
        let body = bounded_vault_json(response, "encrypt").await?;
        let ciphertext = body
            .pointer("/data/ciphertext")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| KvCryptoError::new("Vault Transit encrypt omitted ciphertext"))?;
        Ok(format!(
            "{KV_VAULT_ENVELOPE_PREFIX}{}",
            base64::engine::general_purpose::STANDARD.encode(ciphertext)
        ))
    }

    async fn unseal(
        &self,
        storage_key: &str,
        stored: &str,
    ) -> Result<UnsealedValue, KvCryptoError> {
        let Some(encoded) = stored.strip_prefix(KV_VAULT_ENVELOPE_PREFIX) else {
            if is_fiducia_envelope(stored) {
                return Err(KvCryptoError::new(
                    "KV ciphertext was created by a different protection provider",
                ));
            }
            return Ok(UnsealedValue {
                value: stored.to_string(),
                protection: KvProtection::plaintext(),
            });
        };
        let ciphertext = String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| KvCryptoError::new("malformed Vault KV envelope"))?,
        )
        .map_err(|_| KvCryptoError::new("Vault KV envelope is not UTF-8"))?;
        let response = self
            .request(self.endpoint("decrypt")?)
            .json(&json!({
                "ciphertext": ciphertext,
                "context": base64::engine::general_purpose::STANDARD.encode(storage_key),
            }))
            .send()
            .await
            .map_err(|_| KvCryptoError::new("Vault Transit decrypt request failed"))?;
        if !response.status().is_success() {
            return Err(KvCryptoError::new(format!(
                "Vault Transit decrypt rejected with HTTP {}",
                response.status().as_u16()
            )));
        }
        let body = bounded_vault_json(response, "decrypt").await?;
        let encoded_plaintext = body
            .pointer("/data/plaintext")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| KvCryptoError::new("Vault Transit decrypt omitted plaintext"))?;
        let plaintext = base64::engine::general_purpose::STANDARD
            .decode(encoded_plaintext)
            .map_err(|_| KvCryptoError::new("Vault Transit plaintext was not base64"))?;
        let key_version = ciphertext
            .split(':')
            .nth(1)
            .and_then(|value| value.strip_prefix('v'))
            .and_then(|value| value.parse().ok());
        decoded_plaintext(
            plaintext,
            KvProtection {
                at_rest: "encrypted",
                provider: Some("vault_transit"),
                key_id: Some(self.key_name.clone()),
                key_version,
            },
        )
    }
}

fn nonempty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

async fn bounded_vault_json(
    mut response: reqwest::Response,
    operation: &'static str,
) -> Result<serde_json::Value, KvCryptoError> {
    if response
        .content_length()
        .is_some_and(|length| length > VAULT_RESPONSE_MAX_BYTES as u64)
    {
        return Err(KvCryptoError::new(format!(
            "Vault Transit {operation} response exceeded {VAULT_RESPONSE_MAX_BYTES} bytes"
        )));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| KvCryptoError::new(format!("Vault Transit {operation} response failed")))?
    {
        if body.len().saturating_add(chunk.len()) > VAULT_RESPONSE_MAX_BYTES {
            return Err(KvCryptoError::new(format!(
                "Vault Transit {operation} response exceeded {VAULT_RESPONSE_MAX_BYTES} bytes"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body)
        .map_err(|_| KvCryptoError::new(format!("Vault Transit {operation} response was not JSON")))
}

fn validate_key_id(key_id: &str) -> Result<(), KvCryptoError> {
    if key_id.is_empty()
        || key_id.len() > 128
        || !key_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return Err(KvCryptoError::new(
            "KV key ids must be 1-128 ASCII letters, numbers, '.', '_' or '-'",
        ));
    }
    Ok(())
}

fn validate_vault_segment(name: &str, value: &str) -> Result<(), KvCryptoError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
    {
        return Err(KvCryptoError::new(format!(
            "{name} must be 1-128 ASCII letters, numbers, '_' or '-'"
        )));
    }
    Ok(())
}

fn validate_vault_address(raw: &str) -> Result<Url, KvCryptoError> {
    let mut url = Url::parse(raw)
        .map_err(|_| KvCryptoError::new("FIDUCIA_KV_VAULT_ADDR is not a valid URL"))?;
    let host = url
        .host_str()
        .ok_or_else(|| KvCryptoError::new("FIDUCIA_KV_VAULT_ADDR must include a host"))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
        || !matches!(url.scheme(), "http" | "https")
        || (url.scheme() == "http" && !cleartext_internal_host_allowed(host))
    {
        return Err(KvCryptoError::new(
            "Vault address must be HTTPS (or trusted internal HTTP) without credentials, path, query, or fragment",
        ));
    }
    url.set_path("");
    Ok(url)
}

/// Whether `host` names something inside the cluster's trust boundary. Vault
/// uses it to permit cleartext HTTP to such a host; [`crate::schedule`] uses the
/// same predicate with the opposite polarity, to refuse *delivering* to one.
pub(crate) fn cleartext_internal_host_allowed(host: &str) -> bool {
    let host = host.to_ascii_lowercase();
    if host == "localhost" || host.ends_with(".localhost") {
        return true;
    }
    if let Ok(address) = host.parse::<IpAddr>() {
        return match address {
            IpAddr::V4(address) => {
                address.is_loopback() || address.is_private() || address.is_link_local()
            }
            IpAddr::V6(address) => {
                let first = address.segments()[0];
                address.is_loopback() || (first & 0xfe00) == 0xfc00 || (first & 0xffc0) == 0xfe80
            }
        };
    }
    !host.contains('.')
        || [
            ".svc",
            ".svc.cluster.local",
            ".cluster.local",
            ".internal",
            ".local",
        ]
        .iter()
        .any(|suffix| host.ends_with(suffix))
}

fn local_aad(key_id: &str, storage_key: &str) -> String {
    format!("fiducia-kv:v2:{key_id}:{storage_key}")
}

fn decode_cipher_blob(encoded: &str) -> Result<Vec<u8>, KvCryptoError> {
    let blob = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| KvCryptoError::new("KV ciphertext is not valid base64"))?;
    if blob.len() < 12 + 16 {
        return Err(KvCryptoError::new(
            "KV ciphertext is too short for a nonce and authentication tag",
        ));
    }
    Ok(blob)
}

fn decoded_plaintext(
    plaintext: Vec<u8>,
    protection: KvProtection,
) -> Result<UnsealedValue, KvCryptoError> {
    Ok(UnsealedValue {
        value: String::from_utf8(plaintext)
            .map_err(|_| KvCryptoError::new("decrypted KV value is not UTF-8"))?,
        protection,
    })
}

fn is_fiducia_envelope(value: &str) -> bool {
    value.starts_with("fcenc:")
}

/// Reserved caller-facing keyspace used by the secrets client surface.
///
/// Keep this check at the node boundary as well as in clients: callers can use
/// the generic KV API directly, so client-side value stripping alone is not a
/// confidentiality boundary.
fn is_secret_keyspace(caller_key: &str) -> bool {
    caller_key == "secret" || caller_key.starts_with(SECRET_KEYSPACE_PREFIX)
}

/// Whether the trusted-hop identity may intentionally persist one plaintext KV
/// value. The load balancer strips all client-supplied `x-fiducia-*` headers and
/// injects one canonical space-separated scope header after authentication.
///
/// Fail closed when the header is absent, malformed, duplicated, or merely
/// carries `*`: plaintext is a separate administrative authority, not an
/// implication of broad data-plane access. Secret-delivery keys never permit an
/// opt-out, including for administrators.
fn plaintext_write_authorized(headers: &HeaderMap, caller_key: &str) -> bool {
    if is_secret_keyspace(caller_key) {
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
    node: &Node,
    storage_key: &str,
    value: String,
    plaintext_opt_out: bool,
) -> Result<String, KvCryptoError> {
    match node.kv_cipher() {
        Some(cipher) if !plaintext_opt_out => cipher.seal(storage_key, &value).await,
        _ => Ok(value),
    }
}

/// Unseal a read value with the node's cipher if configured.
async fn unseal_for_read(
    node: &Node,
    storage_key: &str,
    value: &str,
) -> Result<UnsealedValue, KvCryptoError> {
    match node.kv_cipher() {
        Some(cipher) => cipher.unseal(storage_key, value).await,
        None if is_fiducia_envelope(value) => Err(KvCryptoError::new(
            "encrypted KV value is unreadable because no protection backend is configured",
        )),
        None => Ok(UnsealedValue {
            value: value.to_string(),
            protection: KvProtection::plaintext(),
        }),
    }
}

#[derive(Debug, Deserialize)]
pub struct PutBody {
    pub value: String,
    pub ttl_ms: Option<u64>,
    /// Optional compare-and-swap guard: only write if the current revision
    /// equals this. `0` means "must not exist".
    pub prev_revision: Option<u64>,
    /// Opt this write out of at-rest encryption (stored verbatim). Defaults to
    /// encrypted whenever the cluster has a KV protection backend configured.
    #[serde(default)]
    pub plaintext: bool,
}

/// Query parameters shared by the KV verbs. `key` selects a single key;
/// `prefix` selects a range (for list / prefix-watch); `watch` switches a read
/// into an SSE stream.
#[derive(Debug, Default, Deserialize)]
pub struct KvParams {
    pub key: Option<String>,
    pub prefix: Option<String>,
    pub watch: Option<bool>,
}

pub fn router() -> Router<Arc<Node>> {
    Router::new().route("/", get(get_or_list).put(put_key).delete(delete_key))
}

/// `GET /v1/kv` — read a key, watch a key/prefix, or list a prefix, by query.
async fn get_or_list(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Query(q): Query<KvParams>,
) -> Response {
    if q.watch.unwrap_or(false) {
        return match (q.key, q.prefix) {
            (Some(key), _) => watch(node, org.scope(&key), false, org).await,
            (None, Some(prefix)) => watch(node, org.scope(&prefix), true, org).await,
            (None, None) => bad_request("watch requires `key` or `prefix`"),
        };
    }
    match q.key {
        // The caller's key is namespaced into their org before it reaches the
        // state machine; the response echoes the caller-facing key, not the scoped
        // one, so the isolation is invisible to the client.
        Some(key) => {
            let storage_key = org.scope(&key);
            match node
                .query(ReadRequest::Kv {
                    key: storage_key.clone(),
                })
                .await
            {
                Ok(ReadResponse::Kv(Some(mut entry))) => {
                    let unsealed = match unseal_for_read(&node, &storage_key, &entry.value).await {
                        Ok(value) => value,
                        Err(error) => return no_store(crypto_failure("decrypt", &error)),
                    };
                    entry.value = unsealed.value;
                    no_store(
                        Json(json!({
                            "key": key,
                            "found": true,
                            "entry": entry,
                            "protection": unsealed.protection,
                        }))
                        .into_response(),
                    )
                }
                Ok(ReadResponse::Kv(None)) => {
                    no_store(Json(json!({ "key": key, "found": false })).into_response())
                }
                Err(err) => no_store(read_error_response(err, &uri)),
                _ => no_store(Json(json!({ "error": "unavailable" })).into_response()),
            }
        }
        None => list(node, org, q.prefix.unwrap_or_default()).await,
    }
}

/// `PUT /v1/kv?key=K` — upsert (optionally compare-and-swap). Value in the body.
async fn put_key(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    headers: HeaderMap,
    uri: Uri,
    Query(q): Query<KvParams>,
    Json(body): Json<PutBody>,
) -> Response {
    let Some(key) = q.key else {
        return bad_request("missing `key`");
    };
    if body.plaintext && !plaintext_write_authorized(&headers, &key) {
        return plaintext_write_forbidden();
    }
    // Seal before the value enters the log, so ciphertext is what gets
    // replicated and persisted (log + snapshot). Sealing here (once) keeps the
    // replicated command byte-identical across replicas.
    let storage_key = org.scope(&key);
    let value = match seal_for_write(&node, &storage_key, body.value, body.plaintext).await {
        Ok(value) => value,
        Err(error) => return crypto_failure("encrypt", &error),
    };
    let result = node
        .propose(Command::KvPut {
            key: storage_key,
            value,
            ttl_ms: body.ttl_ms,
            prev_revision: body.prev_revision,
        })
        .await;
    org.propose_response(result, &uri)
}

/// `DELETE /v1/kv?key=K` — remove a key.
async fn delete_key(
    State(node): State<Arc<Node>>,
    org: OrgScope,
    uri: Uri,
    Query(q): Query<KvParams>,
) -> Response {
    let Some(key) = q.key else {
        return bad_request("missing `key`");
    };
    let result = node
        .propose(Command::KvDelete {
            key: org.scope(&key),
        })
        .await;
    org.propose_response(result, &uri)
}

/// `GET /v1/kv?prefix=...` — list live keys under a prefix, scoped to the caller's
/// org. The caller's prefix is namespaced, and every returned key is un-namespaced
/// back to the caller-facing form; keys outside the org's space are filtered out
/// (they never share the prefix), which is what makes the list read tenant-safe.
async fn list(node: Arc<Node>, org: OrgScope, prefix: String) -> Response {
    let scoped_prefix = org.scope(&prefix);
    let mut keys = Vec::new();
    for item in node.list_kv(&scoped_prefix).await {
        let storage_key = item.key.clone();
        let Some(unscoped) = org.unscope(&storage_key) else {
            continue;
        };
        let unsealed = match unseal_for_read(&node, &storage_key, &item.entry.value).await {
            Ok(value) => value,
            Err(error) => return no_store(crypto_failure("decrypt", &error)),
        };
        keys.push(list_row(
            unscoped,
            unsealed,
            item.entry.mod_revision,
            item.entry.expires_at_ms,
        ));
    }
    no_store(Json(json!({ "prefix": prefix, "count": keys.len(), "keys": keys })).into_response())
}

/// Render one prefix-list row. Reserved secret values are deliberately absent:
/// an explicit single-key GET is the only API operation that may reveal one.
fn list_row(
    caller_key: &str,
    unsealed: UnsealedValue,
    mod_revision: u64,
    expires_at_ms: Option<u64>,
) -> serde_json::Value {
    let UnsealedValue { value, protection } = unsealed;
    let mut row = json!({
        "key": caller_key,
        "mod_revision": mod_revision,
        "protection": protection,
    });
    if is_secret_keyspace(caller_key) {
        row["value_redacted"] = json!(true);
    } else {
        row["value"] = json!(value);
    }
    if let Some(expires_at_ms) = expires_at_ms {
        row["expires_at_ms"] = json!(expires_at_ms);
    }
    row
}

/// SSE stream of change events for a key (or, when `prefix`, every key under it).
///
/// Subscribes to the owning shard's change broadcast and pushes one SSE event per
/// committed put/delete that matches. The connection is long-lived (no request
/// timeout layer) with periodic keep-alive comments.
async fn watch(node: Arc<Node>, key: String, prefix: bool, org: OrgScope) -> Response {
    if prefix {
        return watch_prefix(node, key, org).await;
    }
    let Some(rx) = node.watch(&key).await else {
        return Json(json!({ "error": "unavailable", "op": "kv.watch" })).into_response();
    };
    let stream = BroadcastStream::new(rx).filter_map(move |item| {
        let event = item.ok()?; // drop lag/closed notifications
        if event.scope != "kv" {
            return None; // ignore election/service changes on the shared shard stream
        }
        if event.key != key {
            return None;
        }
        Some(Ok::<Event, Infallible>(unscoped_change_event(&event, &org)))
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

async fn watch_prefix(node: Arc<Node>, prefix: String, org: OrgScope) -> Response {
    let receivers = node.watch_all().await;
    if receivers.is_empty() {
        return Json(json!({ "error": "unavailable", "op": "kv.watch" })).into_response();
    }

    let mut streams = StreamMap::new();
    for (idx, receiver) in receivers.into_iter().enumerate() {
        streams.insert(idx, BroadcastStream::new(receiver));
    }
    let stream = streams.filter_map(move |(_, item)| {
        let event = item.ok()?;
        if !is_kv_change(event.kind) || !event.key.starts_with(&prefix) {
            return None;
        }
        Some(Ok::<Event, Infallible>(unscoped_change_event(&event, &org)))
    });

    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

/// Emit a KV change event with its key un-namespaced back to the caller-facing
/// form, so a watcher never sees another org's key or the internal prefix.
fn unscoped_change_event<T: serde::Serialize>(event: &T, org: &OrgScope) -> Event {
    let mut value = match serde_json::to_value(event) {
        Ok(v) => v,
        Err(_) => return Event::default().comment("serialize-error"),
    };
    if let Some(scoped) = value.get("key").and_then(|k| k.as_str()) {
        match org.unscope(scoped) {
            Some(caller_key) => value["key"] = json!(caller_key),
            None => return Event::default().comment("out-of-scope"),
        }
    }
    let kind = value
        .get("kind")
        .and_then(|k| k.as_str())
        .unwrap_or("change")
        .to_string();
    Event::default().event(kind).data(value.to_string())
}

fn is_kv_change(kind: &str) -> bool {
    matches!(kind, "put" | "delete")
}

fn bad_request(detail: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": "bad_request", "detail": detail })),
    )
        .into_response()
}

fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn plaintext_write_forbidden() -> Response {
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
    tracing::error!(operation, error = %error, "KV protection operation failed");
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({
            "error": "kv_protection_unavailable",
            "detail": "the KV value could not be protected or read safely",
        })),
    )
        .into_response()
}

#[cfg(test)]
mod kv_cipher_tests {
    use super::*;
    use axum::extract::Path;
    use axum::http::HeaderMap;
    use axum::routing::post;

    fn encoded_key(byte: u8) -> String {
        base64::engine::general_purpose::STANDARD.encode([byte; 32])
    }

    fn keyring(active_key_id: &str, keys: &[(&str, u8)]) -> LocalKeyring {
        LocalKeyring::new(
            active_key_id.to_string(),
            keys.iter()
                .map(|(key_id, byte)| ((*key_id).to_string(), encoded_key(*byte)))
                .collect(),
        )
        .expect("valid test keyring")
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
    fn prefix_listing_redacts_reserved_secret_values_at_the_server_boundary() {
        let protection = KvProtection {
            at_rest: "encrypted",
            provider: Some("local_keyring"),
            key_id: Some("current".to_string()),
            key_version: None,
        };
        let secret = list_row(
            "secret/database-password",
            UnsealedValue {
                value: "must-not-leak".to_string(),
                protection: protection.clone(),
            },
            7,
            Some(99),
        );
        assert!(secret.get("value").is_none());
        assert_eq!(secret["value_redacted"], true);
        assert_eq!(secret["mod_revision"], 7);
        assert_eq!(secret["expires_at_ms"], 99);

        let config = list_row(
            "flags/checkout",
            UnsealedValue {
                value: "enabled".to_string(),
                protection,
            },
            8,
            None,
        );
        assert_eq!(config["value"], "enabled");
        assert!(config.get("value_redacted").is_none());
    }

    #[test]
    fn kv_read_responses_are_explicitly_non_cacheable() {
        let response = no_store(Json(json!({ "found": true })).into_response());
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    }

    #[test]
    fn seal_then_unseal_round_trips_with_protection_metadata() {
        let c = keyring("2026-07", &[("2026-07", 7)]);
        let sealed = c
            .seal("org/acme/flags/new-checkout", "flags/new-checkout=on")
            .expect("seal");
        assert!(sealed.starts_with(KV_ENVELOPE_V2_PREFIX));
        assert!(!sealed.contains("new-checkout")); // ciphertext hides plaintext
        let unsealed = c
            .unseal("org/acme/flags/new-checkout", &sealed)
            .expect("unseal");
        assert_eq!(unsealed.value, "flags/new-checkout=on");
        assert_eq!(
            unsealed.protection,
            KvProtection {
                at_rest: "encrypted",
                provider: Some("local_keyring"),
                key_id: Some("2026-07".to_string()),
                key_version: None,
            }
        );
    }

    #[test]
    fn plaintext_passes_through_but_malformed_envelopes_fail_closed() {
        let c = keyring("current", &[("current", 7)]);
        let unsealed = c.unseal("org/acme/key", "just-plaintext").expect("plain");
        assert_eq!(unsealed.value, "just-plaintext");
        assert_eq!(unsealed.protection, KvProtection::plaintext());
        assert!(c.unseal("org/acme/key", "fcenc:v2:missing-fields").is_err());
        assert!(c
            .unseal("org/acme/key", "fcenc:vault:v1:not-base64!!")
            .is_err());
    }

    #[test]
    fn nonce_is_random_so_ciphertexts_differ_but_both_decrypt() {
        let c = keyring("current", &[("current", 7)]);
        let a = c.seal("org/acme/key", "same").expect("seal a");
        let b = c.seal("org/acme/key", "same").expect("seal b");
        assert_ne!(a, b, "random nonce should vary the ciphertext");
        assert_eq!(
            c.unseal("org/acme/key", &a).expect("unseal a").value,
            "same"
        );
        assert_eq!(
            c.unseal("org/acme/key", &b).expect("unseal b").value,
            "same"
        );
    }

    #[test]
    fn wrong_key_tamper_and_cross_key_copy_fail_closed() {
        let c = keyring("current", &[("current", 7)]);
        let sealed = c.seal("org/acme/source", "secret-value").expect("seal");
        let other = keyring("current", &[("current", 9)]);
        assert!(other.unseal("org/acme/source", &sealed).is_err());
        assert!(c.unseal("org/acme/destination", &sealed).is_err());
        let mut tampered = sealed.clone();
        tampered.push('A');
        assert!(c.unseal("org/acme/source", &tampered).is_err());
    }

    #[test]
    fn keyring_rotation_reads_old_values_and_writes_with_the_active_key() {
        let old = keyring("key-1", &[("key-1", 1)]);
        let old_value = old.seal("org/acme/key", "old value").expect("old seal");

        let rotated = keyring("key-2", &[("key-1", 1), ("key-2", 2)]);
        assert_eq!(
            rotated
                .unseal("org/acme/key", &old_value)
                .expect("old value remains readable")
                .value,
            "old value"
        );
        let new_value = rotated.seal("org/acme/key", "new value").expect("new seal");
        assert!(new_value.contains("a2V5LTI:"));
        assert!(old.unseal("org/acme/key", &new_value).is_err());
    }

    #[test]
    fn backend_configuration_rejects_partial_and_conflicting_inputs() {
        let load = |values: &[(&str, &str)]| {
            KvCipher::from_lookup(|name| {
                values
                    .iter()
                    .find(|(candidate, _)| *candidate == name)
                    .map(|(_, value)| (*value).to_string())
            })
        };

        assert!(load(&[]).expect("empty config").is_none());
        assert!(load(&[("FIDUCIA_KV_VAULT_MOUNT", "transit")])
            .expect_err("partial Vault config")
            .to_string()
            .contains("FIDUCIA_KV_VAULT_ADDR"));
        assert!(load(&[("FIDUCIA_KV_ENCRYPTION_ACTIVE_KEY_ID", "current")])
            .expect_err("partial keyring")
            .to_string()
            .contains("FIDUCIA_KV_ENCRYPTION_KEYS"));
        assert!(load(&[("FIDUCIA_KV_ENCRYPTION_KEY_ID", "legacy-1")])
            .expect_err("partial legacy config")
            .to_string()
            .contains("FIDUCIA_KV_ENCRYPTION_KEY is required"));
        assert!(load(&[
            ("FIDUCIA_KV_VAULT_ADDR", "https://vault.example.com"),
            ("FIDUCIA_KV_VAULT_TOKEN", "token"),
            ("FIDUCIA_KV_VAULT_KEY", "fiducia-kv"),
            ("FIDUCIA_KV_ENCRYPTION_KEY", "not-inspected"),
        ])
        .expect_err("conflicting backends")
        .to_string()
        .contains("exactly one KV protection backend"));

        let encoded = encoded_key(7);
        let keyring_json = format!(r#"{{"current":"{encoded}"}}"#);
        let cipher = load(&[
            ("FIDUCIA_KV_ENCRYPTION_KEYS", keyring_json.as_str()),
            ("FIDUCIA_KV_ENCRYPTION_ACTIVE_KEY_ID", "current"),
        ])
        .expect("valid keyring")
        .expect("configured cipher");
        assert!(matches!(cipher, KvCipher::Local(_)));
    }

    #[tokio::test]
    async fn vault_transit_round_trip_binds_context_and_reports_key_version() {
        async fn transit(
            Path((operation, key_name)): Path<(String, String)>,
            headers: HeaderMap,
            Json(body): Json<serde_json::Value>,
        ) -> Json<serde_json::Value> {
            assert_eq!(key_name, "fiducia-kv");
            assert_eq!(headers["x-vault-token"], "test-token");
            assert_eq!(headers["x-vault-namespace"], "engineering");
            assert_eq!(
                body["context"],
                base64::engine::general_purpose::STANDARD.encode("org/acme/database/password")
            );
            match operation.as_str() {
                "encrypt" => Json(json!({
                    "data": {
                        "ciphertext": format!("vault:v7:{}", body["plaintext"].as_str().unwrap())
                    }
                })),
                "decrypt" => {
                    let plaintext = body["ciphertext"]
                        .as_str()
                        .and_then(|value| value.split_once("vault:v7:"))
                        .map(|(_, plaintext)| plaintext)
                        .expect("test ciphertext");
                    Json(json!({ "data": { "plaintext": plaintext } }))
                }
                other => panic!("unexpected Vault operation: {other}"),
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake Vault");
        let address = listener.local_addr().expect("fake Vault address");
        let app = Router::new().route("/v1/transit/:operation/:key", post(transit));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve fake Vault");
        });
        let vault = VaultTransit {
            address: Url::parse(&format!("http://{address}")).expect("Vault URL"),
            mount: "transit".to_string(),
            key_name: "fiducia-kv".to_string(),
            token: "test-token".to_string(),
            namespace: Some("engineering".to_string()),
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("HTTP client"),
        };

        let sealed = vault
            .seal("org/acme/database/password", "correct horse")
            .await
            .expect("Vault encrypt");
        assert!(sealed.starts_with(KV_VAULT_ENVELOPE_PREFIX));
        assert!(!sealed.contains("correct horse"));
        let unsealed = vault
            .unseal("org/acme/database/password", &sealed)
            .await
            .expect("Vault decrypt");
        assert_eq!(unsealed.value, "correct horse");
        assert_eq!(
            unsealed.protection,
            KvProtection {
                at_rest: "encrypted",
                provider: Some("vault_transit"),
                key_id: Some("fiducia-kv".to_string()),
                key_version: Some(7),
            }
        );
        server.abort();
    }

    #[tokio::test]
    async fn vault_transit_rejects_oversized_responses() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake Vault");
        let address = listener.local_addr().expect("fake Vault address");
        let app = Router::new().route(
            "/v1/transit/encrypt/fiducia-kv",
            post(|| async { "x".repeat(VAULT_RESPONSE_MAX_BYTES + 1) }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve fake Vault");
        });
        let vault = VaultTransit {
            address: Url::parse(&format!("http://{address}")).expect("Vault URL"),
            mount: "transit".to_string(),
            key_name: "fiducia-kv".to_string(),
            token: "test-token".to_string(),
            namespace: None,
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("HTTP client"),
        };

        let error = vault
            .seal("org/acme/key", "secret")
            .await
            .expect_err("oversized response must fail closed");
        assert!(error.to_string().contains("exceeded"));
        server.abort();
    }

    #[test]
    fn vault_address_rejects_cleartext_public_and_credentialed_urls() {
        assert!(validate_vault_address("http://vault.example.com").is_err());
        assert!(validate_vault_address("https://user:pass@vault.example.com").is_err());
        assert!(validate_vault_address("http://vault.vault.svc:8200").is_ok());
        assert!(validate_vault_address("https://vault.example.com:8200").is_ok());
    }
}
