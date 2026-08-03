# Cron webhook authenticity

`fiducia-node` can authenticate cron webhook deliveries with an HMAC-SHA256
signature. Configure the node process with:

```text
FIDUCIA_CRON_WEBHOOK_SIGNING_SECRET=<at least 32 bytes>
```

When configured, every `webhook` schedule delivery includes:

```text
X-Fiducia-Signature-256: sha256=<64 lowercase hexadecimal characters>
```

The MAC covers the exact UTF-8 JSON bytes sent in the HTTP request body. The
body is serialized and signed once per durable fire, before retry processing, so
retries preserve the same body, signature, `X-Fiducia-Schedule`, and
`Idempotency-Key`. Receivers should verify the signature over the raw body before
JSON parsing, compare the digest in constant time, validate the schedule name and
idempotency key, and reject missing signatures.

The secret is operator configuration only. It is never placed in a schedule, the
Raft log, delivery body, result history, metrics, or tracing fields. The setting
is intentionally opt-in for backward compatibility; webhook receivers that
require authentication should fail closed until every delivering node has the
same secret configured.

The secret currently defines one node-cluster webhook trust domain. Do not share
it with untrusted receivers. Rotate it by first making receivers accept both the
old and new secret, rolling the new value across all nodes, and then removing the
old receiver secret.
