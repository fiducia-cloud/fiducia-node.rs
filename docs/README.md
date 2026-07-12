# docs — design notes

Prose documentation about how the node works, kept alongside the code it
describes. Currently `storage.md`, which explains what actually backs the
coordination primitives (the owning shard's replicated Raft log + in-memory
applied state — *not* Postgres/Supabase/Redis) and where durable business data
lives instead.

These are reference/design notes for engineers, distinct from the user-facing
API overview in the repo-root `README.md`.
