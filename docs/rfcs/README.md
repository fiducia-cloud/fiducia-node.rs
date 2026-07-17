# docs/rfcs — design RFCs

Numbered proposals for new primitives or protocol changes that are bigger than
a design note in `../`. Each RFC records motivation, non-goals, an API sketch,
and its status header (Draft / Accepted / Implemented); the RFC stays as the
durable rationale even after implementation.

- `rfc-0001-reservations.md` — reservations primitive: long-TTL,
  capacity-aware, externally-owned leases (distinct from locks, whose holder
  liveness releases them). Status: Draft.
