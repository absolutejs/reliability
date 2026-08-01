# @absolutejs/reliability

Provider-neutral reliability primitives for side-effecting integrations:

- checked-out PostgreSQL transactions that keep every statement on one connection;
- scoped, payload-fingerprinted idempotent operations with fencing and an explicit
  `indeterminate` state after an external side effect may have started;
- an atomic webhook inbox with leases, replay claims, completion, and retention purge.

The package has no database-driver dependency. Pass a pool with `connect()` to
`createPostgresTransactionRunner()`, apply the exported schemas, and construct
the stores. A plain pool-level `query()` is intentionally not accepted because
it cannot guarantee transaction connection affinity.

## License

BSL-1.1.
