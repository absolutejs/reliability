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

## Installation

```sh
bun add @absolutejs/reliability
```

## Idempotent operations

```ts
import {
	createMemoryIdempotentOperationStore,
	fingerprintPayload,
	operationId
} from '@absolutejs/reliability';

const scope = { actorId: 'tenant-1', key: 'charge-42', operation: 'charge' };
const id = operationId(scope);
const fingerprint = await fingerprintPayload({ amount: 2500, currency: 'usd' });
const store = createMemoryIdempotentOperationStore();
```

Use `createPostgresIdempotentOperationStore()` in multi-process production deployments. The store fences concurrent claims and preserves an `indeterminate` result when an external side effect may have started but its final outcome is unknown.

## Durable webhook inbox

`createMemoryWebhookInboxStore()` covers tests. `createPostgresWebhookInboxStore()` and `drainWebhookInbox()` provide atomic leases, retries, replay protection, completion, and retention purging for production webhook consumers.

## Transaction affinity

`createPostgresTransactionRunner()` checks out one connection for the complete transaction. This prevents transaction statements from accidentally crossing pooled connections.

## License

BSL-1.1.
