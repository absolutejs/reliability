import type { SqlClient, TransactionRunner } from "./transaction";

export type OperationScope = {
  account?: string;
  key: string;
  namespace: string;
  provider?: string;
  tenant?: string;
};

export type OperationClaim<Result> =
  | { disposition: "claimed"; operationId: string; token: string }
  | { disposition: "completed"; operationId: string; result: Result }
  | { disposition: "conflict"; operationId: string }
  | { disposition: "in-flight"; operationId: string }
  | { disposition: "indeterminate"; operationId: string; reason?: string };

export type IdempotentOperationStore<Result = unknown> = {
  begin: (input: {
    fingerprint: string;
    leaseMs: number;
    now?: number;
    scope: OperationScope;
  }) => Promise<OperationClaim<Result>>;
  complete: (
    operationId: string,
    token: string,
    result: Result,
    now?: number,
  ) => Promise<void>;
  markExecuting: (
    operationId: string,
    token: string,
    now?: number,
  ) => Promise<void>;
  markIndeterminate: (
    operationId: string,
    token: string,
    reason?: string,
    now?: number,
  ) => Promise<void>;
  releasePrepared: (operationId: string, token: string) => Promise<void>;
  renew: (
    operationId: string,
    token: string,
    leaseMs: number,
    now?: number,
  ) => Promise<void>;
};

export const operationId = (scope: OperationScope) =>
  JSON.stringify([
    scope.namespace,
    scope.provider ?? null,
    scope.account ?? null,
    scope.tenant ?? null,
    scope.key,
  ]);

const canonical = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(canonical);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, item]) => [key, canonical(item)]),
    );
  }
  return value;
};

export const fingerprintPayload = async (payload: unknown) => {
  const bytes = new TextEncoder().encode(JSON.stringify(canonical(payload)));
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((value) => value.toString(16).padStart(2, "0"))
    .join("");
};

type Entry<Result> = {
  fingerprint: string;
  leaseUntil: number;
  reason?: string;
  result?: Result;
  state: "completed" | "executing" | "indeterminate" | "prepared";
  token?: string;
};

export const createMemoryIdempotentOperationStore = <Result>(): IdempotentOperationStore<Result> => {
  const entries = new Map<string, Entry<Result>>();
  const getOwned = (id: string, token: string) => {
    const entry = entries.get(id);
    if (entry?.token !== token) throw new Error("invalid operation fencing token");
    return entry;
  };
  return {
    begin: async ({ fingerprint, leaseMs, now = Date.now(), scope }) => {
      const id = operationId(scope);
      const found = entries.get(id);
      if (found !== undefined && found.fingerprint !== fingerprint) {
        return { disposition: "conflict", operationId: id };
      }
      if (found?.state === "completed") {
        return { disposition: "completed", operationId: id, result: found.result as Result };
      }
      if (found?.state === "indeterminate" || (found?.state === "executing" && found.leaseUntil <= now)) {
        found.state = "indeterminate";
        delete found.token;
        return { disposition: "indeterminate", operationId: id, ...(found.reason ? { reason: found.reason } : {}) };
      }
      if (found !== undefined && found.leaseUntil > now) {
        return { disposition: "in-flight", operationId: id };
      }
      const token = crypto.randomUUID();
      entries.set(id, { fingerprint, leaseUntil: now + leaseMs, state: "prepared", token });
      return { disposition: "claimed", operationId: id, token };
    },
    complete: async (id, token, result) => {
      const entry = getOwned(id, token);
      entry.state = "completed";
      entry.result = structuredClone(result);
      delete entry.token;
    },
    markExecuting: async (id, token) => {
      getOwned(id, token).state = "executing";
    },
    markIndeterminate: async (id, token, reason) => {
      const entry = getOwned(id, token);
      entry.state = "indeterminate";
      entry.reason = reason;
      delete entry.token;
    },
    releasePrepared: async (id, token) => {
      const entry = getOwned(id, token);
      if (entry.state !== "prepared") throw new Error("cannot release an operation after execution started");
      entries.delete(id);
    },
    renew: async (id, token, leaseMs, now = Date.now()) => {
      getOwned(id, token).leaseUntil = now + leaseMs;
    },
  };
};

export const IDEMPOTENT_OPERATION_POSTGRES_SCHEMA = `
CREATE TABLE IF NOT EXISTS absolute_idempotent_operations (
  operation_id text PRIMARY KEY,
  fingerprint text NOT NULL,
  state text NOT NULL CHECK (state IN ('prepared', 'executing', 'completed', 'indeterminate')),
  fencing_token text,
  lease_until_ms bigint,
  result jsonb,
  reason text,
  updated_at_ms bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS absolute_idempotent_operations_lease_idx
  ON absolute_idempotent_operations (lease_until_ms)
  WHERE state IN ('prepared', 'executing');
`;

const requireOwned = async (client: SqlClient, id: string, token: string) => {
  const result = await client.query(
    "SELECT state FROM absolute_idempotent_operations WHERE operation_id = $1 AND fencing_token = $2 FOR UPDATE",
    [id, token],
  );
  if (result.rows.length !== 1) throw new Error("invalid operation fencing token");
  return String(result.rows[0]?.state);
};

export const createPostgresIdempotentOperationStore = <Result>(
  runner: TransactionRunner,
): IdempotentOperationStore<Result> => ({
  begin: ({ fingerprint, leaseMs, now = Date.now(), scope }) =>
    runner.transaction(async (client) => {
      const id = operationId(scope);
      const result = await client.query(
        "SELECT fingerprint, state, fencing_token, lease_until_ms, result, reason FROM absolute_idempotent_operations WHERE operation_id = $1 FOR UPDATE",
        [id],
      );
      const row = result.rows[0];
      if (row !== undefined && row.fingerprint !== fingerprint)
        return { disposition: "conflict", operationId: id };
      if (row?.state === "completed")
        return { disposition: "completed", operationId: id, result: row.result as Result };
      if (row?.state === "indeterminate" || (row?.state === "executing" && Number(row.lease_until_ms) <= now)) {
        await client.query(
          "UPDATE absolute_idempotent_operations SET state = 'indeterminate', fencing_token = NULL, updated_at_ms = $2 WHERE operation_id = $1",
          [id, now],
        );
        return { disposition: "indeterminate", operationId: id, ...(row?.reason ? { reason: String(row.reason) } : {}) };
      }
      if (row !== undefined && Number(row.lease_until_ms) > now)
        return { disposition: "in-flight", operationId: id };
      const token = crypto.randomUUID();
      await client.query(
        `INSERT INTO absolute_idempotent_operations
          (operation_id, fingerprint, state, fencing_token, lease_until_ms, updated_at_ms)
         VALUES ($1, $2, 'prepared', $3, $4, $5)
         ON CONFLICT (operation_id) DO UPDATE SET
           state = 'prepared', fencing_token = EXCLUDED.fencing_token,
           lease_until_ms = EXCLUDED.lease_until_ms, updated_at_ms = EXCLUDED.updated_at_ms`,
        [id, fingerprint, token, now + leaseMs, now],
      );
      return { disposition: "claimed", operationId: id, token };
    }),
  complete: (id, token, result, now = Date.now()) =>
    runner.transaction(async (client) => {
      await requireOwned(client, id, token);
      await client.query(
        "UPDATE absolute_idempotent_operations SET state = 'completed', result = $3::jsonb, fencing_token = NULL, lease_until_ms = NULL, updated_at_ms = $4 WHERE operation_id = $1 AND fencing_token = $2",
        [id, token, JSON.stringify(result), now],
      );
    }),
  markExecuting: (id, token, now = Date.now()) =>
    runner.transaction(async (client) => {
      await requireOwned(client, id, token);
      await client.query(
        "UPDATE absolute_idempotent_operations SET state = 'executing', updated_at_ms = $3 WHERE operation_id = $1 AND fencing_token = $2",
        [id, token, now],
      );
    }),
  markIndeterminate: (id, token, reason, now = Date.now()) =>
    runner.transaction(async (client) => {
      await requireOwned(client, id, token);
      await client.query(
        "UPDATE absolute_idempotent_operations SET state = 'indeterminate', reason = $3, fencing_token = NULL, lease_until_ms = NULL, updated_at_ms = $4 WHERE operation_id = $1 AND fencing_token = $2",
        [id, token, reason ?? null, now],
      );
    }),
  releasePrepared: (id, token) =>
    runner.transaction(async (client) => {
      const state = await requireOwned(client, id, token);
      if (state !== "prepared") throw new Error("cannot release an operation after execution started");
      await client.query(
        "DELETE FROM absolute_idempotent_operations WHERE operation_id = $1 AND fencing_token = $2",
        [id, token],
      );
    }),
  renew: (id, token, leaseMs, now = Date.now()) =>
    runner.transaction(async (client) => {
      await requireOwned(client, id, token);
      await client.query(
        "UPDATE absolute_idempotent_operations SET lease_until_ms = $3, updated_at_ms = $4 WHERE operation_id = $1 AND fencing_token = $2",
        [id, token, now + leaseMs, now],
      );
    }),
});
