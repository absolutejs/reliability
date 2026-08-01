import type { TransactionRunner } from "./transaction";

export type WebhookInboxEvent<Payload = unknown> = {
  eventId: string;
  occurredAt: number;
  payload: Payload;
  provider: string;
  streamId: string;
};

export type WebhookInboxClaim<Payload = unknown> = {
  disposition: "accepted" | "duplicate";
  event: WebhookInboxEvent<Payload>;
  token?: string;
};

export type WebhookInboxStore<Payload = unknown> = {
  accept: (
    event: WebhookInboxEvent<Payload>,
    options?: { leaseMs?: number; now?: number },
  ) => Promise<WebhookInboxClaim<Payload>>;
  claimPending: (options?: {
    leaseMs?: number;
    limit?: number;
    now?: number;
  }) => Promise<Array<WebhookInboxClaim<Payload> & { token: string }>>;
  complete: (eventId: string, token: string, now?: number) => Promise<void>;
  release: (eventId: string, token: string) => Promise<void>;
  purgeCompleted: (before: number) => Promise<number>;
};

type MemoryEntry<Payload> = {
  completedAt?: number;
  event: WebhookInboxEvent<Payload>;
  leaseUntil?: number;
  token?: string;
};

export const createMemoryWebhookInboxStore = <Payload>(): WebhookInboxStore<Payload> => {
  const entries = new Map<string, MemoryEntry<Payload>>();
  const claim = (entry: MemoryEntry<Payload>, now: number, leaseMs: number) => {
    const token = crypto.randomUUID();
    entry.token = token;
    entry.leaseUntil = now + leaseMs;
    return token;
  };
  return {
    accept: async (event, { leaseMs = 60_000, now = Date.now() } = {}) => {
      const found = entries.get(event.eventId);
      if (found !== undefined) {
        if (found.completedAt !== undefined || (found.leaseUntil ?? 0) > now)
          return { disposition: "duplicate", event: structuredClone(found.event) };
        return { disposition: "duplicate", event: structuredClone(found.event), token: claim(found, now, leaseMs) };
      }
      const entry = { event: structuredClone(event) };
      entries.set(event.eventId, entry);
      return { disposition: "accepted", event: structuredClone(event), token: claim(entry, now, leaseMs) };
    },
    claimPending: async ({ leaseMs = 60_000, limit = 100, now = Date.now() } = {}) =>
      [...entries.values()]
        .filter((entry) => entry.completedAt === undefined && (entry.leaseUntil ?? 0) <= now)
        .slice(0, limit)
        .map((entry) => ({ disposition: "duplicate" as const, event: structuredClone(entry.event), token: claim(entry, now, leaseMs) })),
    complete: async (eventId, token, now = Date.now()) => {
      const entry = entries.get(eventId);
      if (entry?.token !== token) throw new Error("invalid webhook inbox fencing token");
      entry.completedAt = now;
      delete entry.token;
      delete entry.leaseUntil;
    },
    release: async (eventId, token) => {
      const entry = entries.get(eventId);
      if (entry?.token !== token) throw new Error("invalid webhook inbox fencing token");
      delete entry.token;
      delete entry.leaseUntil;
    },
    purgeCompleted: async (before) => {
      let count = 0;
      for (const [id, entry] of entries) {
        if (entry.completedAt !== undefined && entry.completedAt < before) {
          entries.delete(id);
          count += 1;
        }
      }
      return count;
    },
  };
};

export const WEBHOOK_INBOX_POSTGRES_SCHEMA = `
CREATE TABLE IF NOT EXISTS absolute_webhook_inbox (
  event_id text PRIMARY KEY,
  provider text NOT NULL,
  stream_id text NOT NULL,
  occurred_at_ms bigint NOT NULL,
  payload jsonb NOT NULL,
  fencing_token text,
  lease_until_ms bigint,
  completed_at_ms bigint,
  created_at_ms bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS absolute_webhook_inbox_pending_idx
  ON absolute_webhook_inbox (lease_until_ms, created_at_ms)
  WHERE completed_at_ms IS NULL;
`;

const fromRow = <Payload>(row: Record<string, unknown>): WebhookInboxEvent<Payload> => ({
  eventId: String(row.event_id),
  occurredAt: Number(row.occurred_at_ms),
  payload: row.payload as Payload,
  provider: String(row.provider),
  streamId: String(row.stream_id),
});

export const createPostgresWebhookInboxStore = <Payload>(
  runner: TransactionRunner,
): WebhookInboxStore<Payload> => ({
  accept: (event, { leaseMs = 60_000, now = Date.now() } = {}) =>
    runner.transaction(async (client) => {
      const inserted = await client.query(
        `INSERT INTO absolute_webhook_inbox
          (event_id, provider, stream_id, occurred_at_ms, payload, created_at_ms)
         VALUES ($1, $2, $3, $4, $5::jsonb, $6)
         ON CONFLICT (event_id) DO NOTHING RETURNING event_id`,
        [event.eventId, event.provider, event.streamId, event.occurredAt, JSON.stringify(event.payload), now],
      );
      const found = await client.query(
        "SELECT event_id, provider, stream_id, occurred_at_ms, payload, fencing_token, lease_until_ms, completed_at_ms FROM absolute_webhook_inbox WHERE event_id = $1 FOR UPDATE",
        [event.eventId],
      );
      const row = found.rows[0]!;
      const disposition = inserted.rows.length === 1 ? "accepted" : "duplicate";
      if (row.completed_at_ms !== null && row.completed_at_ms !== undefined)
        return { disposition, event: fromRow<Payload>(row) };
      if (row.fencing_token && Number(row.lease_until_ms) > now)
        return { disposition, event: fromRow<Payload>(row) };
      const token = crypto.randomUUID();
      await client.query(
        "UPDATE absolute_webhook_inbox SET fencing_token = $2, lease_until_ms = $3 WHERE event_id = $1",
        [event.eventId, token, now + leaseMs],
      );
      return { disposition, event: fromRow<Payload>(row), token };
    }),
  claimPending: ({ leaseMs = 60_000, limit = 100, now = Date.now() } = {}) =>
    runner.transaction(async (client) => {
      const found = await client.query(
        `SELECT event_id, provider, stream_id, occurred_at_ms, payload
         FROM absolute_webhook_inbox
         WHERE completed_at_ms IS NULL AND (lease_until_ms IS NULL OR lease_until_ms <= $1)
         ORDER BY created_at_ms ASC LIMIT $2 FOR UPDATE SKIP LOCKED`,
        [now, limit],
      );
      const claims = [];
      for (const row of found.rows) {
        const token = crypto.randomUUID();
        await client.query(
          "UPDATE absolute_webhook_inbox SET fencing_token = $2, lease_until_ms = $3 WHERE event_id = $1",
          [row.event_id, token, now + leaseMs],
        );
        claims.push({ disposition: "duplicate" as const, event: fromRow<Payload>(row), token });
      }
      return claims;
    }),
  complete: (eventId, token, now = Date.now()) =>
    runner.transaction(async (client) => {
      const result = await client.query(
        "UPDATE absolute_webhook_inbox SET completed_at_ms = $3, fencing_token = NULL, lease_until_ms = NULL WHERE event_id = $1 AND fencing_token = $2 AND completed_at_ms IS NULL RETURNING event_id",
        [eventId, token, now],
      );
      if (result.rows.length !== 1) throw new Error("invalid webhook inbox fencing token");
    }),
  release: (eventId, token) =>
    runner.transaction(async (client) => {
      const result = await client.query(
        "UPDATE absolute_webhook_inbox SET fencing_token = NULL, lease_until_ms = NULL WHERE event_id = $1 AND fencing_token = $2 AND completed_at_ms IS NULL RETURNING event_id",
        [eventId, token],
      );
      if (result.rows.length !== 1) throw new Error("invalid webhook inbox fencing token");
    }),
  purgeCompleted: (before) =>
    runner.transaction(async (client) => {
      const result = await client.query(
        "DELETE FROM absolute_webhook_inbox WHERE completed_at_ms < $1 RETURNING event_id",
        [before],
      );
      return result.rows.length;
    }),
});

export const drainWebhookInbox = async <Payload>(input: {
  handler: (event: WebhookInboxEvent<Payload>) => Promise<void> | void;
  limit?: number;
  store: WebhookInboxStore<Payload>;
}) => {
  const claims = await input.store.claimPending({ limit: input.limit });
  await Promise.all(
    claims.map(async ({ event, token }) => {
      try {
        await input.handler(event);
        await input.store.complete(event.eventId, token);
      } catch (error) {
        await input.store.release(event.eventId, token).catch(() => undefined);
        throw error;
      }
    }),
  );
  return claims.length;
};
