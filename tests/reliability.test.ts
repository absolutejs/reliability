import { describe, expect, test } from "bun:test";
import {
  createMemoryIdempotentOperationStore,
  createMemoryWebhookInboxStore,
  createPostgresTransactionRunner,
  drainWebhookInbox,
  fingerprintPayload,
} from "../src";

describe("idempotent operations", () => {
  test("scopes keys and rejects payload reuse", async () => {
    const store = createMemoryIdempotentOperationStore<{ id: string }>();
    const scope = { key: "incident-42", namespace: "message", tenant: "a" };
    const first = await store.begin({
      fingerprint: await fingerprintPayload({ body: "one" }),
      leaseMs: 1_000,
      now: 0,
      scope,
    });
    expect(first.disposition).toBe("claimed");
    if (first.disposition !== "claimed") throw new Error("claim expected");
    await store.markExecuting(first.operationId, first.token, 1);
    await store.complete(first.operationId, first.token, { id: "provider-1" }, 2);
    expect(
      await store.begin({
        fingerprint: await fingerprintPayload({ body: "one" }),
        leaseMs: 1_000,
        now: 3,
        scope,
      }),
    ).toMatchObject({ disposition: "completed", result: { id: "provider-1" } });
    expect(
      await store.begin({
        fingerprint: await fingerprintPayload({ body: "different" }),
        leaseMs: 1_000,
        now: 3,
        scope,
      }),
    ).toMatchObject({ disposition: "conflict" });
  });

  test("never automatically retries an expired executing side effect", async () => {
    const store = createMemoryIdempotentOperationStore();
    const claim = await store.begin({
      fingerprint: "payload",
      leaseMs: 10,
      now: 0,
      scope: { key: "one", namespace: "test" },
    });
    if (claim.disposition !== "claimed") throw new Error("claim expected");
    await store.markExecuting(claim.operationId, claim.token, 1);
    expect(
      await store.begin({
        fingerprint: "payload",
        leaseMs: 10,
        now: 11,
        scope: { key: "one", namespace: "test" },
      }),
    ).toMatchObject({ disposition: "indeterminate" });
  });
});

describe("webhook inbox", () => {
  test("deduplicates, reclaims, drains, and purges", async () => {
    const store = createMemoryWebhookInboxStore<{ status: string }>();
    const event = {
      eventId: "provider:event-1",
      occurredAt: 1,
      payload: { status: "delivered" },
      provider: "provider",
      streamId: "message-1",
    };
    const first = await store.accept(event, { leaseMs: 10, now: 0 });
    expect(first.disposition).toBe("accepted");
    expect((await store.accept(event, { leaseMs: 10, now: 1 })).token).toBeUndefined();
    if (first.token === undefined) throw new Error("token expected");
    await store.release(event.eventId, first.token);
    const seen: string[] = [];
    expect(
      await drainWebhookInbox({
        handler: (claimed) => void seen.push(claimed.eventId),
        store,
      }),
    ).toBe(1);
    expect(seen).toEqual([event.eventId]);
    expect(await store.purgeCompleted(Date.now() + 1)).toBe(1);
  });
});

test("Postgres runner keeps the transaction on one checked-out client", async () => {
  const statements: string[] = [];
  let released = 0;
  const runner = createPostgresTransactionRunner({
    connect: async () => ({
      query: async (sql) => {
        statements.push(sql);
        return { rows: [] };
      },
      release: () => {
        released += 1;
      },
    }),
  });
  await runner.transaction(async (client) => {
    await client.query("SELECT 1");
    await client.query("SELECT 2");
  });
  expect(statements).toEqual(["BEGIN", "SELECT 1", "SELECT 2", "COMMIT"]);
  expect(released).toBe(1);
});
