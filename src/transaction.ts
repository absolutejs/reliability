export type SqlQueryResult = {
  rowCount?: number | null;
  rows: Array<Record<string, unknown>>;
};

export type SqlClient = {
  query: (
    text: string,
    values?: ReadonlyArray<unknown>,
  ) => Promise<SqlQueryResult>;
};

export type SqlPool = {
  connect: () => Promise<SqlClient & { release: () => void }>;
};

export type TransactionRunner = {
  transaction: <T>(run: (client: SqlClient) => Promise<T>) => Promise<T>;
};

/** Keeps BEGIN, every statement, and COMMIT on one checked-out connection. */
export const createPostgresTransactionRunner = (
  pool: SqlPool,
): TransactionRunner => ({
  transaction: async (run) => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await run(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  },
});
