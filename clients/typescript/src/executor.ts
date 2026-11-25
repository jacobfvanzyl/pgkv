/**
 * The client does not open connections. It runs its one statement through whatever the
 * application already has: a node-postgres Pool, Client or PoolClient, a postgres.js
 * `sql` (also its transactions and reserved connections), or any object with a
 * node-postgres-shaped `query`. It never needs two statements to reach the same
 * connection, so a pool, a single connection and a transaction are all the same to it.
 */

/** node-postgres (`Pool`, `Client`, `PoolClient`) or anything shaped like it. */
export interface Queryable {
  query(text: string, values: string[]): Promise<{ rows: ArrayLike<unknown> }>;
}

/** postgres.js's `sql`, a transaction from `sql.begin`, or a reserved connection. Bun's `SQL` has the same shape. */
export interface PostgresJsLike {
  unsafe(text: string, values: string[]): PromiseLike<ArrayLike<unknown>>;
}

export type Executor = Queryable | PostgresJsLike;

/** Runs one statement, with text parameters for $1, $2, ..., and resolves to its rows. */
export type Run = (text: string, values: string[]) => Promise<ArrayLike<unknown>>;

const has = (object: unknown, method: string): boolean =>
  typeof (object as Record<string, unknown> | null | undefined)?.[method] === "function";

export function toRun(executor: Executor): Run {
  if (has(executor, "unsafe")) {
    const sql = executor as PostgresJsLike;
    return async (text, values) => await sql.unsafe(text, values);
  }
  if (has(executor, "query")) {
    const target = executor as Queryable;
    return async (text, values) => (await target.query(text, values)).rows;
  }
  throw new TypeError(
    "pgkv: expected a node-postgres Pool, Client or PoolClient, a postgres.js sql, " +
      "or an object with query(text, values) resolving to { rows }",
  );
}
