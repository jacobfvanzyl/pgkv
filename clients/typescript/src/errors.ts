/**
 * A Redis error reply, e.g. "WRONGTYPE Operation against a key holding the wrong kind of
 * value" or "ERR value is not an integer or out of range". Same `name` and `message` as
 * the ReplyError ioredis rejects with.
 */
export class ReplyError extends Error {
  /** The command that failed, as ioredis attaches it (not inside `multi()`, as in ioredis). */
  declare command?: { name: string; args: string[] };
  /**
   * On the EXECABORT error `multi().exec()` rejects with: the errors of the commands that
   * were refused, in the order they were queued, each naming its command. As in ioredis.
   */
  declare previousErrors?: ReplyError[];

  constructor(message: string, options?: { cause?: unknown; command?: { name: string; args: string[] } }) {
    super(message, options?.cause === undefined ? undefined : { cause: options.cause });
    if (options?.command) this.command = options.command;
  }
}
// On the prototype rather than an instance field, so it does not show up as an own property.
Object.defineProperty(ReplyError.prototype, "name", { value: "ReplyError", writable: true, configurable: true });

const SQLSTATE = /^[0-9A-Z]{5}$/;

function stateOf(error: object): string | undefined {
  const { code, errno } = error as { code?: unknown; errno?: unknown };
  if (typeof code === "string" && SQLSTATE.test(code)) return code; // node-postgres, postgres.js
  if (typeof errno === "string" && SQLSTATE.test(errno)) return errno; // Bun's SQL
  return undefined;
}

/** The first error in the `cause` chain that carries a SQLSTATE, with that SQLSTATE. */
export function findDatabaseError(error: unknown): { state: string; error: object } | undefined {
  for (let depth = 0, e = error; depth < 5 && typeof e === "object" && e !== null; depth++) {
    const state = stateOf(e);
    if (state !== undefined) return { state, error: e };
    e = (e as { cause?: unknown }).cause;
  }
  return undefined;
}

/**
 * The five-character SQLSTATE of a database error, or undefined if it has none. Looks
 * through `cause`, so it also works on errors a query builder wrapped.
 */
export function sqlState(error: unknown): string | undefined {
  return findDatabaseError(error)?.state;
}

/**
 * Whether the error is a serialization failure (40001) or a deadlock (40P01): the
 * transaction did nothing wrong and can be run again from the start.
 */
export function isRetryable(error: unknown): boolean {
  const state = sqlState(error);
  return state === "40001" || state === "40P01";
}
