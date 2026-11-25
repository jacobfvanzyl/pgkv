import { findDatabaseError, ReplyError } from "./errors.js";
import type { Run } from "./executor.js";
import { shapeReply } from "./reply.js";

/**
 * The one statement the client sends, for a single command, a pipeline and a multi alike.
 * Only text crosses the driver, in both directions, so nothing depends on how a driver
 * encodes a jsonb or boolean parameter or decodes a jsonb column: the commands go in as
 * JSON text, the flag as "true" or "false", and the replies come back as JSON text.
 * (Given `$1::jsonb`, postgres.js would JSON-encode the string a second time, and given
 * `$2::boolean` it would send the string "false" as true.)
 */
export const PIPELINE = "SELECT pgkv.pipeline($1::text::jsonb, $2::text::boolean)::text AS reply";

/**
 * Run the commands in one statement and resolve to their raw replies, one per command.
 * A command that failed has `{"error": message}` as its reply; see `errorOf`.
 *
 * Rejects with the EXECABORT ReplyError if the server discarded an atomic pipeline: it
 * then answers with one error object in place of the replies, and has applied nothing.
 *
 * Rejects also when the statement itself fails, in which case the database has undone
 * all of it, with the driver's error as it is.
 */
export async function runPipeline(
  run: Run,
  commands: readonly (readonly string[])[],
  atomic: boolean,
): Promise<unknown[]> {
  let rows: ArrayLike<unknown>;
  try {
    rows = await run(PIPELINE, [JSON.stringify(commands), atomic ? "true" : "false"]);
  } catch (error) {
    throw mapError(error);
  }
  const row = rows[0];
  const text: unknown = Array.isArray(row) ? row[0] : (row as { reply?: unknown } | undefined)?.reply;
  if (typeof text !== "string") {
    throw new TypeError("pgkv: the executor did not return the reply column of pgkv.pipeline as text");
  }
  const replies: unknown = JSON.parse(text);
  const discarded = errorOf(replies);
  if (discarded !== undefined) throw execAbort(discarded, (replies as { previous?: unknown }).previous, commands);
  if (!Array.isArray(replies) || replies.length !== commands.length) {
    throw new TypeError("pgkv: pgkv.pipeline did not return one reply per command");
  }
  return replies;
}

/**
 * The error ioredis rejects `multi().exec()` with when Redis discarded the transaction:
 * it names EXEC as its command and lists, in `previousErrors`, the error of each command
 * Redis refused to queue, which names that command. The server gives those as
 * `[{"index": position of the command, "error": message}, ...]`.
 */
function execAbort(message: string, previous: unknown, commands: readonly (readonly string[])[]): ReplyError {
  const abort = new ReplyError(message, { command: { name: "exec", args: [] } });
  abort.previousErrors = [];
  for (const entry of Array.isArray(previous) ? previous : []) {
    const { index, error } = entry as { index: number; error: string };
    const [name, ...args] = commands[index] as readonly string[];
    abort.previousErrors.push(new ReplyError(error, { command: { name: name as string, args } }));
  }
  return abort;
}

/**
 * A SQLSTATE of class KV is a Redis error reply and becomes a ReplyError. Nothing the
 * client sends makes the statement raise one (the errors of commands are returned, not
 * raised), so this is for completeness. Anything else is a database error, not a reply,
 * and is returned as it is.
 */
export function mapError(error: unknown): unknown {
  const found = findDatabaseError(error);
  if (!found?.state.startsWith("KV")) return error;
  return new ReplyError(String((found.error as { message?: unknown }).message), { cause: error });
}

/**
 * The message of an error reply, or undefined if the reply is not an error. No command
 * replies with a JSON object, so an object with an error message is always an error.
 */
export function errorOf(reply: unknown): string | undefined {
  if (reply === null || typeof reply !== "object" || Array.isArray(reply)) return undefined;
  const { error } = reply as { error?: unknown };
  return typeof error === "string" ? error : undefined;
}

/** Run one command, as a pipeline of one, and resolve to its reply shaped as ioredis shapes it. */
export async function runCommand(run: Run, name: string, argv: readonly string[]): Promise<unknown> {
  const [reply] = await runPipeline(run, [argv], false);
  const message = errorOf(reply);
  if (message !== undefined) throw new ReplyError(message, { command: { name, args: argv.slice(1) } });
  return shapeReply(name, reply);
}
