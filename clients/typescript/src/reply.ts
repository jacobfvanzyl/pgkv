/**
 * A JSON reply from pgkv to what ioredis resolves with.
 *
 * pgkv answers a status reply (+OK) as {"status": "OK"} so that it can be told from the
 * bulk string "OK"; ioredis gives both as a plain string. An error reply, {"error": ...},
 * only stands in place of a whole reply and is taken out before this (see run.ts), so
 * every object met here is a status.
 */

type ReplyTransformer = (reply: unknown) => unknown;

function unwrap(reply: unknown): unknown {
  if (reply === null || typeof reply !== "object") return reply;
  if (Array.isArray(reply)) {
    for (let i = 0; i < reply.length; i++) {
      const item: unknown = reply[i];
      if (item !== null && typeof item === "object") reply[i] = unwrap(item);
    }
    return reply;
  }
  return (reply as { status?: unknown }).status;
}

/** Flat [field, value, ...] to an object, exactly as ioredis's hgetall transformer. */
function pairsToObject(reply: unknown): unknown {
  if (!Array.isArray(reply)) return reply;
  const out: Record<string, unknown> = {};
  for (let i = 0; i < reply.length; i += 2) {
    const key = reply[i] as string;
    const value: unknown = reply[i + 1];
    if (key in out) {
      // "__proto__", "constructor", ...: define it as an own property instead of assigning.
      Object.defineProperty(out, key, { value, configurable: true, enumerable: true, writable: true });
    } else {
      out[key] = value;
    }
  }
  return out;
}

/**
 * ioredis's built-in reply transformers that change a RESP2 reply. (Its others, for
 * hrandfield and the sorted-set WITHSCORES commands, only act on RESP3.) Like ioredis,
 * they apply to the exact lowercase name: `call("hgetall", k)` returns an object and
 * `call("HGETALL", k)` the flat array.
 */
const REPLY_TRANSFORMERS: Readonly<Record<string, ReplyTransformer>> = Object.assign(Object.create(null), {
  hgetall: pairsToObject,
});

export function shapeReply(command: string, reply: unknown): unknown {
  const shaped = unwrap(reply);
  const transform = REPLY_TRANSFORMERS[command];
  return transform ? transform(shaped) : shaped;
}
