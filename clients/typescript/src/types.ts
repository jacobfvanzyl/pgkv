import type { Pipeline } from "./pipeline.js";

/** A key. Bytes are accepted when they are valid UTF-8: pgkv keys and values are text. */
export type RedisKey = string | Uint8Array;
/** A value. Bytes are accepted when they are valid UTF-8: pgkv keys and values are text. */
export type RedisValue = string | Uint8Array | number;

/** Where a command method is called: on the client it returns a promise, on a pipeline the pipeline. */
export type ClientContext = "default" | "pipeline";

export interface ResultTypes<T> {
  default: Promise<T>;
  pipeline: Pipeline;
}

export type Result<T, Context extends ClientContext> = ResultTypes<T>[Context];

/** What `pipeline().exec()` and `multi().exec()` resolve to, as in ioredis. */
export type ExecResult = [error: Error | null, result: unknown][];
