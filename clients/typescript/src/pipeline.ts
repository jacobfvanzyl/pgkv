import { toArgv } from "./args.js";
import type { Commands } from "./client.js";
import { COMMANDS } from "./commands.js";
import { ReplyError } from "./errors.js";
import type { Run } from "./executor.js";
import { shapeReply } from "./reply.js";
import { errorOf, runPipeline } from "./run.js";
import type { ExecResult } from "./types.js";

/**
 * Commands queued by `pipeline()` or `multi()` and sent by `exec()`. Every command method
 * returns the pipeline, so calls chain.
 */
// biome-ignore lint/suspicious/noUnsafeDeclarationMerging: the command methods are installed on the prototype below
export class Pipeline {
  readonly #run: Run;
  readonly #atomic: boolean;
  readonly #queue: { name: string; argv: string[] }[] = [];
  #result: Promise<ExecResult> | undefined;

  /** @internal Use `pipeline()` or `multi()` on the client. */
  constructor(run: Run, atomic: boolean) {
    this.#run = run;
    this.#atomic = atomic;
  }

  /** How many commands are queued. */
  get length(): number {
    return this.#queue.length;
  }

  static {
    // One method per command, and `call` for any command by name.
    const define = (name: string, value: (this: Pipeline, ...args: unknown[]) => Pipeline) =>
      Object.defineProperty(Pipeline.prototype, name, { value, writable: true, configurable: true });
    for (const name of COMMANDS) {
      define(name, function (...args) {
        return this.#push(name, args);
      });
    }
    define("call", function (command, ...args) {
      return this.#push(command as string, args);
    });
  }

  /** Queue commands given as arrays: `[["set", "k", "v"], ["get", "k"]]`. */
  addBatch(commands: readonly (readonly unknown[])[]): this {
    for (const [name, ...args] of commands) this.#push(name as string, args);
    return this;
  }

  /**
   * Send the queued commands as one statement and resolve to one `[error, result]` pair
   * per command, in order. Other sessions see all of the commands' effects or none.
   *
   * A command that fails has a ReplyError as the only element of its pair, and the
   * others still apply: from `pipeline()` and from `multi()` alike, as in Redis.
   *
   * From `multi()`, a command Redis refuses to queue (an unknown one, or one with a number
   * of arguments its command table rules out) discards them all: nothing is applied and
   * the promise rejects with an EXECABORT ReplyError, whose `previousErrors` are the
   * errors of the refused commands. Nothing is raised in the database for it, so a
   * transaction it runs in carries on.
   *
   * A database error that is not a Redis reply (lost connection, deadlock, serialization
   * failure) rejects the promise as it is. The database has then undone the whole
   * statement: no command was applied.
   */
  exec(): Promise<ExecResult> {
    this.#result ??= this.#exec();
    return this.#result;
  }

  #push(name: string, args: readonly unknown[]): this {
    if (this.#result) throw new Error("pgkv: exec() has already been called on this pipeline");
    this.#queue.push({ name, argv: toArgv(name, args) });
    return this;
  }

  async #exec(): Promise<ExecResult> {
    const queue = this.#queue;
    if (queue.length === 0) return [];
    const replies = await runPipeline(
      this.#run,
      queue.map((command) => command.argv),
      this.#atomic,
    );
    return replies.map((reply, i) => {
      const { name, argv } = queue[i] as (typeof queue)[number];
      const message = errorOf(reply);
      if (message === undefined) return [null, shapeReply(name, reply)];
      // As ioredis: the pair has one element, and the error names its command in a
      // pipeline but not in a transaction, where the errors arrive together in EXEC's reply.
      const error = new ReplyError(message, this.#atomic ? undefined : { command: { name, args: argv.slice(1) } });
      return [error] as unknown as ExecResult[number];
    });
  }
}

export interface Pipeline extends Commands<"pipeline"> {}
