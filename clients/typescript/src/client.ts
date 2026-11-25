import { toArgv } from "./args.js";
import type { GeneratedCommands } from "./commands.generated.js";
import { COMMANDS } from "./commands.js";
import { type Executor, type Run, toRun } from "./executor.js";
import { Pipeline } from "./pipeline.js";
import { runCommand } from "./run.js";
import type { ClientContext, RedisKey, RedisValue, Result } from "./types.js";

/**
 * Every command, with ioredis's signatures. The two commands ioredis does not declare are
 * written out here.
 */
export interface Commands<Context extends ClientContext = "default"> extends GeneratedCommands<Context> {
  /** Delete the key; with IFEQ only if it holds the given value. Replies 1 if it was deleted. */
  delex(key: RedisKey): Result<number, Context>;
  delex(key: RedisKey, ifeq: "IFEQ", value: RedisValue): Result<number, Context>;
  /** Delete the key if it holds the given value. Replies 1 if it was deleted. */
  delifeq(key: RedisKey, value: RedisValue): Result<number, Context>;
}

/**
 * Redis commands on PostgreSQL, with the API of ioredis.
 *
 * ```ts
 * const kv = new Pgkv(pool);              // node-postgres Pool, Client or PoolClient
 * const kv = new Pgkv(sql);               // postgres.js
 * await kv.set("greeting", "hello", "EX", 60);
 * ```
 *
 * The client holds no connection and no state, and everything it does is one statement:
 * constructing one is free, so make one per transaction to have its commands commit or
 * roll back with the rest of that transaction.
 */
// biome-ignore lint/suspicious/noUnsafeDeclarationMerging: the command methods are installed on the prototype below
export class Pgkv {
  readonly #run: Run;

  constructor(executor: Executor) {
    this.#run = toRun(executor);
  }

  static {
    // One method per command, and `call` for any command by name.
    const define = (name: string, value: (this: Pgkv, ...args: unknown[]) => Promise<unknown>) =>
      Object.defineProperty(Pgkv.prototype, name, { value, writable: true, configurable: true });
    for (const name of COMMANDS) {
      define(name, function (...args) {
        return this.#send(name, args);
      });
    }
    define("call", function (command, ...args) {
      return this.#send(command as string, args);
    });
  }

  async #send(command: string, args: readonly unknown[]): Promise<unknown> {
    return runCommand(this.#run, command, toArgv(command, args));
  }

  /**
   * Queue commands and send them with `exec()` as one statement, in one round trip. Each
   * command succeeds or fails on its own.
   */
  pipeline(commands?: readonly (readonly unknown[])[]): Pipeline {
    const pipeline = new Pipeline(this.#run, false);
    return commands ? pipeline.addBatch(commands) : pipeline;
  }

  /**
   * Queue commands and run them with `exec()` as Redis MULTI/EXEC does, in one statement.
   * A command that fails while running has its error in its pair and the others still
   * apply; a command Redis would refuse to queue (unknown, or too few arguments) discards them all.
   */
  multi(commands?: readonly (readonly unknown[])[]): Pipeline {
    const pipeline = new Pipeline(this.#run, true);
    return commands ? pipeline.addBatch(commands) : pipeline;
  }

  /** The names of the commands that are methods on this client. */
  getBuiltinCommands(): string[] {
    return [...COMMANDS];
  }
}

export interface Pgkv extends Commands<"default"> {}
