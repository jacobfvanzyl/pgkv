/**
 * Method arguments to argv, the way ioredis does it: nested arrays are flattened one level,
 * `mset`/`msetnx`/`hset`/`hmset` take an object or a Map, and everything else is turned
 * into a string.
 */

type ArgumentTransformer = (args: unknown[]) => unknown[];

function pairs(source: unknown): unknown[] | undefined {
  if (source instanceof Map) {
    const out: unknown[] = [];
    for (const [key, value] of source) out.push(key, value);
    return out;
  }
  if (typeof source === "object" && source !== null && !(source instanceof Uint8Array)) {
    const out: unknown[] = [];
    for (const key of Object.keys(source)) out.push(key, (source as Record<string, unknown>)[key]);
    return out;
  }
  return undefined;
}

const msetArguments: ArgumentTransformer = (args) => (args.length === 1 ? (pairs(args[0]) ?? args) : args);

const hsetArguments: ArgumentTransformer = (args) => {
  const fields = args.length === 2 ? pairs(args[1]) : undefined;
  return fields ? [args[0], ...fields] : args;
};

/** ioredis's built-in argument transformers. Like ioredis, they apply to the exact lowercase name. */
const ARGUMENT_TRANSFORMERS: Readonly<Record<string, ArgumentTransformer>> = Object.assign(Object.create(null), {
  mset: msetArguments,
  msetnx: msetArguments,
  hset: hsetArguments,
  hmset: hsetArguments,
});

// A web standard global in every runtime this runs in; declared here so that the build
// needs neither the DOM nor the Node.js type libraries.
declare const TextDecoder: new (
  label: string,
  options: { fatal: boolean; ignoreBOM: boolean },
) => { decode(input: Uint8Array): string };

let decoder: InstanceType<typeof TextDecoder> | undefined;

function fail(command: string, index: number, why: string): never {
  throw new TypeError(`pgkv: argument ${index + 1} of '${command}' ${why}`);
}

const LONE_SURROGATE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;

function toText(command: string, index: number, arg: unknown): string {
  let text: string;
  if (typeof arg === "string") {
    text = arg;
  } else if (arg instanceof Uint8Array) {
    decoder ??= new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
    try {
      text = decoder.decode(arg);
    } catch {
      fail(command, index, "is not valid UTF-8: pgkv stores text, not arbitrary bytes");
    }
  } else if (arg === null || arg === undefined) {
    text = ""; // as ioredis
  } else if (typeof arg === "number" && Number.isInteger(arg) && !Number.isSafeInteger(arg)) {
    text = BigInt(arg).toString(); // as ioredis: 1e21 is sent as digits
  } else if (typeof arg === "function") {
    fail(command, index, "is a function: callbacks are not supported, await the returned promise");
  } else {
    text = String(arg);
  }
  if (text.includes("\u0000")) fail(command, index, "contains a NUL character, which PostgreSQL text cannot hold");
  if (LONE_SURROGATE.test(text)) fail(command, index, "is not well-formed Unicode (lone surrogate)");
  return text;
}

/** The words of the Redis command: its name followed by its arguments as text. */
export function toArgv(command: string, args: readonly unknown[]): string[] {
  if (typeof command !== "string" || command === "")
    throw new TypeError("pgkv: the command name must be a non-empty string");
  let flat: unknown[] = args.flat();
  const transform = ARGUMENT_TRANSFORMERS[command];
  if (transform) flat = transform(flat);
  const argv = [toText(command, -1, command)];
  for (let i = 0; i < flat.length; i++) argv.push(toText(command, i, flat[i]));
  return argv;
}
