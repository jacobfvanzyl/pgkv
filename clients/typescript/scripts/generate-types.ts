/**
 * Writes src/commands.generated.ts: the method signatures of every command in
 * src/commands.ts, taken from the ioredis declarations installed as a dev dependency.
 *
 *   node scripts/generate-types.ts           rewrite the file
 *   node scripts/generate-types.ts --check   fail if the file is out of date
 *
 * What is kept from ioredis: the promise-returning overloads of the string-reply methods.
 * What changes: callback overloads are dropped (pgkv has no callback API), `Buffer`
 * arguments become `Uint8Array` (accepted when valid UTF-8), and replies typed for both
 * protocols (`RespShape<Resp2<X>, Resp3<Y>, Context>`) keep their RESP2 type, which is
 * what pgkv.call answers.
 */
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { COMMANDS, NOT_IN_IOREDIS } from "../src/commands.ts";

const source = fileURLToPath(new URL("../node_modules/ioredis/built/utils/RedisCommander.d.ts", import.meta.url));
const target = fileURLToPath(new URL("../src/commands.generated.ts", import.meta.url));
const version = JSON.parse(
  readFileSync(fileURLToPath(new URL("../node_modules/ioredis/package.json", import.meta.url)), "utf8"),
).version as string;

const OPEN = "([{<";
const CLOSE = ")]}>";

/** Split text at a separator that is not nested in brackets or inside a comment. */
function splitTop(text: string, separator: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let start = 0;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i] as string;
    if (text.startsWith("/*", i)) {
      i = text.indexOf("*/", i) + 1;
    } else if (text.startsWith("=>", i)) {
      i++;
    } else if (OPEN.includes(ch)) {
      depth++;
    } else if (CLOSE.includes(ch)) {
      depth--;
    } else if (ch === separator && depth === 0) {
      parts.push(text.slice(start, i));
      start = i + 1;
    }
  }
  parts.push(text.slice(start));
  return parts;
}

/** Index of the bracket closing the one at `open`. */
function closing(text: string, open: number): number {
  let depth = 0;
  for (let i = open; i < text.length; i++) {
    const ch = text[i] as string;
    if (text.startsWith("=>", i)) i++;
    else if (OPEN.includes(ch)) depth++;
    else if (CLOSE.includes(ch) && --depth === 0) return i;
  }
  throw new Error(`unbalanced: ${text.slice(open, open + 80)}`);
}

/** RespShape<Resp2<X>, Resp3<Y>, Context> -> X */
function resp2(text: string): string {
  for (;;) {
    const at = text.indexOf("RespShape<");
    if (at < 0) return text;
    const open = at + "RespShape".length;
    const end = closing(text, open);
    const first = (splitTop(text.slice(open + 1, end), ",")[0] as string).trim();
    if (!first.startsWith("Resp2<") || !first.endsWith(">")) throw new Error(`unexpected RespShape: ${first}`);
    text = text.slice(0, at) + first.slice("Resp2<".length, -1) + text.slice(end + 1);
  }
}

const isCallback = (param: string) => /^\s*callback\??:/.test(param);

/** One overload without its callback, or null if it only exists to take a callback. */
function convert(signature: string): string | null {
  const open = signature.indexOf("(");
  const close = closing(signature, open);
  const name = signature.slice(0, open);
  let params = splitTop(signature.slice(open + 1, close), ",").map((p) => p.trim());
  const returns = signature.slice(close + 1);
  if (params.length === 1 && (params[0] as string).startsWith("...args: [")) {
    // (...args: [a: A, ...rest: B[], callback: Callback<T>]): a required callback has a twin
    // overload without it; an optional one is simply removed.
    const tuple = params[0] as string;
    const elements = splitTop(tuple.slice(tuple.indexOf("[") + 1, tuple.lastIndexOf("]")), ",").map((p) => p.trim());
    const last = elements[elements.length - 1] as string;
    if (isCallback(last)) {
      if (!last.startsWith("callback?:")) return null;
      elements.pop();
    }
    params = [`...args: [${elements.join(", ")}]`];
  } else if (params.length > 0 && isCallback(params[params.length - 1] as string)) {
    if (!(params[params.length - 1] as string).startsWith("callback?:")) return null;
    params.pop();
  }
  if (params.some((p) => p.includes("Callback<"))) throw new Error(`callback left in: ${signature}`);
  return `${name}(${params.join(", ").replace(/\bBuffer\b/g, "Uint8Array")})${resp2(returns)}`;
}

const CALL_DOC = `/**
   * Run any command by name: \`call("set", "k", "v")\` is \`set("k", "v")\`. A command pgkv
   * does not have rejects with a ReplyError "ERR unknown command ...".
   */`;

const declarations = readFileSync(source, "utf8");
const bodyStart = declarations.indexOf("interface RedisCommander<");
const bodyOpen = declarations.indexOf("{\n", declarations.indexOf("}>", bodyStart));
const body = declarations.slice(bodyOpen + 1, declarations.lastIndexOf("}\nexport default"));

const wanted = new Set<string>([...COMMANDS, "call"]);
const found = new Set<string>();
const seen = new Set<string>();
const lines: string[] = [];
for (const member of splitTop(body, ";")) {
  const match = /^((?:\s*\/\*\*[\s\S]*?\*\/)*)\s*((\w+)\([\s\S]*)$/.exec(member);
  if (!match) continue;
  const [, comment, signature, name] = match as unknown as [string, string, string, string];
  if (!wanted.has(name)) continue;
  const converted = convert(
    signature
      .replace(/\s*\n\s*/g, " ")
      .replace(/\[ /g, "[")
      .replace(/,? \]/g, "]"),
  );
  if (converted === null || seen.has(converted)) continue;
  seen.add(converted);
  if (!found.has(name) && comment.trim()) {
    // ioredis documents `call` with a callback example, which does not apply here.
    const doc = name === "call" ? CALL_DOC : comment.trim().replace(/\n\s*/g, "\n   ");
    lines.push(`  ${doc}`);
  }
  found.add(name);
  lines.push(`  ${converted};`);
}

const missing = [...wanted].filter((name) => !found.has(name)).sort();
const expected = [...NOT_IN_IOREDIS].sort();
if (missing.join() !== expected.join()) {
  throw new Error(
    `commands ioredis ${version} does not declare: ${missing.join(", ")} (expected: ${expected.join(", ")})`,
  );
}

const output = `/**
 * Generated by scripts/generate-types.ts from the type declarations of ioredis ${version}
 * (MIT License, Copyright (c) 2015-2022 Zihua Li). Do not edit; run \`pnpm generate\`.
 *
 * The promise-returning string overloads of every command in commands.ts, as ioredis
 * declares them, without callbacks and with Uint8Array where ioredis says Buffer.
 */
import type { ClientContext, RedisKey, RedisValue, Result } from "./types.js";

export interface GeneratedCommands<Context extends ClientContext = "default"> {
${lines.join("\n")}
}
`;

if (process.argv.includes("--check")) {
  if (readFileSync(target, "utf8") !== output) {
    console.error(`src/commands.generated.ts does not match ioredis ${version}: run pnpm generate`);
    process.exit(1);
  }
  console.log(`src/commands.generated.ts matches ioredis ${version} (${found.size} methods, ${seen.size} signatures)`);
} else {
  writeFileSync(target, output);
  console.log(`wrote ${seen.size} signatures of ${found.size} methods from ioredis ${version}`);
}
