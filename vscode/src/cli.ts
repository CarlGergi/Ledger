// Reads the log through the `ledger` command, so that the extension and the gateway share
// one reader of the database and its schema.

import { execFile } from "node:child_process";

import type { LedgerEvent, SessionSummary } from "./model";

export interface Ledger {
  /** The ledger command, or a path to it. */
  command: string;
  /** The log's database. Empty or missing leaves DATABASE_URL as it is. */
  databaseUrl?: string;
}

export function run(ledger: Ledger, args: string[]): Promise<string> {
  const env = { ...process.env };
  if (ledger.databaseUrl) {
    env.DATABASE_URL = ledger.databaseUrl;
  }
  return new Promise((resolve, reject) => {
    execFile(ledger.command, args, { env, maxBuffer: 256 * 1024 * 1024 }, (error, stdout, stderr) => {
      if (error) {
        const detail = stderr.trim() || error.message;
        reject(new Error(`${ledger.command} ${args.join(" ")} failed: ${detail}`));
      } else {
        resolve(stdout);
      }
    });
  });
}

function jsonLines<T>(text: string): T[] {
  return text
    .split("\n")
    .filter((line) => line.trim() !== "")
    .map((line) => JSON.parse(line) as T);
}

export async function sessions(ledger: Ledger): Promise<SessionSummary[]> {
  return jsonLines<SessionSummary>(await run(ledger, ["sessions", "--json"]));
}

export async function events(ledger: Ledger, session: string): Promise<LedgerEvent[]> {
  return jsonLines<LedgerEvent>(await run(ledger, ["log", session, "--json"]));
}
