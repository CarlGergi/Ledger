// The VS Code side: a tree of sessions and their calls, a read-only document to inspect a
// call, and a document that steps through a session one event at a time.

import * as vscode from "vscode";

import * as cli from "./cli";
import { Call, LedgerEvent, SessionSummary, calls, description, inspectText, label, replayAt, status } from "./model";

const INSPECT = "ledger-inspect";
const REPLAY = "ledger-replay";

type Node = { kind: "session"; summary: SessionSummary } | { kind: "call"; session: string; call: Call };

function ledger(): cli.Ledger {
  const config = vscode.workspace.getConfiguration("ledger");
  return {
    command: config.get<string>("command") || "ledger",
    databaseUrl: config.get<string>("databaseUrl") || undefined,
  };
}

/** Each session's events, read once and kept until the next refresh. */
class Log {
  private cache = new Map<string, Promise<LedgerEvent[]>>();

  events(session: string): Promise<LedgerEvent[]> {
    let events = this.cache.get(session);
    if (!events) {
      events = cli.events(ledger(), session);
      this.cache.set(session, events);
    }
    return events;
  }

  async call(session: string, seq: number): Promise<Call | undefined> {
    return calls(await this.events(session)).find((c) => c.seq === seq);
  }

  clear(): void {
    this.cache.clear();
  }
}

function icon(call: Call): vscode.ThemeIcon {
  const current = status(call);
  if (current === "ok" && call.kind === "model") return new vscode.ThemeIcon("sparkle");
  if (current === "ok") return new vscode.ThemeIcon("pass", new vscode.ThemeColor("testing.iconPassed"));
  if (current === "in_doubt") return new vscode.ThemeIcon("warning", new vscode.ThemeColor("list.warningForeground"));
  if (current.startsWith("open")) return new vscode.ThemeIcon("circle-large-outline");
  return new vscode.ThemeIcon("error", new vscode.ThemeColor("testing.iconFailed"));
}

class Sessions implements vscode.TreeDataProvider<Node> {
  private changed = new vscode.EventEmitter<Node | undefined>();
  readonly onDidChangeTreeData = this.changed.event;

  constructor(private log: Log) {}

  refresh(): void {
    this.log.clear();
    this.changed.fire(undefined);
  }

  getTreeItem(node: Node): vscode.TreeItem {
    if (node.kind === "session") {
      const item = new vscode.TreeItem(node.summary.session_id, vscode.TreeItemCollapsibleState.Collapsed);
      item.description = `${node.summary.event_count} events`;
      item.tooltip = `Started ${node.summary.created_at}`;
      item.contextValue = "session";
      item.iconPath = new vscode.ThemeIcon("history");
      return item;
    }
    const item = new vscode.TreeItem(label(node.call), vscode.TreeItemCollapsibleState.None);
    item.description = description(node.call);
    item.iconPath = icon(node.call);
    item.contextValue = "call";
    item.command = { command: "ledger.inspect", title: "Inspect", arguments: [node] };
    return item;
  }

  async getChildren(node?: Node): Promise<Node[]> {
    try {
      if (!node) {
        return (await cli.sessions(ledger())).map((summary) => ({ kind: "session", summary }));
      }
      if (node.kind === "session") {
        const session = node.summary.session_id;
        return calls(await this.log.events(session)).map((call) => ({ kind: "call", session, call }));
      }
    } catch (error) {
      vscode.window.showErrorMessage(`Ledger: ${(error as Error).message}`);
    }
    return [];
  }
}

/** Read-only documents for inspecting a call: ledger-inspect:/<session>/<seq>.json */
class Inspector implements vscode.TextDocumentContentProvider {
  constructor(private log: Log) {}

  async provideTextDocumentContent(uri: vscode.Uri): Promise<string> {
    const [, session, file] = uri.path.split("/");
    const call = await this.log.call(decodeURIComponent(session), Number.parseInt(file, 10));
    return call ? inspectText(call) : "No such call.\n";
  }
}

/** One document per session being replayed: ledger-replay:/<session>.txt */
class Replayer implements vscode.TextDocumentContentProvider {
  private changed = new vscode.EventEmitter<vscode.Uri>();
  readonly onDidChange = this.changed.event;
  private steps = new Map<string, number>();

  constructor(private log: Log) {}

  static uri(session: string): vscode.Uri {
    return vscode.Uri.from({ scheme: REPLAY, path: `/${encodeURIComponent(session)}.txt` });
  }

  private session(uri: vscode.Uri): string {
    return decodeURIComponent(uri.path.slice(1).replace(/\.txt$/, ""));
  }

  async provideTextDocumentContent(uri: vscode.Uri): Promise<string> {
    const session = this.session(uri);
    return replayAt(await this.log.events(session), this.steps.get(session) ?? 1);
  }

  async start(session: string): Promise<void> {
    this.steps.set(session, 1);
    const document = await vscode.workspace.openTextDocument(Replayer.uri(session));
    await vscode.window.showTextDocument(document, { preview: false });
    await vscode.commands.executeCommand("setContext", "ledger.replaying", true);
  }

  async move(by: number): Promise<void> {
    const uri = vscode.window.activeTextEditor?.document.uri;
    if (!uri || uri.scheme !== REPLAY) {
      return;
    }
    const session = this.session(uri);
    const total = (await this.log.events(session)).length;
    const next = Math.min(Math.max((this.steps.get(session) ?? 1) + by, 1), Math.max(total, 1));
    this.steps.set(session, next);
    this.changed.fire(uri);
  }
}

export function activate(context: vscode.ExtensionContext): void {
  const log = new Log();
  const sessions = new Sessions(log);
  const replayer = new Replayer(log);

  context.subscriptions.push(
    vscode.window.registerTreeDataProvider("ledger.sessions", sessions),
    vscode.workspace.registerTextDocumentContentProvider(INSPECT, new Inspector(log)),
    vscode.workspace.registerTextDocumentContentProvider(REPLAY, replayer),
    vscode.commands.registerCommand("ledger.refresh", () => sessions.refresh()),
    vscode.commands.registerCommand("ledger.inspect", async (node?: Node) => {
      if (node?.kind !== "call") {
        return;
      }
      const uri = vscode.Uri.from({
        scheme: INSPECT,
        path: `/${encodeURIComponent(node.session)}/${node.call.seq}.json`,
      });
      const document = await vscode.workspace.openTextDocument(uri);
      await vscode.languages.setTextDocumentLanguage(document, "json");
      await vscode.window.showTextDocument(document, { preview: true });
    }),
    vscode.commands.registerCommand("ledger.replay", async (node?: Node) => {
      const session =
        node?.kind === "session"
          ? node.summary.session_id
          : await vscode.window.showInputBox({ prompt: "Session to replay" });
      if (session) {
        await replayer.start(session);
      }
    }),
    vscode.commands.registerCommand("ledger.replay.next", () => replayer.move(1)),
    vscode.commands.registerCommand("ledger.replay.previous", () => replayer.move(-1)),
  );
}

export function deactivate(): void {}
