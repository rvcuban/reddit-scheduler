import { spawn, type ChildProcess } from "node:child_process";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { existsSync, watch } from "node:fs";
import { appendFile, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { QueueStore } from "./queue-store.js";
import { scheduleQueue } from "./schedule.js";
import { POST_STATUS, type EngineEvent, type QueueItem, type SchedulerState, type Settings } from "./types.js";

const ROOT = process.cwd();
const queue = new QueueStore(join(ROOT, "queue.json"));
const settingsPath = join(ROOT, "settings.json");
const eventsPath = join(ROOT, "events.jsonl");
const clients = new Set<ServerResponse>();
let scheduler: ChildProcess | undefined;
const DEFAULT_SETTINGS: Settings = { interval_hours: 48, check_every_seconds: 3_600 };

function send(response: ServerResponse, status: number, payload: unknown): void { response.writeHead(status, { "content-type": "application/json" }); response.end(JSON.stringify(payload)); }
async function body(request: IncomingMessage): Promise<unknown> { let value = ""; for await (const chunk of request) value += String(chunk); return value ? JSON.parse(value) as unknown : {}; }
async function settings(): Promise<Settings> { try { return { ...DEFAULT_SETTINGS, ...JSON.parse(await readFile(settingsPath, "utf8")) as Partial<Settings> }; } catch { return DEFAULT_SETTINGS; } }
function schedulerState(): SchedulerState { return scheduler?.exitCode === null ? { running: true, pid: scheduler.pid } : { running: false }; }
function broadcast(type: string, payload: unknown): void { for (const client of clients) client.write(`event: ${type}\ndata: ${JSON.stringify(payload)}\n\n`); }
async function snapshot(): Promise<void> { const [items, config] = await Promise.all([queue.list(), settings()]); broadcast("snapshot", { items, settings: config, scheduler: schedulerState(), slots: Object.fromEntries(scheduleQueue(items, config.interval_hours)) }); }
async function appendEvent(event: EngineEvent): Promise<void> { await appendFile(eventsPath, `${JSON.stringify(event)}\n`); broadcast("log", event); }
function safeItem(value: unknown): Partial<QueueItem> { return typeof value === "object" && value !== null ? value as Partial<QueueItem> : {}; }

async function handler(request: IncomingMessage, response: ServerResponse): Promise<void> {
  const url = new URL(request.url ?? "/", "http://localhost");
  try {
    if (url.pathname === "/api/events") {
      response.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" });
      clients.add(response); await snapshot(); request.on("close", () => clients.delete(response)); return;
    }
    if (request.method === "GET" && url.pathname === "/api/status") {
      const [items, config] = await Promise.all([queue.list(), settings()]);
      return send(response, 200, { items, settings: config, scheduler: schedulerState(), slots: Object.fromEntries(scheduleQueue(items, config.interval_hours)), credentials: ["REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", "REDDIT_USER_AGENT", "REDDIT_USERNAME", "REDDIT_PASSWORD"].map((name) => ({ name, set: Boolean(process.env[name]) })) });
    }
    if (request.method === "GET" && url.pathname === "/api/events/log") { const text = existsSync(eventsPath) ? await readFile(eventsPath, "utf8") : ""; return send(response, 200, text.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line) as EngineEvent)); }
    if (request.method === "POST" && url.pathname === "/api/queue") {
      const input = safeItem(await body(request));
      if (!input.title || !input.subreddit) return send(response, 400, { error: "El título y el subreddit son obligatorios." });
      const item = await queue.add({ title: input.title.slice(0, 300), body: input.body ?? "", subreddit: input.subreddit, status: input.status === POST_STATUS.DRAFT ? POST_STATUS.DRAFT : POST_STATUS.PENDING, ...(input.not_before ? { not_before: input.not_before } : {}) });
      await appendEvent({ timestamp: new Date().toISOString(), level: "info", post_id: item.id, step: "queued", message: `Añadido r/${item.subreddit} a la cola.` }); await snapshot(); return send(response, 201, item);
    }
    const match = url.pathname.match(/^\/api\/queue\/([^/]+)(?:\/(reorder|publish|retry))?$/u);
    if (match !== null && request.method === "PATCH" && match[2] === undefined) {
      const input = safeItem(await body(request)); const items = await queue.update((current) => current.map((item) => item.id === match[1] ? { ...item, ...input, id: item.id, created_at: item.created_at, subreddit: typeof input.subreddit === "string" ? input.subreddit.replace(/^r\//u, "") : item.subreddit } : item)); await snapshot(); return send(response, 200, items.find((item) => item.id === match[1]));
    }
    if (match !== null && request.method === "DELETE" && match[2] === undefined) { const removed = (await queue.list()).find((item) => item.id === match[1]); await queue.update((items) => items.filter((item) => item.id !== match[1])); await snapshot(); return send(response, 200, removed); }
    if (match !== null && request.method === "POST" && match[2] === "reorder") { const input = await body(request); const direction = typeof input === "object" && input !== null && "direction" in input && input.direction === "up" ? -1 : 1; await queue.update((items) => { const index = items.findIndex((item) => item.id === match[1]); const target = index + direction; if (index < 0 || target < 0 || target >= items.length) return items; const copy = [...items]; [copy[index], copy[target]] = [copy[target]!, copy[index]!]; return copy; }); await snapshot(); return send(response, 200, { ok: true }); }
    if (match !== null && request.method === "POST" && match[2] === "retry") { await queue.update((items) => items.map((item) => item.id === match[1] ? { ...item, status: POST_STATUS.PENDING, error: undefined } : item)); await snapshot(); return send(response, 200, { ok: true }); }
    if (match !== null && request.method === "POST" && match[2] === "publish") return runEngine(response, ["publish-id", match[1]!]);
    if (request.method === "PUT" && url.pathname === "/api/settings") { const input = await body(request); const values = typeof input === "object" && input !== null ? input as Record<string, unknown> : {}; const next: Settings = { interval_hours: Number(values["interval_hours"]) || DEFAULT_SETTINGS.interval_hours, check_every_seconds: Number(values["check_every_seconds"]) || DEFAULT_SETTINGS.check_every_seconds }; await writeFile(settingsPath, `${JSON.stringify(next, null, 2)}\n`); await snapshot(); return send(response, 200, next); }
    if (request.method === "POST" && url.pathname === "/api/scheduler/start") { if (schedulerState().running) return send(response, 409, { error: "El planificador ya está activo." }); const config = await settings(); scheduler = spawn(join(ROOT, ".venv/bin/python"), ["reddit_scheduler.py", "loop"], { cwd: ROOT, env: { ...process.env, INTERVAL_HOURS: String(config.interval_hours), CHECK_EVERY_SECONDS: String(config.check_every_seconds) } }); scheduler.on("exit", () => { broadcast("scheduler", schedulerState()); }); await appendEvent({ timestamp: new Date().toISOString(), level: "info", step: "queued", message: "Planificador iniciado." }); broadcast("scheduler", schedulerState()); return send(response, 200, schedulerState()); }
    if (request.method === "POST" && url.pathname === "/api/scheduler/stop") { scheduler?.kill("SIGTERM"); scheduler = undefined; await appendEvent({ timestamp: new Date().toISOString(), level: "info", step: "queued", message: "Planificador detenido." }); broadcast("scheduler", schedulerState()); return send(response, 200, schedulerState()); }
    if (request.method === "POST" && url.pathname === "/api/credentials/check") return runEngine(response, ["check"]);
    return send(response, 404, { error: "No encontrado." });
  } catch (error: unknown) { return send(response, 500, { error: error instanceof Error ? error.message : "Error interno." }); }
}

function runEngine(response: ServerResponse, args: string[]): void { const child = spawn(join(ROOT, ".venv/bin/python"), ["reddit_scheduler.py", ...args], { cwd: ROOT }); child.on("exit", (code) => send(response, code === 0 ? 200 : 400, { ok: code === 0 })); }
watch(ROOT, (_event, filename) => {
  if (filename === "queue.json") void snapshot();
  if (filename === "events.jsonl") {
    void readFile(eventsPath, "utf8").then((text) => {
      const line = text.trim().split("\n").at(-1);
      if (line !== undefined) broadcast("log", JSON.parse(line) as EngineEvent);
    }).catch(() => undefined);
  }
});
const server = createServer((request, response) => { void handler(request, response); });
server.listen(5318, "127.0.0.1", () => console.log("Reddit Scheduler API on http://127.0.0.1:5318"));
process.on("SIGTERM", () => { scheduler?.kill("SIGTERM"); server.close(); });
