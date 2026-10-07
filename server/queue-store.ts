import { randomUUID } from "node:crypto";
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { POST_STATUS, type QueueItem } from "./types.js";

interface LegacyQueueItem {
  title?: unknown; body?: unknown; subreddit?: unknown; status?: unknown; id?: unknown; created_at?: unknown;
  not_before?: unknown; posted_at?: unknown; url?: unknown; error?: unknown;
}

function asString(value: unknown, fallback = ""): string { return typeof value === "string" ? value : fallback; }

export function migrateItem(input: LegacyQueueItem): QueueItem {
  const statuses = new Set(Object.values(POST_STATUS));
  const status = asString(input.status, POST_STATUS.PENDING);
  return {
    id: asString(input.id) || randomUUID(), title: asString(input.title), body: asString(input.body),
    subreddit: asString(input.subreddit).replace(/^r\//u, ""),
    status: statuses.has(status as QueueItem["status"]) ? status as QueueItem["status"] : POST_STATUS.PENDING,
    created_at: asString(input.created_at) || new Date().toISOString(),
    ...(typeof input.not_before === "string" ? { not_before: input.not_before } : {}),
    ...(typeof input.posted_at === "string" ? { posted_at: input.posted_at } : {}),
    ...(typeof input.url === "string" ? { url: input.url } : {}),
    ...(typeof input.error === "string" ? { error: input.error } : {}),
  };
}

/** Queue writes use an exclusive queue.lock file and atomic rename, matching the Python protocol. */
export class QueueStore {
  public constructor(private readonly path: string) {}

  public async list(): Promise<QueueItem[]> {
    const raw = await this.readRaw();
    const migrated = raw.map(migrateItem);
    if (JSON.stringify(raw) !== JSON.stringify(migrated)) await this.write(migrated);
    return migrated;
  }

  public async update(mutator: (items: QueueItem[]) => QueueItem[]): Promise<QueueItem[]> {
    await mkdir(dirname(this.path), { recursive: true });
    await this.acquireLock();
    try {
      const items = (await this.readRaw()).map(migrateItem);
      const next = mutator(items);
      await this.write(next);
      return next;
    } finally { await rm(`${this.path}.lock`, { force: true }); }
  }

  public async add(item: Omit<QueueItem, "id" | "created_at">): Promise<QueueItem> {
    const created: QueueItem = { ...item, id: randomUUID(), created_at: new Date().toISOString() };
    await this.update((items) => [...items, created]);
    return created;
  }

  private async readRaw(): Promise<LegacyQueueItem[]> {
    try { const parsed: unknown = JSON.parse(await readFile(this.path, "utf8")); return Array.isArray(parsed) ? parsed as LegacyQueueItem[] : []; }
    catch (error: unknown) { if (error instanceof Error && "code" in error && error.code === "ENOENT") return []; throw error; }
  }

  private async write(items: readonly QueueItem[]): Promise<void> {
    await mkdir(dirname(this.path), { recursive: true });
    const temp = `${this.path}.${process.pid}.tmp`;
    await writeFile(temp, `${JSON.stringify(items, null, 2)}\n`, "utf8");
    await rename(temp, this.path);
  }

  private async acquireLock(): Promise<void> {
    const lockPath = `${this.path}.lock`;
    for (let attempt = 0; attempt < 100; attempt += 1) {
      try { await writeFile(lockPath, String(process.pid), { flag: "wx" }); return; }
      catch (error: unknown) {
        if (!(error instanceof Error) || !("code" in error) || error.code !== "EEXIST") throw error;
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
    }
    throw new Error("Queue is busy; try again shortly.");
  }
}
