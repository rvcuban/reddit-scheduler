import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { QueueStore } from "../server/queue-store.js";

const folders: string[] = [];
async function fixture(): Promise<{ store: QueueStore; path: string }> { const folder = await mkdtemp(join(tmpdir(), "reddit-queue-")); folders.push(folder); const path = join(folder, "queue.json"); return { store: new QueueStore(path), path }; }
afterEach(async () => { await Promise.all(folders.splice(0).map((folder) => rm(folder, { recursive: true, force: true }))); });
describe("QueueStore", () => {
  it("migrates legacy entries and persists stable fields", async () => { const { store, path } = await fixture(); await writeFile(path, JSON.stringify([{ title: "Old", subreddit: "r/test", status: "pending" }])); const [item] = await store.list(); expect(item?.id).toBeTruthy(); expect(item?.created_at).toBeTruthy(); expect(JSON.parse(await readFile(path, "utf8"))[0].id).toBe(item?.id); });
  it("reorders items in an atomic update", async () => { const { store } = await fixture(); const first = await store.add({ title: "One", body: "", subreddit: "a", status: "pending" }); const second = await store.add({ title: "Two", body: "", subreddit: "b", status: "pending" }); await store.update((items) => [items[1]!, items[0]!]); expect((await store.list()).map((item) => item.id)).toEqual([second.id, first.id]); });
});
