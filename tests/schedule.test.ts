import { describe, expect, it } from "vitest";
import { scheduleQueue } from "../server/schedule.js";
import type { QueueItem } from "../server/types.js";

const item = (id: string, extra: Partial<QueueItem> = {}): QueueItem => ({ id, title: id, body: "", subreddit: "test", status: "pending", created_at: "2026-01-01T00:00:00Z", ...extra });
describe("scheduleQueue", () => {
  it("chains future slots after the last publication", () => {
    const slots = scheduleQueue([item("posted", { status: "posted", posted_at: "2026-01-01T00:00:00Z" }), item("one"), item("two")], 48, new Date("2026-01-01T01:00:00Z"));
    expect(slots.get("one")?.toISOString()).toBe("2026-01-03T00:00:00.000Z");
    expect(slots.get("two")?.toISOString()).toBe("2026-01-05T00:00:00.000Z");
  });
  it("honours no-earlier-than dates", () => { const slots = scheduleQueue([item("one", { not_before: "2026-01-05T12:00:00Z" })], 24, new Date("2026-01-01T00:00:00Z")); expect(slots.get("one")?.toISOString()).toBe("2026-01-05T12:00:00.000Z"); });
});
