import type { QueueItem } from "./types.js";

/** Computes each pending post's chained publication slot in queue order. */
export function scheduleQueue(items: readonly QueueItem[], intervalHours: number, now: Date = new Date()): Map<string, Date> {
  const slots = new Map<string, Date>();
  const latestPosted = items.reduce<Date | undefined>((latest, item) => {
    if (item.posted_at === undefined) return latest;
    const date = new Date(item.posted_at);
    return Number.isNaN(date.getTime()) || (latest !== undefined && date <= latest) ? latest : date;
  }, undefined);
  let cursor = latestPosted === undefined ? now : new Date(latestPosted.getTime() + intervalHours * 3_600_000);
  for (const item of items) {
    if (item.status !== "pending" && item.status !== "failed") continue;
    const earliest = item.not_before === undefined ? undefined : new Date(item.not_before);
    if (earliest !== undefined && !Number.isNaN(earliest.getTime()) && earliest > cursor) cursor = earliest;
    slots.set(item.id, cursor);
    cursor = new Date(cursor.getTime() + intervalHours * 3_600_000);
  }
  return slots;
}
