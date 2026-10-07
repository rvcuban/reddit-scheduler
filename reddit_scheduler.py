"""Small PRAW scheduler. Commands: check | post TITLE BODY SUB | publish-id ID | run-once | loop."""
from __future__ import annotations

import json
import os
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import praw
from praw.exceptions import RedditAPIException
from prawcore.exceptions import PrawcoreException

BASE_DIR = Path(__file__).resolve().parent
QUEUE_FILE = Path(os.getenv("QUEUE_FILE", BASE_DIR / "queue.json"))
LOCK_FILE = Path(f"{QUEUE_FILE}.lock")
EVENTS_FILE = Path(os.getenv("EVENTS_FILE", BASE_DIR / "events.jsonl"))
INTERVAL = timedelta(hours=float(os.getenv("INTERVAL_HOURS", "48")))
CHECK_EVERY_SECONDS = int(os.getenv("CHECK_EVERY_SECONDS", "3600"))


def load_env(path: Path = BASE_DIR / ".env") -> None:
    if not path.exists(): return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1); os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def emit(level: str, step: str, message: str, post_id: str | None = None) -> None:
    event = {"timestamp": datetime.now(timezone.utc).isoformat(), "level": level, "step": step, "message": message}
    if post_id: event["post_id"] = post_id
    with EVENTS_FILE.open("a", encoding="utf-8") as output: output.write(json.dumps(event, ensure_ascii=False) + "\n")
    print(f"{event['timestamp']} {message}", flush=True)


@contextmanager
def queue_lock():
    """Cross-process exclusive-create lock shared with the Node server."""
    for _ in range(100):
        try:
            descriptor = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(descriptor, str(os.getpid()).encode()); os.close(descriptor)
            break
        except FileExistsError: time.sleep(0.02)
    else: raise RuntimeError("Queue is busy; try again shortly.")
    try: yield
    finally: LOCK_FILE.unlink(missing_ok=True)


def read_queue() -> list[dict]:
    if not QUEUE_FILE.exists(): return []
    items = json.loads(QUEUE_FILE.read_text(encoding="utf-8"))
    changed = False
    for item in items:
        if not item.get("id"): item["id"] = str(uuid.uuid4()); changed = True
        if not item.get("created_at"): item["created_at"] = datetime.now(timezone.utc).isoformat(); changed = True
    if changed: save_queue(items)
    return items


def save_queue(items: list[dict]) -> None:
    temporary = QUEUE_FILE.with_suffix(".tmp")
    temporary.write_text(json.dumps(items, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(QUEUE_FILE)


def get_reddit() -> praw.Reddit:
    required = ("REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", "REDDIT_USER_AGENT", "REDDIT_USERNAME", "REDDIT_PASSWORD")
    missing = [name for name in required if not os.getenv(name)]
    if missing: raise SystemExit(f"Missing env vars: {', '.join(missing)}")
    return praw.Reddit(client_id=os.environ["REDDIT_CLIENT_ID"], client_secret=os.environ["REDDIT_CLIENT_SECRET"], user_agent=os.environ["REDDIT_USER_AGENT"], username=os.environ["REDDIT_USERNAME"], password=os.environ["REDDIT_PASSWORD"], ratelimit_seconds=300)


def publish(reddit: praw.Reddit, item: dict) -> str:
    submission = reddit.subreddit(item["subreddit"]).submit(title=item["title"], selftext=item.get("body", ""))
    return f"https://www.reddit.com{submission.permalink}"


def last_posted(items: list[dict]) -> datetime | None:
    times = [datetime.fromisoformat(item["posted_at"]) for item in items if item.get("posted_at")]
    return max(times) if times else None


def publish_item(reddit: praw.Reddit, post_id: str) -> None:
    with queue_lock():
        items = read_queue(); item = next((item for item in items if item.get("id") == post_id), None)
        if item is None: raise SystemExit(f"Post not found: {post_id}")
        item["status"] = "publishing"; item.pop("error", None); save_queue(items)
    emit("info", "publishing", f"Publishing r/{item['subreddit']}…", post_id)
    try: url = publish(reddit, item)
    except (RedditAPIException, PrawcoreException) as exc:
        with queue_lock():
            items = read_queue(); target = next(value for value in items if value.get("id") == post_id); target.update(status="failed", error=str(exc)); save_queue(items)
        emit("error", "failed", f"Failed r/{item['subreddit']}: {exc}", post_id); return
    with queue_lock():
        items = read_queue(); target = next(value for value in items if value.get("id") == post_id); target.update(status="posted", posted_at=datetime.now(timezone.utc).isoformat(), url=url); target.pop("error", None); save_queue(items)
    emit("info", "posted", f"Posted r/{item['subreddit']}: {url}", post_id)


def run_once(reddit: praw.Reddit) -> None:
    with queue_lock(): items = read_queue()
    last = last_posted(items); now = datetime.now(timezone.utc)
    if last and now - last < INTERVAL:
        emit("info", "not_due", f"Not due yet. Next slot: {(last + INTERVAL).astimezone().isoformat(timespec='minutes')}"); return
    item = next((value for value in items if value.get("status", "pending") == "pending" and (not value.get("not_before") or datetime.fromisoformat(value["not_before"]) <= now)), None)
    if item is None: emit("info", "skipped", "Queue empty or no post is due."); return
    publish_item(reddit, item["id"])


def loop(reddit: praw.Reddit) -> None:
    emit("info", "queued", f"Scheduler started. Interval {INTERVAL}, checking every {CHECK_EVERY_SECONDS}s.")
    while True:
        try: run_once(reddit)
        except Exception as exc: emit("error", "failed", f"Unexpected error: {exc}")
        time.sleep(CHECK_EVERY_SECONDS)


def main(argv: list[str]) -> None:
    load_env(); command = argv[1] if len(argv) > 1 else "run-once"
    if command == "check":
        try: emit("info", "auth_ok", f"Authenticated as u/{get_reddit().user.me()}")
        except Exception as exc: emit("error", "auth_failed", str(exc)); raise
    elif command == "post" and len(argv) == 5:
        item = {"title": argv[2], "body": argv[3], "subreddit": argv[4]}; print(publish(get_reddit(), item))
    elif command == "publish-id" and len(argv) == 3: publish_item(get_reddit(), argv[2])
    elif command == "run-once": run_once(get_reddit())
    elif command == "loop": loop(get_reddit())
    else: raise SystemExit(__doc__)


if __name__ == "__main__": main(sys.argv)
