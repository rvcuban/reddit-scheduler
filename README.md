# Reddit Scheduler

Local-only queue and scheduler UI for text posts. It works without credentials: compose posts, arrange the queue, view scheduled slots and activity. Reddit is contacted only when you explicitly use **Comprobar**, **Publicar ahora**, or start the scheduler with valid credentials.

## Setup

1. Create a Reddit app at [reddit.com/prefs/apps](https://www.reddit.com/prefs/apps) with type **script**.
2. Copy `.env.example` to `.env` and fill in `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`, `REDDIT_USER_AGENT`, `REDDIT_USERNAME`, and `REDDIT_PASSWORD`.
3. Install Python requirements in `.venv` and install the UI dependencies: `pnpm install`.
4. Run `pnpm dev`, then open <http://localhost:5173>.

`queue.json`, `settings.json`, and `events.jsonl` are created locally and ignored by Git.

## Architecture

- `ui/` — Vite + React 19 UI, SSE subscription, accessible queue/editor/activity/settings views.
- `server/index.ts` — local Node API and SSE bridge on port 5318.
- `server/queue-store.ts` — migration, exclusive lock and atomic queue writes.
- `server/schedule.ts` — pure chained scheduling calculations.
- `reddit_scheduler.py` — PRAW engine, structured event emission and scheduler loop.

## API

- `GET /api/status`, `GET /api/events/log`, `GET /api/events` (SSE)
- `POST /api/queue`, `PATCH|DELETE /api/queue/:id`, `POST /api/queue/:id/reorder|retry|publish`
- `PUT /api/settings`, `POST /api/scheduler/start|stop`, `POST /api/credentials/check`

## Consistency strategy

The Node store and Python engine use the same exclusive-create `queue.lock` protocol before a queue read-modify-write operation. Every queue update writes a temporary file then atomically renames it into place. This prevents either process from committing partial JSON or overwriting another process's completed update.
