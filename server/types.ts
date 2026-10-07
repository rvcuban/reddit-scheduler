export const POST_STATUS = { PENDING: "pending", PUBLISHING: "publishing", POSTED: "posted", FAILED: "failed", DRAFT: "draft" } as const;
export type PostStatus = (typeof POST_STATUS)[keyof typeof POST_STATUS];

export interface QueueItem {
  id: string;
  title: string;
  body: string;
  subreddit: string;
  status: PostStatus;
  created_at: string;
  not_before?: string;
  posted_at?: string;
  url?: string;
  error?: string;
}

export interface Settings {
  interval_hours: number;
  check_every_seconds: number;
}

export interface SchedulerState {
  running: boolean;
  pid?: number;
}

export interface EngineEvent {
  timestamp: string;
  level: string;
  post_id?: string;
  step: string;
  message: string;
}
