export type AnalyticsSummary = {
  window_hours: number;
  total_events: number;
  total_users: number;
  events_by_type: Record<string, number>;
};

export type EventRecord = {
  event_id: string;
  user_id: string;
  event_type: string;
  occurred_at: string;
  received_at: string;
  properties: Record<string, unknown>;
};

export type UserProfile = {
  user_id: string;
  total_events: number;
  purchase_count: number;
  total_purchase_amount: string | number;
  last_seen_at: string | null;
};

export type IngestPayload = {
  event_id: string;
  user_id: string;
  event_type: string;
  timestamp: string;
  properties: Record<string, unknown>;
};

export type IngestResponse = {
  event_id: string;
  accepted: boolean;
  status: string;
  request_id: string | null;
};
