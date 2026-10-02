import type {
  AnalyticsSummary,
  EventRecord,
  IngestPayload,
  IngestResponse,
  UserProfile,
} from "./types";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options?.headers ?? {}),
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(body || `Request failed with status ${response.status}`);
  }

  return response.json() as Promise<T>;
}

export const api = {
  getSummary(hours = 24) {
    return request<AnalyticsSummary>(`/v1/analytics/summary?hours=${hours}`);
  },

  getRecentEvents(limit = 12) {
    return request<EventRecord[]>(`/v1/events/recent?limit=${limit}`);
  },

  getUser(userId: string) {
    return request<UserProfile>(`/v1/users/${encodeURIComponent(userId)}`);
  },

  ingestEvent(payload: IngestPayload) {
    return request<IngestResponse>("/v1/events", {
      method: "POST",
      body: JSON.stringify(payload),
    });
  },
};
