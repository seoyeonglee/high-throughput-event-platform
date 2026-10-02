import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";

import { api } from "./api";
import type { AnalyticsSummary, EventRecord, UserProfile } from "./types";

const EVENT_TYPES = ["product_view", "add_to_cart", "purchase", "login"];

function formatTime(value: string | null) {
  if (!value) return "—";
  return new Intl.DateTimeFormat("ko-KR", {
    dateStyle: "short",
    timeStyle: "medium",
  }).format(new Date(value));
}

function App() {
  const [summary, setSummary] = useState<AnalyticsSummary | null>(null);
  const [events, setEvents] = useState<EventRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [notice, setNotice] = useState("API와 실시간으로 연결되는 운영 콘솔입니다.");
  const [userId, setUserId] = useState("user_123");
  const [profile, setProfile] = useState<UserProfile | null>(null);

  const loadDashboard = useCallback(async () => {
    setLoading(true);
    try {
      const [nextSummary, nextEvents] = await Promise.all([
        api.getSummary(24),
        api.getRecentEvents(12),
      ]);
      setSummary(nextSummary);
      setEvents(nextEvents);
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "대시보드 데이터를 불러오지 못했습니다.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadDashboard();
  }, [loadDashboard]);

  const maxCount = useMemo(
    () => Math.max(1, ...Object.values(summary?.events_by_type ?? {})),
    [summary],
  );

  async function handleCreateEvent(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const eventType = String(form.get("eventType") || "product_view");
    const currentUserId = String(form.get("userId") || "user_123");
    const productId = String(form.get("productId") || "SKU-DEMO");

    try {
      const result = await api.ingestEvent({
        event_id: `evt_${crypto.randomUUID()}`,
        user_id: currentUserId,
        event_type: eventType,
        timestamp: new Date().toISOString(),
        properties: {
          product_id: productId,
          source: "portfolio-dashboard",
          amount: eventType === "purchase" ? 89000 : undefined,
          currency: eventType === "purchase" ? "KRW" : undefined,
        },
      });
      setNotice(
        result.accepted
          ? `${eventType} 이벤트가 큐에 접수되었습니다. 비동기 worker 처리 후 지표에 반영됩니다.`
          : "중복 이벤트로 판단되어 재처리하지 않았습니다.",
      );
      window.setTimeout(() => void loadDashboard(), 900);
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "이벤트 전송에 실패했습니다.");
    }
  }

  async function handleLookupUser(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setProfile(null);
    try {
      setProfile(await api.getUser(userId));
      setNotice(`${userId} 사용자 프로필을 조회했습니다.`);
    } catch {
      setNotice("아직 처리된 사용자 프로필이 없습니다. 먼저 이벤트를 생성해 보세요.");
    }
  }

  return (
    <main className="app-shell">
      <header className="hero">
        <div>
          <p className="eyebrow">FULL-STACK OPERATIONS CONSOLE</p>
          <h1>High-Throughput Event Platform</h1>
          <p className="hero-copy">
            React/TypeScript UI에서 FastAPI로 이벤트를 보내고, Redis Streams worker와
            PostgreSQL 처리 결과를 다시 읽어오는 End-to-End 데모입니다.
          </p>
        </div>
        <button className="secondary-button" onClick={() => void loadDashboard()}>
          {loading ? "새로고침 중…" : "데이터 새로고침"}
        </button>
      </header>

      <section className="notice" aria-live="polite">{notice}</section>

      <section className="metrics-grid">
        <article className="metric-card">
          <span>최근 24시간 이벤트</span>
          <strong>{summary?.total_events ?? "—"}</strong>
        </article>
        <article className="metric-card">
          <span>누적 사용자</span>
          <strong>{summary?.total_users ?? "—"}</strong>
        </article>
        <article className="metric-card">
          <span>이벤트 타입</span>
          <strong>{Object.keys(summary?.events_by_type ?? {}).length || "—"}</strong>
        </article>
        <article className="metric-card">
          <span>처리 모델</span>
          <strong className="metric-word">Async</strong>
        </article>
      </section>

      <section className="two-column">
        <article className="panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">INGESTION</p>
              <h2>실시간 이벤트 생성</h2>
            </div>
            <span className="pill">POST /v1/events</span>
          </div>
          <form className="stack-form" onSubmit={handleCreateEvent}>
            <label>
              사용자 ID
              <input name="userId" defaultValue="user_123" required />
            </label>
            <label>
              이벤트 유형
              <select name="eventType" defaultValue="product_view">
                {EVENT_TYPES.map((eventType) => (
                  <option key={eventType} value={eventType}>{eventType}</option>
                ))}
              </select>
            </label>
            <label>
              상품 ID
              <input name="productId" defaultValue="SKU-3812" required />
            </label>
            <button className="primary-button" type="submit">이벤트 전송</button>
          </form>
        </article>

        <article className="panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">ANALYTICS</p>
              <h2>이벤트 분포</h2>
            </div>
            <span className="pill">GET /v1/analytics/summary</span>
          </div>
          <div className="bars">
            {Object.entries(summary?.events_by_type ?? {}).length === 0 && (
              <p className="empty">이벤트를 생성하면 유형별 분포가 표시됩니다.</p>
            )}
            {Object.entries(summary?.events_by_type ?? {}).map(([name, count]) => (
              <div className="bar-row" key={name}>
                <div className="bar-label">
                  <span>{name}</span>
                  <strong>{count}</strong>
                </div>
                <div className="bar-track">
                  <div className="bar-fill" style={{ width: `${(count / maxCount) * 100}%` }} />
                </div>
              </div>
            ))}
          </div>
        </article>
      </section>

      <section className="two-column">
        <article className="panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">USER AGGREGATE</p>
              <h2>사용자 프로필 조회</h2>
            </div>
            <span className="pill">GET /v1/users/:id</span>
          </div>
          <form className="inline-form" onSubmit={handleLookupUser}>
            <input value={userId} onChange={(event) => setUserId(event.target.value)} required />
            <button className="secondary-button" type="submit">조회</button>
          </form>
          {profile && (
            <dl className="profile-grid">
              <div><dt>총 이벤트</dt><dd>{profile.total_events}</dd></div>
              <div><dt>구매 횟수</dt><dd>{profile.purchase_count}</dd></div>
              <div><dt>누적 구매액</dt><dd>{Number(profile.total_purchase_amount).toLocaleString()}원</dd></div>
              <div><dt>최근 활동</dt><dd>{formatTime(profile.last_seen_at)}</dd></div>
            </dl>
          )}
        </article>

        <article className="panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">ARCHITECTURE</p>
              <h2>처리 경로</h2>
            </div>
          </div>
          <ol className="flow-list">
            <li><span>01</span><div><strong>React UI</strong><p>이벤트 생성 · 조회 · 운영 시각화</p></div></li>
            <li><span>02</span><div><strong>FastAPI</strong><p>검증 · rate limit · idempotency</p></div></li>
            <li><span>03</span><div><strong>Redis Streams</strong><p>비동기 큐 · consumer group · retry/DLQ</p></div></li>
            <li><span>04</span><div><strong>PostgreSQL</strong><p>durable event · user aggregate</p></div></li>
          </ol>
        </article>
      </section>

      <section className="panel">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">RECENT EVENTS</p>
            <h2>최근 처리 이벤트</h2>
          </div>
          <span className="pill">GET /v1/events/recent</span>
        </div>
        <div className="table-wrap">
          <table>
            <thead><tr><th>Event</th><th>User</th><th>Type</th><th>Occurred</th><th>Properties</th></tr></thead>
            <tbody>
              {events.map((item) => (
                <tr key={item.event_id}>
                  <td className="mono">{item.event_id}</td>
                  <td>{item.user_id}</td>
                  <td><span className="event-chip">{item.event_type}</span></td>
                  <td>{formatTime(item.occurred_at)}</td>
                  <td className="mono properties">{JSON.stringify(item.properties)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {!loading && events.length === 0 && <p className="empty table-empty">아직 처리된 이벤트가 없습니다.</p>}
        </div>
      </section>
    </main>
  );
}

export default App;
