import { useEffect, useMemo, useState } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

const API_BASE =
  import.meta.env.VITE_API_BASE ||
  `${window.location.protocol}//${window.location.hostname}:5000`;
const AUTH_TOKEN_KEY = "ole_auth_token";
const GENERATOR_TANK = "Generator Tank";
const EXTERNAL_TANK = "External Tank";

const clampPercent = (value) => Math.max(0, Math.min(100, Number(value || 0)));

const formatTime = (value) => {
  if (!value) return "-";
  return new Date(value).toLocaleString();
};

const formatHours = (value) => {
  if (value == null || Number.isNaN(Number(value))) return "--";
  return `${Number(value).toFixed(1)} h`;
};

const statusFromLatestEvent = (eventType) => {
  if (eventType === "abnormal_drop") return "Critical";
  if (eventType === "refill") return "Refilled";
  return "Stable";
};

function StatCard({ title, value, subtitle, tone = "default" }) {
  return (
    <div className={`card card-${tone}`}>
      <p className="card-title">{title}</p>
      <h3 className="card-value">{value}</h3>
      <p className="card-subtitle">{subtitle}</p>
    </div>
  );
}

export default function App() {
  const [metrics, setMetrics] = useState(null);
  const [events, setEvents] = useState([]);
  const [solenoidCommands, setSolenoidCommands] = useState([]);
  const [iotOverview, setIotOverview] = useState({ fleet: { total: 0, online: 0, degraded: 0, offline: 0 }, devices: [] });
  const [alerts, setAlerts] = useState({ summary: { total: 0, critical: 0, high: 0, warning: 0 }, alerts: [] });
  const [refillRequests, setRefillRequests] = useState({ summary: { total: 0, pending: 0, executed: 0, rejected: 0 }, requests: [] });
  const [authToken, setAuthToken] = useState(localStorage.getItem(AUTH_TOKEN_KEY) || "");
  const [authUser, setAuthUser] = useState(null);
  const [mustChangePassword, setMustChangePassword] = useState(false);
  const [loginUsername, setLoginUsername] = useState("worker");
  const [loginPassword, setLoginPassword] = useState("Worker@123");
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [authLoading, setAuthLoading] = useState(true);
  const [alertsBusy, setAlertsBusy] = useState(false);
  const [refillBusy, setRefillBusy] = useState(false);
  const [error, setError] = useState("");
  const [vpsStatus, setVpsStatus] = useState("Checking");
  const canControl = authUser?.role === "manager" || authUser?.role === "admin";

  const authHeaders = useMemo(
    () =>
      authToken
        ? {
            Authorization: `Bearer ${authToken}`,
          }
        : {},
    [authToken]
  );

  const loadData = async () => {
    try {
      const [healthResponse, metricsResponse, eventsResponse, solenoidResponse, iotResponse, alertsResponse, refillResponse] = await Promise.all([
        fetch(`${API_BASE}/api/health`),
        fetch(`${API_BASE}/api/metrics`, { headers: authHeaders }),
        fetch(`${API_BASE}/api/events`, { headers: authHeaders }),
        fetch(`${API_BASE}/api/solenoid/commands`, { headers: authHeaders }),
        fetch(`${API_BASE}/api/iot/overview`, { headers: authHeaders }),
        fetch(`${API_BASE}/api/alerts?refresh=true&limit=20`, { headers: authHeaders }),
        fetch(`${API_BASE}/api/refill/requests?limit=20`, { headers: authHeaders }),
      ]);

      setVpsStatus(healthResponse.ok ? "Online" : "Degraded");

      if (eventsResponse.status === 401 || metricsResponse.status === 401) {
        setAuthToken("");
        localStorage.removeItem(AUTH_TOKEN_KEY);
        setAuthUser(null);
        throw new Error("Session expired. Please login again.");
      }

      if (!eventsResponse.ok) {
        throw new Error("Failed to load events");
      }

      const eventsJson = await eventsResponse.json();
      setEvents(eventsJson.events || []);

      if (solenoidResponse.ok) {
        const solenoidJson = await solenoidResponse.json();
        setSolenoidCommands(solenoidJson.commands || []);
      }

      if (iotResponse.ok) {
        const iotJson = await iotResponse.json();
        setIotOverview({
          fleet: iotJson.fleet || { total: 0, online: 0, degraded: 0, offline: 0 },
          devices: iotJson.devices || [],
        });
      }

      if (alertsResponse.ok) {
        const alertsJson = await alertsResponse.json();
        setAlerts({
          summary: alertsJson.summary || { total: 0, critical: 0, high: 0, warning: 0 },
          alerts: alertsJson.alerts || [],
        });
      }

      if (refillResponse.ok) {
        const refillJson = await refillResponse.json();
        setRefillRequests({
          summary: refillJson.summary || { total: 0, pending: 0, executed: 0, rejected: 0 },
          requests: refillJson.requests || [],
        });
      }

      if (metricsResponse.ok) {
        const metricsJson = await metricsResponse.json();
        setMetrics(metricsJson);
      }

      if (!metricsResponse.ok && metricsResponse.status !== 404) {
        throw new Error("Failed to load metrics");
      }

      setError("");
    } catch (requestError) {
      setVpsStatus("Offline");
      setError(requestError.message || "Unexpected API error");
    }
  };

  useEffect(() => {
    const verify = async () => {
      if (!authToken) {
        setAuthLoading(false);
        return;
      }
      try {
        const response = await fetch(`${API_BASE}/api/auth/me`, {
          headers: authHeaders,
        });
        if (!response.ok) {
          throw new Error("Invalid session");
        }
        const payload = await response.json();
        setAuthUser(payload.user || null);
        setMustChangePassword(Boolean(payload.user?.must_change_password));
      } catch {
        setAuthToken("");
        localStorage.removeItem(AUTH_TOKEN_KEY);
        setAuthUser(null);
        setMustChangePassword(false);
      } finally {
        setAuthLoading(false);
      }
    };

    verify();
  }, [authHeaders, authToken]);

  useEffect(() => {
    if (!authUser || mustChangePassword) {
      return undefined;
    }
    loadData();
    const timer = setInterval(loadData, 5000);
    return () => clearInterval(timer);
  }, [authUser, mustChangePassword]);

  const handleLogin = async (event) => {
    event.preventDefault();
    try {
      const response = await fetch(`${API_BASE}/api/auth/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: loginUsername, password: loginPassword }),
      });

      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.error || "Login failed");
      }

      setAuthToken(payload.token);
      localStorage.setItem(AUTH_TOKEN_KEY, payload.token);
      setAuthUser(payload.user);
      setMustChangePassword(Boolean(payload.user?.must_change_password));
      setCurrentPassword(loginPassword);
      setError("");
    } catch (loginError) {
      setError(loginError.message || "Unable to login");
    }
  };

  const handleLogout = () => {
    setAuthToken("");
    setAuthUser(null);
    setMustChangePassword(false);
    localStorage.removeItem(AUTH_TOKEN_KEY);
  };

  const handleChangePassword = async (event) => {
    event.preventDefault();
    if (newPassword.length < 8) {
      setError("New password must be at least 8 characters.");
      return;
    }
    if (newPassword !== confirmPassword) {
      setError("New password and confirmation do not match.");
      return;
    }

    try {
      const response = await fetch(`${API_BASE}/api/auth/change-password`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          current_password: currentPassword,
          new_password: newPassword,
        }),
      });

      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.error || "Password update failed");
      }

      setAuthToken(payload.token);
      localStorage.setItem(AUTH_TOKEN_KEY, payload.token);
      setAuthUser(payload.user);
      setMustChangePassword(false);
      setCurrentPassword("");
      setNewPassword("");
      setConfirmPassword("");
      setError("");
    } catch (changeError) {
      setError(changeError.message || "Unable to update password");
    }
  };

  const runAlertAction = async (alertKey, action, silenceMinutes = 30) => {
    if (!canControl) return;

    try {
      setAlertsBusy(true);
      const response = await fetch(`${API_BASE}/api/alerts/${encodeURIComponent(alertKey)}/action`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({ action, silence_minutes: silenceMinutes }),
      });

      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.error || "Alert action failed");
      }

      if (payload.alerts) {
        setAlerts(payload.alerts);
      }
      setError("");
    } catch (actionError) {
      setError(actionError.message || "Unable to perform alert action");
    } finally {
      setAlertsBusy(false);
    }
  };

  const runRefillAction = async (requestId, action) => {
    if (!canControl) return;

    try {
      setRefillBusy(true);
      const response = await fetch(`${API_BASE}/api/refill/requests/${requestId}/action`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({ action }),
      });

      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.error || "Refill action failed");
      }

      if (payload.refill_requests) {
        setRefillRequests(payload.refill_requests);
      }
      setError("");
    } catch (actionError) {
      setError(actionError.message || "Unable to perform refill action");
    } finally {
      setRefillBusy(false);
    }
  };

  const latestEvent = events[0];
  const latestSolenoidCommand = solenoidCommands[0];
  const topCriticalAlert = useMemo(
    () => alerts.alerts.find((alert) => alert.severity === "critical"),
    [alerts]
  );
  const primaryDevice = useMemo(
    () => iotOverview.devices.find((device) => device.tank_name === GENERATOR_TANK) || iotOverview.devices[0],
    [iotOverview]
  );
  const onlineRate = iotOverview.fleet.total
    ? (iotOverview.fleet.online / iotOverview.fleet.total) * 100
    : 0;
  const generatorLevel =
    metrics?.tank_levels?.[GENERATOR_TANK] != null
      ? clampPercent(metrics.tank_levels[GENERATOR_TANK])
      : null;
  const externalLevel =
    metrics?.tank_levels?.[EXTERNAL_TANK] != null
      ? clampPercent(metrics.tank_levels[EXTERNAL_TANK])
      : null;
  const chartData = useMemo(
    () =>
      [...events]
        .filter((event) => event.tank_name === GENERATOR_TANK)
        .reverse()
        .map((event) => ({
          time: new Date(event.recorded_at).toLocaleTimeString([], {
            hour: "2-digit",
            minute: "2-digit",
          }),
          fuel: Number(event.fuel_level),
        })),
    [events]
  );

  if (authLoading) {
    return <div className="app-shell"><main className="dashboard"><section className="panel">Checking session...</section></main></div>;
  }

  if (!authUser) {
    return (
      <div className="app-shell">
        <header className="top-bar">
          <div className="logo-pill">OIL LIBYA ETHIOPIA</div>
        </header>
        <main className="dashboard auth-dashboard">
          {error && <section className="error-banner">{error}</section>}
          <section className="panel auth-panel">
            <div className="panel-header">
              <h2>Secure Operations Login</h2>
            </div>
            <p className="auth-subtitle">
              Authenticate to access real-time telemetry, alerts, and role-based control actions.
            </p>
            <form className="auth-form" onSubmit={handleLogin}>
              <label className="auth-label" htmlFor="username-input">Username</label>
              <input
                id="username-input"
                className="operator-input"
                value={loginUsername}
                onChange={(event) => setLoginUsername(event.target.value)}
                placeholder="Username"
                autoComplete="username"
              />
              <label className="auth-label" htmlFor="password-input">Password</label>
              <input
                id="password-input"
                className="operator-input"
                type="password"
                value={loginPassword}
                onChange={(event) => setLoginPassword(event.target.value)}
                placeholder="Password"
                autoComplete="current-password"
              />
              <button type="submit" className="action-btn auth-btn">Login</button>
            </form>
            <div className="auth-hint-grid">
              <p><strong>Worker:</strong> read-only dashboards</p>
              <p><strong>Manager:</strong> approve refill + actions</p>
              <p><strong>Admin:</strong> full control + privileged resolve</p>
            </div>
          </section>
        </main>
      </div>
    );
  }

  if (mustChangePassword) {
    return (
      <div className="app-shell">
        <header className="top-bar">
          <div className="logo-pill">OIL LIBYA ETHIOPIA</div>
          <div className="session-pill">{authUser.username} · {authUser.role}</div>
        </header>
        <main className="dashboard auth-dashboard">
          {error && <section className="error-banner">{error}</section>}
          <section className="panel auth-panel">
            <div className="panel-header">
              <h2>Password Update Required</h2>
            </div>
            <p className="auth-subtitle">
              For security, change your default password before accessing the operations dashboard.
            </p>
            <form className="auth-form" onSubmit={handleChangePassword}>
              <label className="auth-label" htmlFor="current-password-input">Current Password</label>
              <input
                id="current-password-input"
                className="operator-input"
                type="password"
                value={currentPassword}
                onChange={(event) => setCurrentPassword(event.target.value)}
                placeholder="Current password"
                autoComplete="current-password"
              />
              <label className="auth-label" htmlFor="new-password-input">New Password</label>
              <input
                id="new-password-input"
                className="operator-input"
                type="password"
                value={newPassword}
                onChange={(event) => setNewPassword(event.target.value)}
                placeholder="New password (min 8 chars)"
                autoComplete="new-password"
              />
              <label className="auth-label" htmlFor="confirm-password-input">Confirm New Password</label>
              <input
                id="confirm-password-input"
                className="operator-input"
                type="password"
                value={confirmPassword}
                onChange={(event) => setConfirmPassword(event.target.value)}
                placeholder="Confirm new password"
                autoComplete="new-password"
              />
              <button type="submit" className="action-btn auth-btn">Update Password</button>
            </form>
          </section>
        </main>
      </div>
    );
  }

  return (
    <div className="app-shell">
      <header className="top-bar">
        <div className="logo-pill">OIL LIBYA ETHIOPIA</div>
        <div className="session-pill">
          {authUser.username} · {authUser.role}
          <button type="button" className="action-btn" onClick={handleLogout}>Logout</button>
        </div>
      </header>

      <main className="dashboard">
        {topCriticalAlert && (
          <section className="alert-banner">
            🚨 {topCriticalAlert.title}: {topCriticalAlert.message}
          </section>
        )}

        {!topCriticalAlert && latestEvent?.event_type === "abnormal_drop" && (
          <section className="alert-banner">
            ⚠ Abnormal drop detected in {latestEvent.tank_name} at {formatTime(latestEvent.recorded_at)}.
          </section>
        )}

        {error && <section className="error-banner">{error}</section>}

        <section className="hero-panel">
          <div>
            <p className="hero-kicker">Real-time Energy Intelligence</p>
            <h1>Fuel Command Center</h1>
            <p className="hero-subtitle">
              Live monitoring for generator reliability with automatic refill flow from external reserve.
            </p>
            <div className="stack-badges">
              <span className="badge badge-esp">ESP32 Module</span>
              <span className="badge badge-vps">VPS Flask API</span>
              <span className="badge badge-ui">React Dashboard</span>
            </div>
          </div>
          <div className="hero-metrics">
            <div>
              <span>Generator</span>
              <strong>{generatorLevel != null ? `${generatorLevel.toFixed(2)}%` : "--"}</strong>
            </div>
            <div>
              <span>External</span>
              <strong>{externalLevel != null ? `${externalLevel.toFixed(2)}%` : "--"}</strong>
            </div>
            <div className="status-card">
              <span>VPS Link</span>
              <strong className={`status-${vpsStatus.toLowerCase()}`}>{vpsStatus}</strong>
            </div>
          </div>
        </section>

        <section className="pipeline-panel">
          <h2>System Data Flow</h2>
          <div className="pipeline-row">
            <div className="pipeline-node">
              <p>ESP32 SENSOR NODE</p>
              <strong>Sends fuel payloads</strong>
              <small>Tank level + timestamp + consumption</small>
            </div>
            <div className="pipeline-arrow">→</div>
            <div className="pipeline-node">
              <p>VPS SERVER</p>
              <strong>Flask + SQLite API</strong>
              <small>Receives `/api/ingest`, applies anomaly logic</small>
            </div>
            <div className="pipeline-arrow">→</div>
            <div className="pipeline-node">
              <p>WEB DASHBOARD</p>
              <strong>Operations View</strong>
              <small>Auto refresh every 5 seconds for clients</small>
            </div>
          </div>
        </section>

        <section className="cards-grid">
          <StatCard
            title="Generator Tank"
            value={generatorLevel != null ? `${generatorLevel.toFixed(2)}%` : "--"}
            subtitle={`Minimum ${Number(metrics?.minimum_level || 25).toFixed(0)}%`}
            tone="primary"
          />
          <StatCard
            title="External Tank"
            value={externalLevel != null ? `${externalLevel.toFixed(2)}%` : "--"}
            subtitle="Refill source"
            tone="accent"
          />
          <StatCard
            title="Generator Consumption"
            value={metrics ? `${Number(metrics.consumption_rate).toFixed(2)}%/h` : "--"}
            subtitle="Current estimated usage"
          />
          <StatCard
            title="Last Update"
            value={metrics ? formatTime(metrics.last_update_time) : "--"}
            subtitle="Auto-refresh every 5 seconds"
          />
          <StatCard
            title="Tank Status"
            value={metrics?.tank_status || statusFromLatestEvent(latestEvent?.event_type)}
            subtitle={latestEvent ? `${latestEvent.event_type} · ${latestEvent.tank_name}` : "No events yet"}
            tone={latestEvent?.event_type === "abnormal_drop" ? "danger" : "success"}
          />
        </section>

        <section className="iot-strip">
          <div className="iot-strip-item">
            <p>Fleet Availability</p>
            <strong>{onlineRate.toFixed(0)}%</strong>
            <span>
              {iotOverview.fleet.online}/{iotOverview.fleet.total || 0} devices online
            </span>
          </div>
          <div className="iot-strip-item">
            <p>Primary Device Health</p>
            <strong>{primaryDevice ? `${Number(primaryDevice.health_score).toFixed(1)}/100` : "--"}</strong>
            <span>{primaryDevice ? `${primaryDevice.device_id} · ${primaryDevice.status}` : "No telemetry"}</span>
          </div>
          <div className="iot-strip-item">
            <p>Predicted Runtime</p>
            <strong>{formatHours(primaryDevice?.predicted_hours_to_empty)}</strong>
            <span>Estimated time to empty (generator)</span>
          </div>
        </section>

        <section className="content-grid">
          <article className="panel chart-panel">
            <div className="panel-header">
              <h2>Fuel Level Over Time</h2>
            </div>
            <div className="chart-wrap">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart
                  data={chartData}
                  margin={{ top: 10, right: 16, left: 0, bottom: 8 }}
                >
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="time" minTickGap={26} />
                  <YAxis domain={[0, 100]} unit="%" />
                  <Tooltip />
                  <Line
                    type="monotone"
                    dataKey="fuel"
                    stroke="#2563eb"
                    strokeWidth={3}
                    dot={false}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </article>

          <article className="panel">
            <div className="panel-header">
              <h2>Events Timeline</h2>
            </div>
            <ul className="event-list">
              {events.length === 0 && <li className="event-item muted">No events available yet.</li>}
              {events.map((event, index) => (
                <li key={`${event.recorded_at}-${index}`} className="event-item">
                  <span className={`event-tag ${event.event_type}`}>{event.event_type}</span>
                  <div>
                    <p className="event-main">
                      {event.tank_name} · {clampPercent(event.fuel_level).toFixed(2)}%
                    </p>
                    <p className="event-sub">{formatTime(event.recorded_at)}</p>
                  </div>
                </li>
              ))}
            </ul>
          </article>

          <article className="panel solenoid-panel">
            <div className="panel-header">
              <h2>Solenoid Command Status</h2>
            </div>
            <div className="solenoid-summary">
              <p className="solenoid-label">Latest Command</p>
              <p className="solenoid-main">
                {latestSolenoidCommand
                  ? `${latestSolenoidCommand.command} · ${latestSolenoidCommand.mode}`
                  : "No command yet"}
              </p>
              <p className={`solenoid-status status-${(latestSolenoidCommand?.status || "checking").toLowerCase()}`}>
                {latestSolenoidCommand ? latestSolenoidCommand.status.toUpperCase() : "WAITING"}
              </p>
              <p className="solenoid-sub">
                {latestSolenoidCommand
                  ? `${latestSolenoidCommand.reason || "-"} · ${formatTime(latestSolenoidCommand.created_at)}`
                  : "Commands appear here in real time"}
              </p>
            </div>

            <ul className="solenoid-list">
              {solenoidCommands.slice(0, 5).map((command) => (
                <li key={command.request_id} className="solenoid-item">
                  <span>{command.command}</span>
                  <span>{command.status}</span>
                  <span>{formatTime(command.created_at)}</span>
                </li>
              ))}
            </ul>
          </article>

          <article className="panel iot-panel">
            <div className="panel-header">
              <h2>IoT Device Reliability</h2>
            </div>

            <div className="iot-fleet-summary">
              <span className="status-online">Online: {iotOverview.fleet.online}</span>
              <span className="status-degraded">Degraded: {iotOverview.fleet.degraded}</span>
              <span className="status-offline">Offline: {iotOverview.fleet.offline}</span>
            </div>

            <ul className="iot-device-list">
              {iotOverview.devices.length === 0 && <li className="event-item muted">No IoT devices registered yet.</li>}
              {iotOverview.devices.map((device) => (
                <li key={device.device_id} className="iot-device-item">
                  <div>
                    <p className="event-main">{device.device_id}</p>
                    <p className="event-sub">{device.tank_name}</p>
                  </div>
                  <div>
                    <p className={`solenoid-status status-${String(device.status || "checking").toLowerCase()}`}>
                      {device.status}
                    </p>
                    <p className="event-sub">Last seen: {formatTime(device.last_seen_at)}</p>
                  </div>
                  <div>
                    <p className="event-main">Q {Number(device.quality_score).toFixed(0)} · H {Number(device.health_score).toFixed(0)}</p>
                    <p className="event-sub">
                      {device.battery_voltage != null ? `${Number(device.battery_voltage).toFixed(2)}V` : "-"} · RSSI {device.signal_rssi ?? "-"}
                    </p>
                  </div>
                </li>
              ))}
            </ul>
          </article>

          <article className="panel alerts-panel">
            <div className="panel-header">
              <h2>SLA Alert Queue</h2>
            </div>

            <div className="iot-fleet-summary">
              <span className="status-offline">Critical: {alerts.summary.critical}</span>
              <span className="status-degraded">High: {alerts.summary.high}</span>
              <span className="status-checking">Warning: {alerts.summary.warning}</span>
            </div>

            <div className="operator-controls">
              <input className="operator-input" value={authUser.username} disabled readOnly />
              <input className="operator-input" value={authUser.role} disabled readOnly />
            </div>

            <ul className="event-list">
              {alerts.alerts.length === 0 && <li className="event-item muted">No open SLA alerts.</li>}
              {alerts.alerts.map((alert) => (
                <li key={alert.alert_key} className="event-item">
                  <span className={`event-tag alert-${alert.severity}`}>{alert.severity}</span>
                  <div>
                    <p className="event-main">{alert.title}</p>
                    <p className="event-sub">
                      {alert.message} · state: {alert.status}
                    </p>
                    <div className="alert-actions">
                      <button
                        type="button"
                        className="action-btn"
                        disabled={alertsBusy || !canControl}
                        onClick={() => runAlertAction(alert.alert_key, "acknowledge")}
                      >
                        Ack
                      </button>
                      <button
                        type="button"
                        className="action-btn"
                        disabled={alertsBusy || !canControl}
                        onClick={() => runAlertAction(alert.alert_key, "silence", 30)}
                      >
                        Silence 30m
                      </button>
                      <button
                        type="button"
                        className="action-btn action-btn-danger"
                        disabled={alertsBusy || !canControl || authUser.role !== "admin"}
                        onClick={() => runAlertAction(alert.alert_key, "resolve")}
                      >
                        Resolve
                      </button>
                    </div>
                  </div>
                </li>
              ))}
            </ul>
          </article>

          <article className="panel alerts-panel">
            <div className="panel-header">
              <h2>Refill Approval Queue</h2>
            </div>

            <div className="iot-fleet-summary">
              <span className="status-degraded">Pending: {refillRequests.summary.pending}</span>
              <span className="status-online">Executed: {refillRequests.summary.executed}</span>
              <span className="status-offline">Rejected: {refillRequests.summary.rejected}</span>
            </div>

            <ul className="event-list">
              {refillRequests.requests.length === 0 && <li className="event-item muted">No refill requests.</li>}
              {refillRequests.requests.map((requestItem) => (
                <li key={requestItem.id} className="event-item">
                  <span className="event-tag refill">refill</span>
                  <div>
                    <p className="event-main">
                      #{requestItem.id} · {requestItem.status} · transfer {Number(requestItem.estimated_transfer_amount).toFixed(2)}%
                    </p>
                    <p className="event-sub">
                      {requestItem.reason} · requested by {requestItem.requested_by} · {formatTime(requestItem.created_at)}
                    </p>
                    <div className="alert-actions">
                      <button
                        type="button"
                        className="action-btn"
                        disabled={refillBusy || !canControl || requestItem.status !== "pending"}
                        onClick={() => runRefillAction(requestItem.id, "approve")}
                      >
                        Approve
                      </button>
                      <button
                        type="button"
                        className="action-btn action-btn-danger"
                        disabled={refillBusy || !canControl || requestItem.status !== "pending"}
                        onClick={() => runRefillAction(requestItem.id, "reject")}
                      >
                        Reject
                      </button>
                    </div>
                  </div>
                </li>
              ))}
            </ul>
          </article>
        </section>
      </main>
    </div>
  );
}
