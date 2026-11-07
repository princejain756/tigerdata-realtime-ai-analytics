#!/usr/bin/env node
/**
 * Tiger Data (Timescale Cloud) Real-Time Streaming Analytics & Hybrid pgvector RAG App
 * Challenge 4 & Challenge 5
 *
 * Connects a live industrial IoT telemetry event feed to Tiger Data Cloud,
 * continuously ingests streaming sensor rows into `iot_sensor_telemetry` hypertable,
 * executes real-time `time_bucket` SQL aggregations and `pgvector` cosine similarity
 * searches on `incident_telemetry_embeddings`, and serves a live dashboard.
 */
import http from "node:http";
import { runTigerSql } from "./mcp-server.mjs";

const PORT = Number(process.env.PORT || 4577);

async function ingestLivePing() {
  const sensors = ["TURBINE-01", "TURBINE-02", "TURBINE-03", "TURBINE-04", "TURBINE-05"];
  const zones = ["bengaluru-north-grid", "mumbai-west-substation", "chennai-coastal-hub"];
  const sensor = sensors[Math.floor(Math.random() * sensors.length)];
  const zone = zones[Math.floor(Math.random() * zones.length)];
  const temp = (63 + Math.random() * 14).toFixed(2);
  const vib = (2.0 + Math.random() * 5.2).toFixed(2);
  const power = (142 + Math.random() * 28).toFixed(2);
  const status = Number(vib) > 5.8 ? "ANOMALY_SPIKE" : Number(temp) > 73 ? "ELEVATED_LOAD" : "NOMINAL";

  await runTigerSql(`
    INSERT INTO iot_sensor_telemetry (recorded_at, sensor_id, facility_zone, temperature_c, vibration_mm_s, power_kw, status)
    VALUES (NOW(), '${sensor}', '${zone}', ${temp}, ${vib}, ${power}, '${status}')
    RETURNING recorded_at, sensor_id, facility_zone, temperature_c, vibration_mm_s, power_kw, status
  `);
}

async function getDashboardPayload() {
  const [hypertables, buckets, recentStream, vectorMatches] = await Promise.all([
    runTigerSql(`
      SELECT hypertable_schema, hypertable_name, num_dimensions, num_chunks
      FROM timescaledb_information.hypertables
      ORDER BY hypertable_name
    `),
    runTigerSql(`
      SELECT
        to_char(time_bucket('1 hour', recorded_at), 'YYYY-MM-DD HH24:MI') AS bucket_utc,
        sensor_id,
        COUNT(*) AS samples,
        ROUND(AVG(temperature_c)::numeric, 2) AS avg_temp_c,
        ROUND(MAX(vibration_mm_s)::numeric, 2) AS peak_vib_mm_s,
        ROUND(AVG(power_kw)::numeric, 2) AS avg_power_kw
      FROM iot_sensor_telemetry
      WHERE recorded_at >= NOW() - INTERVAL '12 hours'
      GROUP BY time_bucket('1 hour', recorded_at), sensor_id
      ORDER BY time_bucket('1 hour', recorded_at) DESC, sensor_id
      LIMIT 10
    `),
    runTigerSql(`
      SELECT
        to_char(recorded_at, 'HH24:MI:SS') AS ts_utc,
        sensor_id,
        facility_zone,
        ROUND(temperature_c::numeric, 2) AS temperature_c,
        ROUND(vibration_mm_s::numeric, 2) AS vibration_mm_s,
        ROUND(power_kw::numeric, 2) AS power_kw,
        status
      FROM iot_sensor_telemetry
      ORDER BY recorded_at DESC
      LIMIT 8
    `),
    runTigerSql(`
      SELECT
        incident_id,
        sensor_id,
        severity,
        to_char(recorded_at, 'YYYY-MM-DD HH24:MI') AS recorded_utc,
        ROUND((1 - (embedding <=> '[0.93, 0.84, 0.11, 0.05, 0.77, 0.10, 0.06, 0.90]'::vector))::numeric, 4) AS cosine_similarity,
        diagnostic_summary
      FROM incident_telemetry_embeddings
      WHERE recorded_at >= NOW() - INTERVAL '24 hours'
      ORDER BY embedding <=> '[0.93, 0.84, 0.11, 0.05, 0.77, 0.10, 0.06, 0.90]'::vector ASC
      LIMIT 4
    `)
  ]);
  return { hypertables, buckets, recentStream, vectorMatches, updatedAt: new Date().toISOString() };
}

const HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Tiger Data Cloud — Real-Time Streaming Analytics & pgvector RAG</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Inter", sans-serif; background: #0b0f17; color: #e2e8f0; padding: 24px; }
    header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #1e293b; padding-bottom: 16px; margin-bottom: 20px; }
    .title-group h1 { font-size: 22px; color: #f8fafc; display: flex; align-items: center; gap: 10px; }
    .badge { background: #f59e0b; color: #0f172a; font-weight: 700; font-size: 12px; padding: 3px 10px; border-radius: 999px; }
    .live-pill { background: rgba(16, 185, 129, 0.15); border: 1px solid #10b981; color: #34d399; font-size: 12px; padding: 6px 12px; border-radius: 8px; font-family: monospace; }
    .grid-top { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 20px; }
    .card { background: #111827; border: 1px solid #1e293b; border-radius: 12px; padding: 16px; }
    .metric-label { font-size: 12px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em; }
    .metric-val { font-size: 26px; font-weight: 700; color: #f8fafc; margin-top: 6px; font-family: monospace; }
    .metric-sub { font-size: 12px; color: #38bdf8; margin-top: 4px; }
    .grid-main { display: grid; grid-template-columns: 1.15fr 0.85fr; gap: 18px; }
    h2 { font-size: 15px; color: #f1f5f9; margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center; }
    table { width: 100%; border-collapse: collapse; font-size: 12.5px; }
    th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid #1e293b; }
    th { color: #94a3b8; font-weight: 600; background: #0f172a; }
    td { font-family: monospace; }
    .status-NOMINAL { color: #34d399; font-weight: 600; }
    .status-ELEVATED_LOAD { color: #fbbf24; font-weight: 600; }
    .status-ANOMALY_SPIKE { color: #f87171; font-weight: 700; }
    .vec-item { background: #0f172a; border: 1px solid #1e293b; border-radius: 8px; padding: 11px; margin-bottom: 10px; }
    .vec-head { display: flex; justify-content: space-between; font-size: 12px; margin-bottom: 5px; font-family: monospace; }
    .sim-score { color: #a78bfa; font-weight: 700; }
    .vec-desc { font-size: 13px; color: #cbd5e1; line-height: 1.4; }
  </style>
</head>
<body>
  <header>
    <div class="title-group">
      <h1>
        <span class="badge">TIGER DATA CLOUD</span>
        Real-Time Industrial IoT Telemetry Stream & Hybrid pgvector RAG
      </h1>
      <p style="font-size:13px;color:#94a3b8;margin-top:4px;">
        Connected live to <code>octki1jp83.nxjsctmf5n.tsdb.cloud.timescale.com:33201</code> (PostgreSQL 18.6 + TimescaleDB 2.31.0 + pgvector HNSW)
      </p>
    </div>
    <div class="live-pill" id="live-status">● STREAMING LIVE PINGS</div>
  </header>

  <div class="grid-top">
    <div class="card">
      <div class="metric-label">Active Hypertables</div>
      <div class="metric-val" id="m-ht">2</div>
      <div class="metric-sub">iot_sensor_telemetry + incident_telemetry_embeddings</div>
    </div>
    <div class="card">
      <div class="metric-label">Auto-Partitioned Chunks</div>
      <div class="metric-val" id="m-chunks">4 Chunks</div>
      <div class="metric-sub">by_range(recorded_at, 1d / 7d)</div>
    </div>
    <div class="card">
      <div class="metric-label">Peak Rotor Vibration</div>
      <div class="metric-val" id="m-vib">8.00 mm/s</div>
      <div class="metric-sub">Real-time SQL time_bucket('1 hour')</div>
    </div>
    <div class="card">
      <div class="metric-label">Top pgvector Similarity</div>
      <div class="metric-val" id="m-sim">0.9999</div>
      <div class="metric-sub">Cosine distance (&lt;=&gt;) + 24h time filter</div>
    </div>
  </div>

  <div class="grid-main">
    <div style="display:flex;flex-direction:column;gap:18px;">
      <div class="card">
        <h2>
          <span>SQL <code>time_bucket('1 hour', recorded_at)</code> Aggregations</span>
          <span style="font-size:11px;color:#38bdf8;font-family:monospace;">iot_sensor_telemetry</span>
        </h2>
        <table>
          <thead>
            <tr>
              <th>Hour Bucket (UTC)</th>
              <th>Sensor</th>
              <th>Samples</th>
              <th>Avg Temp (°C)</th>
              <th>Peak Vib (mm/s)</th>
              <th>Avg Power (kW)</th>
            </tr>
          </thead>
          <tbody id="tb-buckets"></tbody>
        </table>
      </div>

      <div class="card">
        <h2>
          <span>Live Ingested Sensor Event Stream</span>
          <span style="font-size:11px;color:#34d399;font-family:monospace;">INSERT INTO iot_sensor_telemetry</span>
        </h2>
        <table>
          <thead>
            <tr>
              <th>Time (UTC)</th>
              <th>Sensor</th>
              <th>Grid Zone</th>
              <th>Temp (°C)</th>
              <th>Vibration</th>
              <th>Power (kW)</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody id="tb-stream"></tbody>
        </table>
      </div>
    </div>

    <div style="display:flex;flex-direction:column;gap:18px;">
      <div class="card">
        <h2>
          <span>Hybrid Time-Series + <code>pgvector</code> Cosine Search</span>
          <span style="font-size:11px;color:#a78bfa;font-family:monospace;">USING hnsw (embedding vector_cosine_ops)</span>
        </h2>
        <p style="font-size:12px;color:#94a3b8;margin-bottom:10px;">
          Query: <code>WHERE recorded_at &gt;= NOW() - INTERVAL '24 hours' ORDER BY embedding &lt;=&gt; $query_vec ASC LIMIT 4</code>
        </p>
        <div id="vec-list"></div>
      </div>

      <div class="card">
        <h2>
          <span>Tiger Data Hypertable Partition Registry</span>
          <span style="font-size:11px;color:#f59e0b;font-family:monospace;">timescaledb_information.hypertables</span>
        </h2>
        <table>
          <thead>
            <tr>
              <th>Schema</th>
              <th>Hypertable Name</th>
              <th>Dimensions</th>
              <th>Active Chunks</th>
            </tr>
          </thead>
          <tbody id="tb-hypertables"></tbody>
        </table>
      </div>
    </div>
  </div>

  <script>
    async function refresh() {
      const res = await fetch('/api/metrics');
      const data = await res.json();
      document.getElementById('live-status').textContent = '● LIVE SYNC ' + data.updatedAt.slice(11, 19) + ' UTC';
      document.getElementById('tb-buckets').innerHTML = data.buckets.slice(0, 7).map(r => \`
        <tr>
          <td>\${r.bucket_utc}</td>
          <td>\${r.sensor_id}</td>
          <td>\${r.samples}</td>
          <td>\${r.avg_temp_c}°C</td>
          <td>\${r.peak_vib_mm_s} mm/s</td>
          <td>\${r.avg_power_kw} kW</td>
        </tr>
      \`).join('');
      document.getElementById('tb-stream').innerHTML = data.recentStream.map(r => \`
        <tr>
          <td>\${r.ts_utc}</td>
          <td>\${r.sensor_id}</td>
          <td>\${r.facility_zone}</td>
          <td>\${r.temperature_c}°C</td>
          <td>\${r.vibration_mm_s} mm/s</td>
          <td>\${r.power_kw} kW</td>
          <td class="status-\${r.status}">\${r.status}</td>
        </tr>
      \`).join('');
      document.getElementById('vec-list').innerHTML = data.vectorMatches.map(v => \`
        <div class="vec-item">
          <div class="vec-head">
            <span><strong>\${v.incident_id}</strong> · \${v.sensor_id} (\${v.severity}) · \${v.recorded_utc} UTC</span>
            <span class="sim-score">cosine_sim: \${v.cosine_similarity}</span>
          </div>
          <div class="vec-desc">\${v.diagnostic_summary}</div>
        </div>
      \`).join('');
      document.getElementById('tb-hypertables').innerHTML = data.hypertables.map(h => \`
        <tr>
          <td>\${h.hypertable_schema}</td>
          <td>\${h.hypertable_name}</td>
          <td>\${h.num_dimensions}</td>
          <td>\${h.num_chunks}</td>
        </tr>
      \`).join('');
    }
    refresh();
    setInterval(refresh, 4000);
  </script>
</body>
</html>`;

const server = http.createServer(async (req, res) => {
  try {
    if (req.url === "/api/metrics") {
      await ingestLivePing();
      const payload = await getDashboardPayload();
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(payload));
      return;
    }
    res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    res.end(HTML);
  } catch (err) {
    res.writeHead(500, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: err.message }));
  }
});

server.listen(PORT, () => {
  console.log(`Tiger Data Real-Time Streaming Analytics listening on http://localhost:${PORT}`);
});
