# Tiger Data Real-Time AI Analytics Schema & Architecture

## 1. TimescaleDB Hypertable (`sensor_telemetry`)
- Partitioned by `recorded_at` using 1-day range chunks.
- Columnstore compression segmented by `device_id, region` and ordered by `recorded_at DESC`.

## 2. Continuous Aggregate (`hourly_device_rollup`)
- Pre-aggregates `AVG(cpu_temp_c)`, `MAX(cpu_temp_c)`, `AVG(power_watts)`, and `SUM(anomaly_flag)` inside `time_bucket('1 hour', recorded_at)`.

## 3. Hybrid Vector Search (`incident_runbooks`)
- Stores 8-dimensional operational telemetry embeddings in PostgreSQL `vector(8)` (`pgvector`) and ranks remediation playbooks via cosine distance (`<=>`).
