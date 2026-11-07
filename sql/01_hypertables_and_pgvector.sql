-- Tiger Data (Timescale Cloud) GHW Builders Week SQL Schema & Queries
-- Challenges 1, 3, 4, and 5

-- 1. Enable vector extensions for hybrid time-series + semantic RAG search
CREATE EXTENSION IF NOT EXISTS vector CASCADE;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vectorscale') THEN
    EXECUTE 'CREATE EXTENSION IF NOT EXISTS vectorscale CASCADE';
  END IF;
END
$$;

-- 2. Challenge 1 & 4: High-velocity IoT Sensor Telemetry Hypertable
CREATE TABLE IF NOT EXISTS iot_sensor_telemetry (
  recorded_at TIMESTAMPTZ NOT NULL,
  sensor_id TEXT NOT NULL,
  facility_zone TEXT NOT NULL,
  temperature_c DOUBLE PRECISION NOT NULL,
  vibration_mm_s DOUBLE PRECISION NOT NULL,
  power_kw DOUBLE PRECISION NOT NULL,
  status TEXT NOT NULL DEFAULT 'NOMINAL'
);

SELECT create_hypertable('iot_sensor_telemetry', by_range('recorded_at', INTERVAL '1 day'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_iot_sensor_time ON iot_sensor_telemetry (sensor_id, recorded_at DESC);

-- 3. Challenge 5: Hybrid Time-Series + pgvector Embeddings Hypertable
CREATE TABLE IF NOT EXISTS incident_telemetry_embeddings (
  recorded_at TIMESTAMPTZ NOT NULL,
  incident_id TEXT NOT NULL,
  sensor_id TEXT NOT NULL,
  severity TEXT NOT NULL,
  diagnostic_summary TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  embedding vector(8) NOT NULL
);

SELECT create_hypertable('incident_telemetry_embeddings', by_range('recorded_at', INTERVAL '7 days'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_incident_embedding_hnsw ON incident_telemetry_embeddings USING hnsw (embedding vector_cosine_ops);

-- 4. Time-Bucketed Streaming Analytics Query
SELECT
  time_bucket('1 hour', recorded_at) AS hour_bucket,
  sensor_id,
  COUNT(*) AS samples,
  ROUND(AVG(temperature_c)::numeric, 2) AS avg_temp_c,
  ROUND(MAX(vibration_mm_s)::numeric, 2) AS peak_vibration_mm_s,
  ROUND(AVG(power_kw)::numeric, 2) AS avg_power_kw
FROM iot_sensor_telemetry
WHERE recorded_at >= NOW() - INTERVAL '24 hours'
GROUP BY hour_bucket, sensor_id
ORDER BY hour_bucket DESC, sensor_id;

-- 5. Hybrid Time-Range + Metadata + Cosine Distance Vector Search
SELECT
  incident_id,
  sensor_id,
  severity,
  recorded_at,
  ROUND((1 - (embedding <=> '[0.93, 0.84, 0.11, 0.05, 0.77, 0.10, 0.06, 0.90]'::vector))::numeric, 4) AS cosine_similarity,
  diagnostic_summary
FROM incident_telemetry_embeddings
WHERE recorded_at >= NOW() - INTERVAL '24 hours'
ORDER BY embedding <=> '[0.93, 0.84, 0.11, 0.05, 0.77, 0.10, 0.06, 0.90]'::vector ASC
LIMIT 5;
