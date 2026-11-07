# Tiger Data Cloud: Real-Time Streaming Analytics, MCP Server & Hybrid pgvector RAG

Built for **MLH Global Hack Week: Builders Week** Tiger Data challenges:

1. **Challenge 1 (Launch Your First Tiger Data Hypertable)**: Auto-partitioned time-series hypertable (`iot_sensor_telemetry`) using `create_hypertable('iot_sensor_telemetry', by_range('recorded_at', INTERVAL '1 day'))` and `time_bucket` SQL aggregations.
2. **Challenge 3 (Supercharge AI Agents with Tiger CLI or MCP Server)**: Model Context Protocol (MCP) server (`src/mcp-server.mjs`) exposing `list_hypertables`, `query_time_bucket_metrics`, and `hybrid_vector_search` directly over stdio JSON-RPC 2.0 to AI agents.
3. **Challenge 4 (Build a Real-Time Streaming Analytics App)**: Live streaming telemetry ingestion and analytics dashboard (`src/streaming-dashboard.mjs`) visualizing hourly `time_bucket` rollups, live sensor event streams, and anomaly alerts.
4. **Challenge 5 (Implement AI-Powered Vector Search with pgvector)**: Hybrid time-series + vector similarity search (`incident_telemetry_embeddings`) combining `pgvector` HNSW cosine distance (`<=>`) with time-window filtering inside PostgreSQL.

## Quick Start

```bash
export TIGER_DB_URL="postgres://tsdbadmin:<password>@<service>.tsdb.cloud.timescale.com:33201/tsdb?sslmode=require"
psql "$TIGER_DB_URL" -f sql/01_hypertables_and_pgvector.sql

# Verify MCP tools
npm run demo

# Launch real-time streaming dashboard on http://localhost:4577
npm start
```
