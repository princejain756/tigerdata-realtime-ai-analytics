/**
 * TimescaleDB Hypertable Compression & Retention Policy Generator
 * Builds SQL DDL statements for chunk compression, columnstore segmenting, and continuous aggregate refresh policies.
 */

export function generateHypertableLifecyclePolicies({
  hypertable = "sensor_telemetry",
  chunkInterval = "1 day",
  compressAfter = "7 days",
  retainFor = "90 days",
  segmentBy = "device_id, region",
  orderBy = "recorded_at DESC"
} = {}) {
  return {
    hypertable,
    chunkInterval,
    compressAfter,
    retainFor,
    ddlStatements: [
      `SELECT create_hypertable('${hypertable}', by_range('recorded_at', INTERVAL '${chunkInterval}'), if_not_exists => TRUE);`,
      `ALTER TABLE ${hypertable} SET (timescaledb.compress, timescaledb.compress_segmentby = '${segmentBy}', timescaledb.compress_orderby = '${orderBy}');`,
      `SELECT add_compression_policy('${hypertable}', INTERVAL '${compressAfter}', if_not_exists => TRUE);`,
      `SELECT add_retention_policy('${hypertable}', INTERVAL '${retainFor}', if_not_exists => TRUE);`
    ]
  };
}
