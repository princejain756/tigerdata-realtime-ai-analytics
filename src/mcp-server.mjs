#!/usr/bin/env node
/**
 * Tiger Data (Timescale Cloud) Model Context Protocol (MCP) Server & CLI
 * Challenge 3: Supercharge AI Agents with Tiger CLI or MCP Server
 *
 * Exposes live Tiger Data Cloud hypertables, time_bucket aggregations,
 * real-time telemetry ingestion, and pgvector hybrid similarity search
 * to MCP-compatible AI agents over stdio JSON-RPC 2.0 or CLI flags.
 */
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import readline from "node:readline";

const execFileAsync = promisify(execFile);
const PSQL_BIN = process.env.PSQL_BIN || "/opt/homebrew/bin/psql";
const TIGER_DB_URL = process.env.TIGER_DB_URL || "postgres://tsdbadmin:YOUR_PASSWORD@YOUR_SERVICE.tsdb.cloud.timescale.com:33201/tsdb?sslmode=require";

export async function runTigerSql(sql) {
  const { stdout } = await execFileAsync(PSQL_BIN, [
    TIGER_DB_URL,
    "-X",
    "-A",
    "-t",
    "-c",
    `SELECT COALESCE(json_agg(t), '[]'::json) FROM (${sql.replace(/;\s*$/, "")}) t;`
  ]);
  return JSON.parse(stdout.trim() || "[]");
}

export const MCP_TOOLS = [
  {
    name: "list_hypertables",
    description: "List all partitioned Tiger Data hypertables, dimensions, and chunk counts.",
    inputSchema: { type: "object", properties: {} }
  },
  {
    name: "query_time_bucket_metrics",
    description: "Run time_bucket aggregated telemetry queries across industrial IoT sensors.",
    inputSchema: {
      type: "object",
      properties: {
        bucket_interval: { type: "string", default: "1 hour" },
        hours_back: { type: "number", default: 12 }
      }
    }
  },
  {
    name: "hybrid_vector_search",
    description: "Search incident_telemetry_embeddings hypertable using pgvector cosine distance and time window filtering.",
    inputSchema: {
      type: "object",
      properties: {
        query_vector: {
          type: "array",
          items: { type: "number" },
          description: "8-dimensional telemetry diagnostic embedding vector"
        },
        hours_back: { type: "number", default: 24 },
        limit: { type: "number", default: 4 }
      },
      required: ["query_vector"]
    }
  }
];

export async function handleToolCall(name, args = {}) {
  if (name === "list_hypertables") {
    return await runTigerSql(`
      SELECT hypertable_schema, hypertable_name, num_dimensions, num_chunks
      FROM timescaledb_information.hypertables
      ORDER BY hypertable_name
    `);
  }
  if (name === "query_time_bucket_metrics") {
    const interval = String(args.bucket_interval || "1 hour").replace(/[^a-zA-Z0-9 ]/g, "");
    const hours = Number(args.hours_back || 12);
    return await runTigerSql(`
      SELECT
        time_bucket('${interval}', recorded_at) AS bucket,
        sensor_id,
        COUNT(*) AS samples,
        ROUND(AVG(temperature_c)::numeric, 2) AS avg_temp_c,
        ROUND(MAX(vibration_mm_s)::numeric, 2) AS peak_vibration_mm_s,
        ROUND(AVG(power_kw)::numeric, 2) AS avg_power_kw
      FROM iot_sensor_telemetry
      WHERE recorded_at >= NOW() - INTERVAL '${hours} hours'
      GROUP BY bucket, sensor_id
      ORDER BY bucket DESC, sensor_id
      LIMIT 15
    `);
  }
  if (name === "hybrid_vector_search") {
    const vec = Array.isArray(args.query_vector) && args.query_vector.length === 8
      ? args.query_vector.map(Number)
      : [0.93, 0.84, 0.11, 0.05, 0.77, 0.10, 0.06, 0.90];
    const hours = Number(args.hours_back || 24);
    const limit = Number(args.limit || 4);
    const vecLiteral = `[${vec.join(",")}]`;
    return await runTigerSql(`
      SELECT
        incident_id,
        sensor_id,
        severity,
        recorded_at,
        ROUND((1 - (embedding <=> '${vecLiteral}'::vector))::numeric, 4) AS cosine_similarity,
        diagnostic_summary,
        metadata
      FROM incident_telemetry_embeddings
      WHERE recorded_at >= NOW() - INTERVAL '${hours} hours'
      ORDER BY embedding <=> '${vecLiteral}'::vector ASC
      LIMIT ${limit}
    `);
  }
  throw new Error(`Unknown MCP tool: ${name}`);
}

async function main() {
  if (process.argv.includes("--demo")) {
    console.log("=== Tiger Data MCP Server & CLI Agent Verification ===");
    const hypertables = await handleToolCall("list_hypertables");
    console.log("\n[MCP Tool: list_hypertables] ->", JSON.stringify(hypertables, null, 2));

    const buckets = await handleToolCall("query_time_bucket_metrics", { bucket_interval: "1 hour", hours_back: 6 });
    console.log("\n[MCP Tool: query_time_bucket_metrics] ->", JSON.stringify(buckets.slice(0, 5), null, 2));

    const vectors = await handleToolCall("hybrid_vector_search", {
      query_vector: [0.93, 0.84, 0.11, 0.05, 0.77, 0.10, 0.06, 0.90],
      hours_back: 24,
      limit: 3
    });
    console.log("\n[MCP Tool: hybrid_vector_search] ->", JSON.stringify(vectors, null, 2));
    return;
  }

  const rl = readline.createInterface({ input: process.stdin, terminal: false });
  rl.on("line", async (line) => {
    if (!line.trim()) return;
    try {
      const req = JSON.parse(line);
      if (req.method === "initialize") {
        process.stdout.write(JSON.stringify({
          jsonrpc: "2.0",
          id: req.id,
          result: {
            protocolVersion: "2024-11-05",
            capabilities: { tools: {} },
            serverInfo: { name: "tiger-data-mcp-server", version: "1.0.0" }
          }
        }) + "\n");
      } else if (req.method === "tools/list") {
        process.stdout.write(JSON.stringify({
          jsonrpc: "2.0",
          id: req.id,
          result: { tools: MCP_TOOLS }
        }) + "\n");
      } else if (req.method === "tools/call") {
        const data = await handleToolCall(req.params.name, req.params.arguments);
        process.stdout.write(JSON.stringify({
          jsonrpc: "2.0",
          id: req.id,
          result: { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] }
        }) + "\n");
      }
    } catch (err) {
      process.stdout.write(JSON.stringify({
        jsonrpc: "2.0",
        id: null,
        error: { code: -32000, message: err.message }
      }) + "\n");
    }
  });
}

main();
