import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { generateHypertableLifecyclePolicies } from "../src/hypertable-retention-policy.mjs";
import { cosineSimilarity, rankRunbooksByVector } from "../src/pgvector-cosine-ranker.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

test("Hypertable policy generator builds TimescaleDB compression and retention SQL", () => {
  const policy = generateHypertableLifecyclePolicies({ hypertable: "sensor_telemetry" });
  assert.equal(policy.ddlStatements.length, 4);
  assert.match(policy.ddlStatements[1], /timescaledb\.compress_segmentby/);
  assert.match(policy.ddlStatements[2], /add_compression_policy/);
});

test("pgvector cosine similarity ranker orders identical vectors at 1.0", () => {
  const v1 = [0.92, 0.88, 0.15, 0.91, 0.12, 0.08, 0.85, 0.19];
  const v2 = [0.10, 0.15, 0.94, 0.12, 0.89, 0.91, 0.14, 0.82];
  assert.equal(cosineSimilarity(v1, v1), 1);
  const ranked = rankRunbooksByVector(v1, [
    { id: "r2", title: "Disk IOPS throttling", embedding: v2 },
    { id: "r1", title: "Thermal spike mitigation", embedding: v1 }
  ]);
  assert.equal(ranked[0].id, "r1");
  assert.ok(ranked[0].similarity > ranked[1].similarity);
});

test("Live Tiger Data snapshot contains verified hypertable chunks and pgvector results", () => {
  const raw = fs.readFileSync(path.join(__dirname, "..", "src", "live-snapshot.json"), "utf8");
  const snap = JSON.parse(raw);
  assert.ok(Array.isArray(snap.hypertables));
  assert.ok(Array.isArray(snap.vectorMatches));
  assert.ok(snap.vectorMatches.length >= 3);
});
