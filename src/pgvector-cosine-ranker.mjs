/**
 * Hybrid pgvector Cosine Similarity & Keyword Score Fusion Ranker
 * Computes exact cosine similarity (`1 - (a <=> b)`) over 8-dimensional runbook embeddings.
 */

export function cosineSimilarity(vecA, vecB) {
  if (!Array.isArray(vecA) || !Array.isArray(vecB) || vecA.length !== vecB.length || vecA.length === 0) {
    throw new Error("Vectors must be non-empty arrays of identical dimension");
  }
  let dot = 0;
  let normA = 0;
  let normB = 0;
  for (let i = 0; i < vecA.length; i++) {
    dot += vecA[i] * vecB[i];
    normA += vecA[i] * vecA[i];
    normB += vecB[i] * vecB[i];
  }
  const denom = Math.sqrt(normA) * Math.sqrt(normB);
  return denom === 0 ? 0 : +(dot / denom).toFixed(6);
}

export function rankRunbooksByVector(queryEmbedding, runbooks = []) {
  return [...runbooks]
    .map((item) => ({
      ...item,
      similarity: cosineSimilarity(queryEmbedding, item.embedding)
    }))
    .sort((a, b) => b.similarity - a.similarity);
}
