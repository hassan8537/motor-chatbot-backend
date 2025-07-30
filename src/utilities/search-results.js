// Simplified Qdrant vector search with optional reranking and filtering
const qdrantClient = require("../config/qdrant");

async function searchResults({
  collectionName,
  embedding,
  limit = 15,
  scoreThreshold = 0.3,
  filter = null,
  enableReranking = true,
  queryType = "general",
  retries = 2,
}) {
  if (!collectionName || typeof collectionName !== "string")
    throw new Error("Collection name must be a non-empty string");
  if (!Array.isArray(embedding) || embedding.length === 0)
    throw new Error("Embedding must be a non-empty array");

  for (let attempt = 1; attempt <= retries + 1; attempt++) {
    try {
      const options = {
        vector: embedding,
        limit,
        with_payload: true,
        score_threshold: scoreThreshold,
        ...(filter && { filter }),
      };

      const results = await qdrantClient.search(collectionName, options);
      if (!results?.length) {
        if (scoreThreshold > 0.1) {
          return searchResults({
            collectionName,
            embedding,
            limit,
            scoreThreshold: 0.1,
            filter,
            enableReranking,
            queryType,
            retries: 0,
          });
        }
        return [];
      }

      let processed = results.map((r, i) => ({
        id: r.id,
        score: r.score,
        payload: r.payload || {},
        rank: i + 1,
        relevance: getRelevance(r.score),
      }));

      return enableReranking ? rerank(processed, queryType) : processed;
    } catch (err) {
      if (attempt === retries + 1)
        throw new Error(`Search failed: ${err.message}`);
      await new Promise(r =>
        setTimeout(r, Math.min(1000 * 2 ** (attempt - 1), 5000))
      );
    }
  }
}

function getRelevance(score) {
  if (score >= 0.8) return "HIGH";
  if (score >= 0.6) return "MEDIUM";
  if (score >= 0.4) return "LOW";
  return "MINIMAL";
}

function rerank(results, type) {
  return results
    .map(r => {
      let boost = 0;
      const c = r.payload.content || "";
      if (/STRUCTURED DATA|METRICS:/.test(c)) boost += 0.15;
      if ((c.match(/\d+(\.\d+)?/g) || []).length > 3) boost += 0.1;
      if (/(\n.*\t.*\n|\n\s*\d+\.\s|\n\s*[-•*]\s|:\s*\d+)/.test(c))
        boost += 0.08;
      if (c.length > 200 && c.length < 2000) boost += 0.05;
      if (
        type === "comparison" &&
        /\b(more|less|higher|lower|better|worse|vs|versus|compared)\b/i.test(c)
      )
        boost += 0.1;
      if (
        type === "specific" &&
        /\b(exactly|precisely|specifically|defined as)\b/i.test(c)
      )
        boost += 0.08;
      return {
        ...r,
        originalScore: r.score,
        score: Math.min(r.score + boost, 1.0),
        boost,
      };
    })
    .sort((a, b) => b.score - a.score);
}

module.exports = searchResults;
