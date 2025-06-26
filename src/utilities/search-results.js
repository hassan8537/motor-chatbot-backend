const qdrantClient = require("../config/qdrant");

async function searchResults({ collectionName, embedding, limit = 5 }) {
  const results = await qdrantClient.search(collectionName, {
    vector: embedding,
    limit,
    with_payload: true
  });

  return results;
}

module.exports = searchResults;
