const qdrantClient = require("../config/qdrant");

async function upsertEmbedding({ collectionName, id, vector, payload }) {
  try {
    await qdrantClient.upsert(collectionName, {
      points: [{ id, vector, payload }]
    });
  } catch (err) {
    console.error(err);
    throw err;
  }
}

module.exports = upsertEmbedding;
