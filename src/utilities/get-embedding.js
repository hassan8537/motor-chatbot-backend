const openaiClient = require("../config/openai");

async function getEmbedding(text) {
  const response = await openaiClient.embeddings.create({
    model: "text-embedding-3-small", // or "text-embedding-3-large"
    input: text,
    dimensions: 1536, // Optional: matches older ada-002 size if needed
  });

  return response.data[0].embedding;
}

module.exports = getEmbedding;
