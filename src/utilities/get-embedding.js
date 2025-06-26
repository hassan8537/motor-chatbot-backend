const openaiClient = require("../config/openai");

async function getEmbedding(text) {
  const response = await openaiClient.embeddings.create({
    model: "text-embedding-ada-002",
    input: text
  });

  return response.data[0].embedding;
}

module.exports = getEmbedding;
