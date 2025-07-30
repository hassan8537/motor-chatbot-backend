const openaiClient = require("../config/openai");

async function getEmbedding(
  inputText,
  { model = "text-embedding-3-small", maxLength = 8191, retries = 2 } = {}
) {
  const texts = Array.isArray(inputText) ? inputText : [inputText];
  const inputs = texts.map(t => truncate(clean(t), maxLength));

  for (let attempt = 1; attempt <= retries + 1; attempt++) {
    try {
      const res = await openaiClient.embeddings.create({
        model,
        input: inputs,
      });
      const embeddings = res.data.map(e => e.embedding);
      validate(embeddings, inputs.length);
      return Array.isArray(inputText) ? embeddings : embeddings[0];
    } catch (err) {
      if (attempt === retries + 1)
        throw new Error(`Embedding failed: ${err.message}`);
      await delay(Math.min(1000 * 2 ** (attempt - 1), 10000));
    }
  }
}

function clean(t) {
  return typeof t === "string" ? t.trim().replace(/\s+/g, " ") : "";
}

function truncate(t, max) {
  if (t.length <= max) return t;
  const cuts = [". ", "! ", "? ", "\n\n", "\n", ": ", "; ", " - ", " | ", " "];
  for (const c of cuts) {
    const i = t.lastIndexOf(c, max);
    if (i > max * 0.7) return t.slice(0, i + c.length).trim();
  }
  return t.slice(0, max - 3).trim() + "...";
}

function validate(arr, expected) {
  if (!Array.isArray(arr) || arr.length !== expected)
    throw new Error("Embedding count mismatch");
  for (const emb of arr)
    if (
      !Array.isArray(emb) ||
      emb.length === 0 ||
      emb.some(n => !Number.isFinite(n))
    )
      throw new Error("Invalid embedding detected");
}

function delay(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function getBatchEmbeddings(textArray, opts = {}) {
  const size = opts.batchSize || 100;
  const out = [];
  for (let i = 0; i < textArray.length; i += size) {
    const chunk = textArray.slice(i, i + size);
    out.push(...(await getEmbedding(chunk, opts)));
    if (i + size < textArray.length) await delay(100);
  }
  return out;
}

module.exports = getEmbedding;
module.exports.getBatchEmbeddings = getBatchEmbeddings;
module.exports.getEmbeddingWithCache = getEmbedding;
