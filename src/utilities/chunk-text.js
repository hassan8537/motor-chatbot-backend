const { RecursiveCharacterTextSplitter } = require("langchain/text_splitter");

/**
 * Simple text chunking utility
 * @param {string} text - Input text
 * @param {Object} options - Optional configuration
 * @returns {Promise<string[]>} - Array of chunks
 */
async function chunkText(text, options = {}) {
  console.log({ text });

  const chunkSize = options.chunkSize || 1350;
  const chunkOverlap = options.chunkOverlap || 250;

  if (!text || typeof text !== "string") {
    throw new Error("Text must be a non-empty string.");
  }

  const splitter = new RecursiveCharacterTextSplitter({
    chunkSize,
    chunkOverlap,
    separators: ["\n\n", "\n", ". ", " ", ""],
  });

  try {
    const docs = await splitter.createDocuments([text.trim()]);
    return docs.map(doc => doc.pageContent.trim()).filter(Boolean);
  } catch (err) {
    console.warn("Chunking failed. Returning fallback chunk.", err.message);
    return [text.trim()];
  }
}

module.exports = chunkText;
