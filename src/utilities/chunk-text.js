const { RecursiveCharacterTextSplitter } = require("langchain/text_splitter");

/**
 * Splits input text into semantically coherent chunks using LangChain.
 * @param {string} text - The full text to split
 * @param {number} chunkSize - Max size of each chunk in characters
 * @param {number} chunkOverlap - Number of overlapping characters
 * @returns {Promise<string[]>}
 */
async function chunkText(text, chunkSize = 1000, chunkOverlap = 200) {
  const splitter = new RecursiveCharacterTextSplitter({
    chunkSize,
    chunkOverlap,
  });

  const docs = await splitter.createDocuments([text]);
  return docs.map((doc) => doc.pageContent);
}

module.exports = chunkText;
