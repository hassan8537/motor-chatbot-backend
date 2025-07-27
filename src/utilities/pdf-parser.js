const pdf = require("pdf-parse");
const Tesseract = require("tesseract.js");

/**
 * Extracts text from a PDF (tries digital first, falls back to OCR if needed)
 * @param {Buffer} buffer - The PDF file buffer
 * @returns {Promise<{ text: string, method: 'digital' | 'ocr' }>}
 */
async function extractFromPDF(buffer) {
  try {
    // Try extracting digital text
    const { text } = await pdf(buffer);
    if (text && text.trim().length > 20) {
      return { text, method: "digital" };
    }

    // Fallback to OCR for scanned/image PDF
    const result = await Tesseract.recognize(buffer, "eng");
    return { text: result.data.text, method: "ocr" };
  } catch (err) {
    console.error("Failed to extract PDF:", err.message);
    throw err;
  }
}

module.exports = extractFromPDF;
