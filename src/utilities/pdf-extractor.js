/**
 * Simple PDF Buffer to Text Converter
 * Converts PDF buffer data to plain text using pdf-parse
 */

const pdfParse = require("pdf-parse");

/**
 * Convert PDF buffer to text
 * @param {Buffer} pdfBuffer - PDF file as buffer
 * @returns {Promise<string>} - Extracted text content
 */
async function pdfBufferToText(pdfBuffer) {
  try {
    const data = await pdfParse(pdfBuffer);
    console.log(
      `Extracted ${data.numpages} pages, ${data.numrender} rendered pages, ${data.text}`
    );

    return data.text;
  } catch (error) {
    throw new Error(`PDF conversion failed: ${error.message}`);
  }
}

// Export the function
module.exports = { pdfBufferToText };

// Usage example:
/*
  // Install: npm install pdf-parse

  const fs = require('fs');
  const { pdfBufferToText } = require('./pdf-converter');

  const pdfBuffer = fs.readFileSync('motor-report.pdf');
  pdfBufferToText(pdfBuffer)
    .then(text => {
      console.log('Extracted text:', text);
    })
    .catch(error => {
      console.error('Error:', error);
    });

  // Or with async/await
  async function convertPDF() {
    try {
      const pdfBuffer = fs.readFileSync('motor-report.pdf');
      const text = await pdfBufferToText(pdfBuffer);
      return text;
    } catch (error) {
      console.error('Conversion error:', error);
    }
  }
  */
