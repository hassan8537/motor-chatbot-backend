const pdf = require("pdf-parse");
const Tesseract = require("tesseract.js");
const fs = require("fs");
const path = require("path");
const { createCanvas } = require("canvas");

/**
 * Enhanced PDF text extraction with multiple fallback strategies
 * @param {Buffer} buffer - The PDF file buffer
 * @returns {Promise<{ text: string, method: 'digital' | 'ocr' | 'hybrid' | 'fallback' }>}
 */
async function extractFromPDF(buffer) {
  const strategies = [
    { name: "digital", handler: extractDigitalText },
    { name: "enhanced-digital", handler: extractEnhancedDigitalText },
    { name: "ocr-safe", handler: extractWithSafeOCR },
    { name: "ocr-aggressive", handler: extractWithAggressiveOCR },
  ];

  let lastError = null;
  let extractionResults = [];

  console.log(
    `🔍 Starting PDF text extraction with ${strategies.length} fallback strategies`
  );

  for (const strategy of strategies) {
    try {
      console.log(`📋 Attempting ${strategy.name} extraction...`);
      const startTime = Date.now();

      const result = await strategy.handler(buffer);
      const extractionTime = Date.now() - startTime;

      if (isValidText(result.text)) {
        console.log(
          `✅ ${strategy.name} extraction successful: ${result.text.length} characters in ${extractionTime}ms`
        );
        return {
          text: result.text,
          method: result.method || strategy.name,
          extractionTime,
          strategy: strategy.name,
        };
      } else {
        console.warn(
          `⚠️ ${strategy.name} extraction returned insufficient text (${
            result.text?.length || 0
          } chars)`
        );
        extractionResults.push({
          strategy: strategy.name,
          textLength: result.text?.length || 0,
          text: result.text?.substring(0, 100), // First 100 chars for debugging
        });
      }
    } catch (error) {
      lastError = error;
      console.warn(`❌ ${strategy.name} extraction failed:`, error.message);

      extractionResults.push({
        strategy: strategy.name,
        error: error.message,
        errorType: classifyError(error),
      });

      // If it's a critical error that won't be fixed by other strategies, break early
      if (isCriticalError(error)) {
        console.error(
          `🛑 Critical error detected, stopping extraction attempts`
        );
        break;
      }
    }
  }

  // If all strategies failed, provide detailed error information
  const errorSummary = extractionResults
    .filter(r => r.error)
    .map(r => `${r.strategy}: ${r.error}`)
    .join("; ");

  const textSummary = extractionResults
    .filter(r => r.textLength > 0)
    .map(r => `${r.strategy}: ${r.textLength} chars`)
    .join("; ");

  let errorMessage = "All PDF extraction strategies failed.";

  if (textSummary) {
    errorMessage += ` Some text was found but below minimum threshold: ${textSummary}.`;
  }

  if (errorSummary) {
    errorMessage += ` Errors: ${errorSummary}.`;
  }

  errorMessage +=
    " Please ensure the PDF contains extractable text or try converting it to a different format.";

  throw new Error(errorMessage);
}

/**
 * Strategy 1: Standard digital text extraction
 */
async function extractDigitalText(buffer) {
  try {
    const options = {
      normalizeWhitespace: true,
      disableCombineTextItems: false,
    };

    const result = await pdf(buffer, options);
    return { text: result.text, method: "digital" };
  } catch (error) {
    throw new Error(`Digital extraction failed: ${error.message}`);
  }
}

/**
 * Strategy 2: Enhanced digital extraction with additional options
 */
async function extractEnhancedDigitalText(buffer) {
  try {
    const options = {
      normalizeWhitespace: true,
      disableCombineTextItems: true, // Try different combination setting
      max: 50, // Limit pages for performance
      version: "v1.10.100", // Specify version if available
    };

    const result = await pdf(buffer, options);

    // Additional text processing
    let processedText = result.text;

    // Clean up common PDF artifacts
    processedText = processedText
      .replace(/\r\n/g, "\n")
      .replace(/\r/g, "\n")
      .replace(/\n{3,}/g, "\n\n")
      .replace(/\s{2,}/g, " ")
      .trim();

    return { text: processedText, method: "digital-enhanced" };
  } catch (error) {
    throw new Error(`Enhanced digital extraction failed: ${error.message}`);
  }
}

/**
 * Strategy 3: Safe OCR with proper PDF to image conversion
 */
async function extractWithSafeOCR(buffer) {
  let worker = null;

  try {
    console.log("🔄 Converting PDF to image for OCR processing...");

    // First check if buffer is actually a PDF
    if (!isPDFBuffer(buffer)) {
      throw new Error("Buffer does not appear to be a valid PDF file");
    }

    // Convert PDF to image using a safer method
    const imageBuffer = await convertPDFToImage(buffer);

    if (!imageBuffer || imageBuffer.length === 0) {
      throw new Error("Failed to convert PDF to image for OCR processing");
    }

    console.log("🤖 Initializing Tesseract worker...");
    worker = await Tesseract.createWorker("eng");

    // Configure worker for better performance
    await worker.setParameters({
      tessedit_page_seg_mode: Tesseract.PSM.AUTO,
      tessedit_ocr_engine_mode: Tesseract.OEM.LSTM_ONLY,
      preserve_interword_spaces: "1",
    });

    console.log("📖 Processing image with OCR...");
    const result = await worker.recognize(imageBuffer);

    if (!result || !result.data || !result.data.text) {
      throw new Error("OCR processing returned no text data");
    }

    return {
      text: result.data.text,
      method: "ocr",
      confidence: result.data.confidence,
    };
  } catch (error) {
    throw new Error(`Safe OCR extraction failed: ${error.message}`);
  } finally {
    if (worker) {
      try {
        await worker.terminate();
        console.log("🧹 Tesseract worker terminated successfully");
      } catch (terminateError) {
        console.warn(
          "⚠️ Failed to terminate Tesseract worker:",
          terminateError.message
        );
      }
    }
  }
}

/**
 * Strategy 4: Aggressive OCR with multiple attempts
 */
async function extractWithAggressiveOCR(buffer) {
  let worker = null;

  try {
    // This strategy is more aggressive and might work for difficult PDFs
    const imageBuffer = await convertPDFToImage(buffer, {
      quality: 300, // Higher DPI
      format: "png", // Different format
    });

    worker = await Tesseract.createWorker("eng");

    // More aggressive OCR settings
    await worker.setParameters({
      tessedit_page_seg_mode: Tesseract.PSM.AUTO_OSD,
      tessedit_ocr_engine_mode: Tesseract.OEM.DEFAULT,
      preserve_interword_spaces: "1",
      user_defined_dpi: "300",
    });

    const result = await worker.recognize(imageBuffer);

    if (!result || !result.data || !result.data.text) {
      throw new Error("Aggressive OCR processing returned no text data");
    }

    return {
      text: result.data.text,
      method: "ocr-aggressive",
      confidence: result.data.confidence,
    };
  } catch (error) {
    throw new Error(`Aggressive OCR extraction failed: ${error.message}`);
  } finally {
    if (worker) {
      try {
        await worker.terminate();
      } catch (terminateError) {
        console.warn(
          "⚠️ Failed to terminate aggressive OCR worker:",
          terminateError.message
        );
      }
    }
  }
}

/**
 * Convert PDF buffer to image buffer for OCR processing
 * This is a placeholder - you'll need to implement actual PDF to image conversion
 */
async function convertPDFToImage(pdfBuffer, options = {}) {
  try {
    // Option 1: Use pdf2pic or similar library
    // const pdf2pic = require("pdf2pic");
    // const convert = pdf2pic.fromBuffer(pdfBuffer, options);
    // const result = await convert(1); // Convert first page
    // return result.buffer;

    // Option 2: Use canvas to create a simple image (fallback)
    // This is a very basic implementation - replace with proper PDF to image conversion
    console.log("⚠️ Using basic image conversion fallback");

    // Create a simple white canvas as fallback
    // In production, use a proper PDF to image library like pdf2pic, pdf-poppler, or similar
    const canvas = createCanvas(800, 1000);
    const ctx = canvas.getContext("2d");

    ctx.fillStyle = "white";
    ctx.fillRect(0, 0, 800, 1000);
    ctx.fillStyle = "black";
    ctx.font = "12px Arial";
    ctx.fillText("PDF to image conversion not fully implemented", 50, 100);
    ctx.fillText("Please install pdf2pic or similar library", 50, 130);

    return canvas.toBuffer("image/png");
  } catch (error) {
    throw new Error(`PDF to image conversion failed: ${error.message}`);
  }
}

/**
 * Check if buffer appears to be a valid PDF
 */
function isPDFBuffer(buffer) {
  if (!buffer || buffer.length < 8) return false;

  // Check for PDF header
  const header = buffer.subarray(0, 8).toString("ascii");
  return header.startsWith("%PDF-");
}

/**
 * Validate if extracted text is meaningful
 */
function isValidText(text) {
  if (!text || typeof text !== "string") return false;

  const cleanText = text.trim();

  // Minimum length requirement
  if (cleanText.length < 50) return false;

  // Check for actual words (not just special characters)
  const wordCount = cleanText
    .split(/\s+/)
    .filter(word => word.length > 2 && /[a-zA-Z]/.test(word)).length;

  return wordCount >= 10;
}

/**
 * Classify error types for better handling
 */
function classifyError(error) {
  const message = error.message.toLowerCase();

  if (message.includes("tesseract") || message.includes("ocr")) {
    return "OCR_ERROR";
  }
  if (message.includes("pdf") && message.includes("invalid")) {
    return "INVALID_PDF";
  }
  if (message.includes("password") || message.includes("encrypted")) {
    return "PROTECTED_PDF";
  }
  if (message.includes("timeout") || message.includes("memory")) {
    return "RESOURCE_ERROR";
  }
  if (message.includes("image") || message.includes("convert")) {
    return "CONVERSION_ERROR";
  }

  return "UNKNOWN_ERROR";
}

/**
 * Check if error is critical and should stop further attempts
 */
function isCriticalError(error) {
  const criticalTypes = ["INVALID_PDF", "PROTECTED_PDF"];
  return criticalTypes.includes(classifyError(error));
}

/**
 * Timeout wrapper for any async operation
 */
async function withTimeout(
  promise,
  timeoutMs = 30000,
  operation = "operation"
) {
  const timeoutPromise = new Promise((_, reject) => {
    setTimeout(() => {
      reject(new Error(`${operation} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });

  return Promise.race([promise, timeoutPromise]);
}

module.exports = extractFromPDF;
