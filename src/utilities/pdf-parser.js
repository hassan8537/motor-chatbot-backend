const pdf = require("pdf-parse");
const Tesseract = require("tesseract.js");
const fs = require("fs");
const path = require("path");
const os = require("os");
const pdf2pic = require("pdf2pic");

function getTempDir() {
  return process.env.LAMBDA_FUNCTION_NAME ? "/tmp" : os.tmpdir();
}

function safeUnlink(filePath) {
  try {
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
  } catch (e) {
    console.warn(`⚠️ Failed to delete ${filePath}: ${e.message}`);
  }
}

/**
 * SMART PDF EXTRACTOR - Hybrid strategy for maximum text extraction
 * Works for both digital text PDFs and scanned image PDFs
 * @param {Buffer} buffer - PDF file buffer
 * @param {Object} [options] - Extraction options
 * @returns {Promise<Object>} - Complete extraction with ALL content
 */
async function extractFromPDF(buffer, options = {}) {
  const {
    forceOCR = false,
    ocrFallbackThreshold = 100,
    maxPages = 0, // 0 = all pages
    ocrQuality = "balanced", // "fast", "balanced", "high"
  } = options;

  console.log(`🔍 SMART PDF EXTRACTION - Hybrid Strategy Starting...`);
  const startTime = Date.now();

  try {
    // STEP 1: Always try digital text extraction first (fastest and most accurate)
    console.log(`\n📄 === STEP 1: DIGITAL TEXT EXTRACTION ===`);
    let digitalResult = null;
    let hasUsableDigitalText = false;

    if (!forceOCR) {
      try {
        digitalResult = await extractDigitalText(buffer, maxPages);
        hasUsableDigitalText =
          digitalResult &&
          digitalResult.text &&
          digitalResult.text.trim().length > ocrFallbackThreshold;

        if (hasUsableDigitalText) {
          console.log(
            `✅ Digital text found: ${digitalResult.text.length} characters`
          );
          console.log(`📊 Quality score: ${digitalResult.qualityScore}`);

          // If digital text is sufficient, return it (much faster and more accurate)
          if (
            digitalResult.qualityScore > 0.7 ||
            digitalResult.text.length > 1000
          ) {
            console.log(
              `🎉 Digital text is sufficient - skipping OCR for efficiency`
            );
            return {
              text: digitalResult.text,
              method: "digital-text",
              extractionTime: Date.now() - startTime,
              qualityScore: digitalResult.qualityScore,
              metadata: digitalResult.metadata,
            };
          }
        } else {
          console.log(
            `⚠️ Digital text insufficient (${
              digitalResult?.text?.length || 0
            } chars) - proceeding to OCR`
          );
        }
      } catch (error) {
        console.warn(
          `⚠️ Digital extraction failed: ${error.message} - proceeding to OCR`
        );
      }
    }

    // STEP 2: OCR extraction (for scanned PDFs or when digital text is insufficient)
    console.log(`\n🔍 === STEP 2: OCR EXTRACTION ===`);
    let ocrResult = null;

    try {
      ocrResult = await extractWithSmartOCR(buffer, {
        maxPages,
        quality: ocrQuality,
        digitalFallback: digitalResult,
      });

      if (ocrResult && ocrResult.text && ocrResult.text.length > 0) {
        console.log(`✅ OCR extraction: ${ocrResult.text.length} characters`);

        // STEP 3: Combine digital and OCR if both exist
        if (hasUsableDigitalText && digitalResult.text.length > 0) {
          console.log(`\n🔄 === STEP 3: INTELLIGENT COMBINATION ===`);
          const combined = combineDigitalAndOCR(digitalResult, ocrResult);

          return {
            text: combined.text,
            method: "hybrid-digital-ocr",
            extractionTime: Date.now() - startTime,
            qualityScore: combined.qualityScore,
            metadata: combined.metadata,
          };
        } else {
          // OCR only
          return {
            text: ocrResult.text,
            method: "ocr-only",
            extractionTime: Date.now() - startTime,
            qualityScore: ocrResult.qualityScore,
            metadata: ocrResult.metadata,
          };
        }
      }
    } catch (error) {
      console.warn(`⚠️ OCR extraction failed: ${error.message}`);
    }

    // STEP 4: Fallback to digital text if OCR failed
    if (hasUsableDigitalText) {
      console.log(`\n🔄 === FALLBACK: USING DIGITAL TEXT ===`);
      return {
        text: digitalResult.text,
        method: "digital-fallback",
        extractionTime: Date.now() - startTime,
        qualityScore: digitalResult.qualityScore,
        metadata: digitalResult.metadata,
      };
    }

    throw new Error("Both digital and OCR extraction failed");
  } catch (error) {
    console.error(`❌ PDF extraction failed: ${error.message}`);
    throw error;
  }
}

/**
 * Extract digital text from PDF with optimized settings
 */
async function extractDigitalText(buffer, maxPages = 0) {
  console.log(`📄 Extracting digital text...`);

  const result = await pdf(buffer, {
    // Optimized settings for maximum text extraction
    normalizeWhitespace: true, // Clean up spacing
    disableCombineTextItems: false, // Allow text combination for better readability
    useOnlyCSSZoom: false,
    max: maxPages || 0, // 0 = all pages
  });

  if (!result.text || result.text.trim().length === 0) {
    throw new Error("No digital text found");
  }

  // Clean and normalize the text
  let cleanText = result.text;

  // Remove excessive whitespace while preserving structure
  cleanText = cleanText.replace(/\n{3,}/g, "\n\n"); // Max 2 consecutive newlines
  cleanText = cleanText.replace(/[ \t]{3,}/g, "  "); // Max 2 consecutive spaces
  cleanText = cleanText.replace(/\r\n/g, "\n"); // Normalize line endings
  cleanText = cleanText.trim();

  // Calculate quality score based on various factors
  const qualityScore = calculateTextQuality(cleanText, result.numpages);

  return {
    text: cleanText,
    qualityScore: qualityScore,
    metadata: {
      pageCount: result.numpages,
      originalLength: result.text.length,
      cleanedLength: cleanText.length,
      extractionType: "digital-text",
      hasStructure: qualityScore > 0.5,
    },
  };
}

/**
 * Smart OCR extraction with adaptive quality settings
 */
async function extractWithSmartOCR(buffer, options = {}) {
  const {
    maxPages = 0,
    quality = "balanced",
    digitalFallback = null,
  } = options;

  console.log(`🔍 Smart OCR extraction (${quality} quality)...`);

  const tmpDir = getTempDir();
  const id = `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  const inputPath = path.join(tmpDir, `smart_ocr_${id}.pdf`);

  try {
    fs.writeFileSync(inputPath, buffer);

    // Get page count
    const pdfInfo = await pdf(buffer, { max: 1 });
    const totalPages =
      maxPages > 0 ? Math.min(pdfInfo.numpages, maxPages) : pdfInfo.numpages;

    console.log(`📋 OCR processing ${totalPages} pages (${quality} quality)`);

    // Quality-based settings
    const settings = getOCRSettings(quality);

    const convert = pdf2pic.fromPath(inputPath, {
      density: settings.density,
      saveDir: tmpDir,
      saveName: `smart_page_${id}`,
      format: "png",
      width: settings.width,
      height: settings.height,
      quality: settings.imageQuality,
    });

    let allText = "";
    let successfulPages = 0;
    const pageResults = [];

    // Process pages with smart batching
    const batchSize = settings.batchSize;
    for (let i = 0; i < totalPages; i += batchSize) {
      const batch = [];
      const batchEnd = Math.min(i + batchSize, totalPages);

      console.log(`📄 Processing pages ${i + 1}-${batchEnd}...`);

      // Convert batch of pages
      for (let pageNum = i + 1; pageNum <= batchEnd; pageNum++) {
        try {
          const img = await convert(pageNum, { responseType: "buffer" });
          if (img && img.buffer && img.buffer.length > 0) {
            batch.push({ pageNum, buffer: img.buffer });
          }
        } catch (error) {
          console.warn(`⚠️ Page ${pageNum}: Image conversion failed`);
        }
      }

      // OCR batch of pages
      for (const { pageNum, buffer: imgBuffer } of batch) {
        try {
          const result = await Tesseract.recognize(imgBuffer, "eng", {
            logger: settings.enableLogging
              ? m => {
                  if (
                    m.status === "recognizing text" &&
                    m.progress % 0.25 < 0.01
                  ) {
                    console.log(
                      `     Page ${pageNum}: ${Math.round(m.progress * 100)}%`
                    );
                  }
                }
              : undefined,
            tessedit_pageseg_mode: settings.pagesegMode,
            tessedit_ocr_engine_mode: settings.ocrEngine,
            preserve_interword_spaces: "1",
          });

          const pageText = result.data.text;
          if (pageText && pageText.trim().length > 10) {
            allText += `\n\n=== PAGE ${pageNum} ===\n${pageText}`;
            successfulPages++;
            pageResults.push({
              page: pageNum,
              confidence: result.data.confidence,
              text: pageText,
              length: pageText.length,
            });
            console.log(
              `✅ Page ${pageNum}: ${pageText.length} chars (${Math.round(
                result.data.confidence
              )}% confidence)`
            );
          }
        } catch (error) {
          console.warn(`❌ Page ${pageNum} OCR failed: ${error.message}`);
        }
      }
    }

    if (successfulPages === 0) {
      throw new Error("No pages could be processed with OCR");
    }

    // Calculate overall confidence
    const avgConfidence =
      pageResults.reduce((sum, p) => sum + p.confidence, 0) /
      pageResults.length;
    const qualityScore = Math.min(avgConfidence / 100, 0.95); // Cap at 0.95

    console.log(
      `✅ OCR completed: ${successfulPages}/${totalPages} pages, ${Math.round(
        avgConfidence
      )}% avg confidence`
    );

    return {
      text: allText.trim(),
      qualityScore: qualityScore,
      metadata: {
        pagesProcessed: successfulPages,
        totalPages: totalPages,
        averageConfidence: avgConfidence,
        extractionType: `ocr-${quality}`,
        ocrSettings: settings,
        pageResults: pageResults,
      },
    };
  } finally {
    safeUnlink(inputPath);
    // Cleanup generated images
    try {
      const files = fs.readdirSync(tmpDir);
      files
        .filter(f => f.includes(id))
        .forEach(f => safeUnlink(path.join(tmpDir, f)));
    } catch (e) {
      console.warn(`⚠️ Cleanup failed: ${e.message}`);
    }
  }
}

/**
 * Get OCR settings based on quality preference
 */
function getOCRSettings(quality) {
  const settings = {
    fast: {
      density: 150,
      width: 1200,
      height: 1600,
      imageQuality: 80,
      pagesegMode: Tesseract.PSM.AUTO,
      ocrEngine: Tesseract.OEM.LSTM_ONLY,
      batchSize: 5,
      enableLogging: false,
    },
    balanced: {
      density: 200,
      width: 1600,
      height: 2100,
      imageQuality: 90,
      pagesegMode: Tesseract.PSM.AUTO,
      ocrEngine: Tesseract.OEM.LSTM_ONLY,
      batchSize: 3,
      enableLogging: true,
    },
    high: {
      density: 300,
      width: 2400,
      height: 3200,
      imageQuality: 100,
      pagesegMode: Tesseract.PSM.AUTO,
      ocrEngine: Tesseract.OEM.LSTM_ONLY,
      batchSize: 1,
      enableLogging: true,
    },
  };

  return settings[quality] || settings.balanced;
}

/**
 * Calculate text quality score
 */
function calculateTextQuality(text, pageCount) {
  if (!text || text.length === 0) return 0;

  let score = 0;

  // Length factor (more text generally means better extraction)
  const lengthScore = Math.min(text.length / (pageCount * 500), 1); // Assume ~500 chars per page minimum
  score += lengthScore * 0.3;

  // Structure factor (paragraphs, sentences)
  const paragraphs = text
    .split("\n\n")
    .filter(p => p.trim().length > 20).length;
  const sentences = text
    .split(/[.!?]+/)
    .filter(s => s.trim().length > 10).length;
  const structureScore = Math.min(
    (paragraphs + sentences) / (pageCount * 5),
    1
  );
  score += structureScore * 0.3;

  // Word quality factor
  const words = text.split(/\s+/).filter(w => w.length > 2);
  const avgWordLength =
    words.reduce((sum, w) => sum + w.length, 0) / words.length;
  const wordQualityScore = Math.min(avgWordLength / 6, 1); // Average English word is ~4-6 chars
  score += wordQualityScore * 0.2;

  // Character variety (more variety usually means better text)
  const uniqueChars = new Set(text.toLowerCase()).size;
  const varietyScore = Math.min(uniqueChars / 50, 1); // Expect at least 50 unique characters
  score += varietyScore * 0.2;

  return Math.min(score, 1.0);
}

/**
 * Intelligently combine digital text and OCR results
 */
function combineDigitalAndOCR(digitalResult, ocrResult) {
  console.log(`🔄 Combining digital text and OCR results...`);

  const digitalText = digitalResult.text;
  const ocrText = ocrResult.text;

  // If digital text is much longer and has good quality, prefer it
  if (
    digitalText.length > ocrText.length * 1.5 &&
    digitalResult.qualityScore > 0.6
  ) {
    console.log(
      `📋 Using digital text as primary (${digitalText.length} vs ${ocrText.length} chars)`
    );
    return {
      text: digitalText,
      qualityScore: digitalResult.qualityScore,
      metadata: {
        combinationStrategy: "digital-primary",
        digitalLength: digitalText.length,
        ocrLength: ocrText.length,
        digitalQuality: digitalResult.qualityScore,
        ocrQuality: ocrResult.qualityScore,
      },
    };
  }

  // If OCR has much higher confidence, prefer it
  if (ocrResult.qualityScore > digitalResult.qualityScore + 0.2) {
    console.log(
      `📋 Using OCR as primary (higher confidence: ${ocrResult.qualityScore} vs ${digitalResult.qualityScore})`
    );
    return {
      text: ocrText,
      qualityScore: ocrResult.qualityScore,
      metadata: {
        combinationStrategy: "ocr-primary",
        digitalLength: digitalText.length,
        ocrLength: ocrText.length,
        digitalQuality: digitalResult.qualityScore,
        ocrQuality: ocrResult.qualityScore,
      },
    };
  }

  // Combine both with digital text as base
  console.log(`📋 Combining both sources with digital as base`);
  const combinedText = `${digitalText}\n\n=== OCR SUPPLEMENTAL DATA ===\n${ocrText}`;
  const combinedQuality =
    (digitalResult.qualityScore + ocrResult.qualityScore) / 2;

  return {
    text: combinedText,
    qualityScore: combinedQuality,
    metadata: {
      combinationStrategy: "hybrid-combined",
      digitalLength: digitalText.length,
      ocrLength: ocrText.length,
      combinedLength: combinedText.length,
      digitalQuality: digitalResult.qualityScore,
      ocrQuality: ocrResult.qualityScore,
    },
  };
}

/**
 * Quick check if PDF has extractable digital text
 */
async function hasExtractableText(buffer) {
  try {
    const result = await pdf(buffer, { max: 1 });
    return result.text && result.text.trim().length > 50;
  } catch (error) {
    return false;
  }
}

/**
 * Extract text with automatic strategy selection
 */
async function extractTextAuto(buffer, options = {}) {
  console.log(`🤖 Auto-selecting extraction strategy...`);

  const hasDigitalText = await hasExtractableText(buffer);

  if (hasDigitalText) {
    console.log(`📄 Digital text detected - using hybrid strategy`);
    return extractFromPDF(buffer, { ...options, forceOCR: false });
  } else {
    console.log(`🔍 No digital text detected - using OCR strategy`);
    return extractFromPDF(buffer, {
      ...options,
      forceOCR: true,
      ocrQuality: "balanced",
    });
  }
}

module.exports = {
  extractFromPDF,
  extractTextAuto,
  hasExtractableText,
};
