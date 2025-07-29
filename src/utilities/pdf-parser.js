const pdf = require("pdf-parse");
const Tesseract = require("tesseract.js");
const fs = require("fs");
const path = require("path");
const os = require("os");
const { spawn } = require("child_process");
const pdf2pic = require("pdf2pic");

function getTempDir() {
  return process.env.AWS_LAMBDA_FUNCTION_NAME ? "/tmp" : os.tmpdir();
}

function safeUnlink(filePath) {
  try {
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
  } catch (e) {
    console.warn(`⚠️ Failed to delete ${filePath}: ${e.message}`);
  }
}

async function testOCRmyPDFAvailable() {
  return new Promise(resolve => {
    console.log("🔍 Testing OCRmyPDF availability...");
    const testProcess = spawn("py", ["-m", "ocrmypdf", "--version"]);
    let hasOutput = false;

    testProcess.stdout.on("data", data => {
      hasOutput = true;
    });

    testProcess.on("close", code => resolve(hasOutput && code === 0));
    testProcess.on("error", () => resolve(false));

    setTimeout(() => {
      if (!hasOutput) {
        testProcess.kill();
        resolve(false);
      }
    }, 10000);
  });
}

// Add function to check if PDF has extractable text
async function hasExtractableText(buffer) {
  try {
    const result = await pdf(buffer, { max: 1 }); // Check only first page
    return result.text && result.text.trim().length > 50;
  } catch (error) {
    return false;
  }
}

/**
 * Enhanced PDF text extraction optimized for drilling reports (BHA, MMR, RVEN)
 * @param {Buffer} buffer - PDF file buffer
 * @param {Object} [options] - Extraction options
 * @param {boolean} [options.preserveStructure=true] - Maintain table and section structure
 * @param {boolean} [options.enhanceDrillingTerms=true] - Apply drilling-specific text enhancement
 * @param {string} [options.documentType] - Document type hint ('BHA', 'MMR', 'RVEN')
 * @returns {Promise<Object>} - Extraction result with enhanced drilling content
 */
async function extractFromPDF(buffer, options = {}) {
  const {
    preserveStructure = true,
    enhanceDrillingTerms = true,
    documentType = null,
  } = options;

  console.log(`🔍 Starting enhanced PDF extraction for drilling reports...`);
  console.log(
    `📋 Options: structure=${preserveStructure}, enhance=${enhanceDrillingTerms}, type=${
      documentType || "auto"
    }`
  );

  // Check if PDF has extractable text first
  const hasText = await hasExtractableText(buffer);
  console.log(`📄 PDF has extractable text: ${hasText}`);

  const ocrAvailable = await testOCRmyPDFAvailable();

  // Enhanced extraction strategies optimized for drilling reports
  const strategies = [
    {
      name: "enhanced-digital",
      handler: extractEnhancedDigitalText,
      timeout: 45000,
      priority: 1,
    },
    {
      name: "structure-preserving",
      handler: extractStructurePreservingText,
      timeout: 60000,
      priority: 2,
    },
    // Only use OCRmyPDF if text extraction fails or produces poor results
    ...(ocrAvailable && !hasText
      ? [
          {
            name: "ocrmypdf-drilling",
            handler: extractWithEnhancedOCRmyPDF,
            timeout: 180000,
            priority: 3,
          },
        ]
      : []),
    {
      name: "tesseract-drilling",
      handler: extractWithEnhancedTesseract,
      timeout: 180000,
      priority: hasText ? 5 : 4, // Lower priority if text exists
    },
  ];

  const results = [];
  let bestResult = null;

  for (const strategy of strategies) {
    try {
      console.log(`🔧 Trying extraction strategy: ${strategy.name}`);
      const start = Date.now();

      const result = await withTimeout(
        strategy.handler(buffer, { documentType, preserveStructure }),
        strategy.timeout,
        strategy.name
      );

      const duration = Date.now() - start;
      const enhancedResult = {
        strategy: strategy.name,
        ...result,
        extractionTime: duration,
        priority: strategy.priority,
      };

      results.push(enhancedResult);

      // Enhanced quality assessment for drilling reports
      const quality = assessDrillingContentQuality(result.text, documentType);
      enhancedResult.qualityScore = quality.score;
      enhancedResult.qualityMetrics = quality.metrics;

      console.log(
        `✅ Strategy '${strategy.name}': ${
          result.text?.length || 0
        } chars, quality: ${quality.score.toFixed(2)}`
      );

      // Accept result if it meets drilling content quality threshold
      if (quality.score >= 0.7 && result.text?.trim().length >= 200) {
        bestResult = enhancedResult;
        break;
      }

      // Keep track of best result so far
      if (!bestResult || quality.score > bestResult.qualityScore) {
        bestResult = enhancedResult;
      }
    } catch (err) {
      console.warn(`⚠️ Strategy '${strategy.name}' failed: ${err.message}`);
      results.push({
        strategy: strategy.name,
        error: err.message,
        textLength: 0,
        qualityScore: 0,
      });
    }

    // Small delay between strategies
    await new Promise(r => setTimeout(r, 300));
  }

  if (!bestResult || !bestResult.text) {
    throw new Error("PDF contains no extractable drilling report content.");
  }

  // Apply drilling-specific enhancements if enabled
  let finalText = bestResult.text;
  if (enhanceDrillingTerms) {
    finalText = enhanceDrillingText(finalText, documentType);
  }

  // Add quality and extraction metadata
  const qualityNote =
    bestResult.qualityScore < 0.5
      ? "\n\n[Note: Extracted content may be incomplete or low quality for drilling analysis.]"
      : bestResult.qualityScore < 0.7
      ? "\n\n[Note: Some drilling data may be missing or unclear.]"
      : bestResult.strategy.includes("ocr") ||
        bestResult.strategy.includes("tesseract")
      ? "\n\n[Note: Content extracted using OCR - verify technical values.]"
      : "";

  console.log(
    `🎉 PDF extraction completed using '${
      bestResult.strategy
    }' with quality score ${bestResult.qualityScore.toFixed(2)}`
  );

  return {
    text: finalText + qualityNote,
    method: bestResult.strategy,
    extractionTime: bestResult.extractionTime,
    qualityScore: bestResult.qualityScore,
    qualityMetrics: bestResult.qualityMetrics,
    documentType: detectDocumentType(finalText) || documentType,
    warning:
      bestResult.qualityScore < 0.7 ? "Low quality extraction" : undefined,
    extractionResults: results.map(r => ({
      strategy: r.strategy,
      success: !r.error,
      textLength: r.textLength || r.text?.length || 0,
      qualityScore: r.qualityScore || 0,
      error: r.error,
    })),
  };
}

/**
 * Enhanced digital text extraction with drilling report structure preservation
 */
async function extractEnhancedDigitalText(buffer, options = {}) {
  console.log("📄 Enhanced digital text extraction...");

  const result = await pdf(buffer, {
    normalizeWhitespace: false, // Preserve spacing for tables
    disableCombineTextItems: true, // Keep text items separate
    useOnlyCSSZoom: true, // Better handling of scaled content
  });

  if (!result.text || result.text.trim().length < 50) {
    throw new Error("Insufficient digital text extracted");
  }

  // Enhanced text processing for drilling reports
  let processedText = result.text;

  if (options.preserveStructure) {
    processedText = preserveDrillingReportStructure(processedText);
  }

  return {
    text: processedText,
    method: "enhanced-digital",
    metadata: {
      pageCount: result.numpages,
      hasDigitalText: true,
      originalLength: result.text.length,
      processedLength: processedText.length,
    },
  };
}

/**
 * Structure-preserving extraction for tabular drilling data
 */
async function extractStructurePreservingText(buffer, options = {}) {
  console.log("📊 Structure-preserving extraction for drilling data...");

  const result = await pdf(buffer, {
    normalizeWhitespace: false,
    disableCombineTextItems: true,
    max: 0, // Process all pages
    version: "v1.10.100", // Use specific version for consistency
  });

  if (!result.text || result.text.trim().length < 100) {
    throw new Error("Insufficient structured text extracted");
  }

  // Apply advanced structure preservation
  const structuredText = preserveTabularStructure(
    result.text,
    options.documentType
  );

  return {
    text: structuredText,
    method: "structure-preserving",
    metadata: {
      pageCount: result.numpages,
      tablesDetected: countTables(structuredText),
      sectionsDetected: countSections(structuredText),
    },
  };
}

/**
 * Enhanced OCRmyPDF with drilling-specific optimizations
 */
async function extractWithEnhancedOCRmyPDF(buffer, options = {}) {
  console.log("🔍 Enhanced OCRmyPDF extraction for drilling reports...");

  const tmpDir = getTempDir();
  const id = `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  const inputPath = path.join(tmpDir, `input_ocr_${id}.pdf`);
  const outputPath = path.join(tmpDir, `output_ocr_${id}.pdf`);

  try {
    fs.writeFileSync(inputPath, buffer);

    // Enhanced OCR settings for drilling reports
    await runEnhancedOCRmyPDF(inputPath, outputPath, options.documentType);

    const ocrBuffer = fs.readFileSync(outputPath);
    const result = await pdf(ocrBuffer, { normalizeWhitespace: false });

    if (!result.text || result.text.trim().length < 100) {
      throw new Error("OCR produced insufficient text");
    }

    // Post-process OCR text for drilling content
    const cleanedText = cleanOCRDrillingText(result.text);

    return {
      text: cleanedText,
      method: "ocrmypdf-drilling",
      metadata: {
        ocrProcessed: true,
        originalLength: result.text.length,
        cleanedLength: cleanedText.length,
      },
    };
  } finally {
    safeUnlink(inputPath);
    safeUnlink(outputPath);
  }
}

/**
 * Enhanced Tesseract OCR optimized for drilling technical documents
 */
async function extractWithEnhancedTesseract(buffer, options = {}) {
  console.log("🔤 Enhanced Tesseract OCR for drilling reports...");

  const tmpDir = getTempDir();
  const id = `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  const inputPath = path.join(tmpDir, `input_tess_${id}.pdf`);

  try {
    fs.writeFileSync(inputPath, buffer);

    // Validate PDF file integrity
    if (!fs.existsSync(inputPath) || fs.statSync(inputPath).size === 0) {
      throw new Error("Invalid PDF file for Tesseract processing");
    }

    // Convert PDF to images with optimized settings for technical content
    const convert = pdf2pic.fromPath(inputPath, {
      density: 150, // Higher density for technical text
      saveDir: tmpDir,
      saveName: `page_${id}`,
      format: "png",
      width: 1200, // Wider for table content
      height: 1600,
    });

    const maxPages = 3; // Reduced to prevent memory issues
    let allText = "";
    let successfulPages = 0;

    for (let i = 1; i <= maxPages; i++) {
      try {
        console.log(`📄 Processing page ${i} with enhanced OCR...`);

        const img = await convert(i, { responseType: "buffer" });

        // Validate image buffer
        if (!img || !img.buffer || img.buffer.length === 0) {
          console.warn(`⚠️ Page ${i}: Invalid image buffer, skipping...`);
          continue;
        }

        // Enhanced Tesseract configuration for drilling reports
        const result = await Tesseract.recognize(img.buffer, "eng", {
          logger: m => {
            if (m.status === "recognizing text" && m.progress % 0.1 < 0.01) {
              console.log(`   OCR progress: ${Math.round(m.progress * 100)}%`);
            }
          },
          tessedit_pageseg_mode: Tesseract.PSM.AUTO,
          tessedit_ocr_engine_mode: Tesseract.OEM.LSTM_ONLY,
          // Optimized for technical/numerical content
          tessedit_char_whitelist:
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.,:-/_()[]{}#%+=<>|\"'\\",
          // Better handling of tables and structured data
          preserve_interword_spaces: "1",
        });

        const pageText = result.data.text.trim();
        if (pageText.length > 20) {
          allText += `\n\n=== PAGE ${i} ===\n${pageText}`;
          successfulPages++;
        }
      } catch (err) {
        console.warn(`⚠️ Page ${i} OCR failed: ${err.message}`);
        // Continue processing other pages
        continue;
      }
    }

    if (allText.trim().length < 100 || successfulPages === 0) {
      throw new Error(
        `Tesseract OCR produced insufficient text (${successfulPages} pages processed)`
      );
    }

    // Clean and enhance OCR output for drilling content
    const cleanedText = cleanOCRDrillingText(allText);

    return {
      text: cleanedText,
      method: "tesseract-drilling",
      metadata: {
        pagesProcessed: successfulPages,
        totalAttempted: maxPages,
        ocrProcessed: true,
        originalLength: allText.length,
        cleanedLength: cleanedText.length,
      },
    };
  } catch (error) {
    console.error(`🚨 Tesseract extraction failed: ${error.message}`);
    throw new Error(`Tesseract OCR failed: ${error.message}`);
  } finally {
    safeUnlink(inputPath);
    // Clean up image files more thoroughly
    try {
      const files = fs.readdirSync(tmpDir);
      files
        .filter(
          f => f.includes(id) && (f.endsWith(".png") || f.endsWith(".jpg"))
        )
        .forEach(f => {
          try {
            safeUnlink(path.join(tmpDir, f));
          } catch (e) {
            console.warn(`⚠️ Failed to clean up ${f}: ${e.message}`);
          }
        });
    } catch (e) {
      console.warn(`⚠️ Failed to clean up temp directory: ${e.message}`);
    }
  }
}

/**
 * Preserve drilling report structure during text extraction
 */
function preserveDrillingReportStructure(text) {
  let structured = text;

  // Preserve section headers
  structured = structured.replace(
    /(BHA Performance Report|Motor Performance Report|Motor data|Bit Data|Drilling Parameters|BHA Details|Additional Comments)/gi,
    "\n\n$1\n"
  );

  // Preserve table headers and data alignment
  structured = structured.replace(
    /(SN|CNX TOP|CNX BTM|OD|ID|Weight|Length|Total Length)/gi,
    "\n$1"
  );

  // Preserve key-value pairs
  structured = structured.replace(
    /(Make|Model|Grade|Stator Fit|TFA|Avg ROP|Total Drilled|Drill Hrs)[\s]*[:]/gi,
    "\n$1:"
  );

  // Clean up excessive whitespace while preserving structure
  structured = structured
    .replace(/\n{4,}/g, "\n\n\n")
    .replace(/[ \t]{3,}/g, " ")
    .trim();

  return structured;
}

/**
 * Preserve tabular structure for drilling data
 */
function preserveTabularStructure(text, documentType) {
  let structured = text;

  // Detect and preserve BHA component tables
  if (documentType === "BHA" || text.includes("BHA Details")) {
    structured = structured.replace(
      /(Description[\s\S]*?SN[\s\S]*?CNX TOP[\s\S]*?Length)/gi,
      "\n\nBHA COMPONENT TABLE:\n$1\n"
    );
  }

  // Preserve performance data tables
  structured = structured.replace(
    /(Run Data[\s\S]*?Motor data[\s\S]*?Drilling Parameters)/gi,
    "\n\nPERFORMANCE DATA TABLE:\n$1\n"
  );

  // Preserve mud data tables
  structured = structured.replace(
    /(600\/300[\s\S]*?200\/100[\s\S]*?Temp \(degF\))/gi,
    "\n\nMUD DATA TABLE:\n$1\n"
  );

  return structured;
}

/**
 * Enhanced OCRmyPDF execution with drilling-specific settings
 */
/**
 * Enhanced OCRmyPDF execution with drilling-specific settings - FIXED VERSION
 */
function runEnhancedOCRmyPDF(input, output, documentType) {
  return new Promise((resolve, reject) => {
    // FIXED: Removed incompatible argument combinations
    // --redo-ocr is not compatible with --deskew, --clean-final, and --remove-background
    const args = [
      "-m",
      "ocrmypdf",
      "--language",
      "eng",
      "--jobs",
      "2",
      "--optimize",
      "1", // Light optimization
      "--oversample",
      "150", // Higher sampling for technical text
      "--force-ocr", // Force OCR even if text exists
      input,
      output,
    ];

    console.log(
      `🔧 Running enhanced OCRmyPDF for ${
        documentType || "drilling"
      } document...`
    );

    const ocr = spawn("py", args, {
      env: { ...process.env },
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stderr = "";
    let stdout = "";

    ocr.stdout.on("data", data => {
      stdout += data.toString();
    });

    ocr.stderr.on("data", data => {
      stderr += data.toString();
    });

    const timeout = setTimeout(() => {
      ocr.kill("SIGKILL");
      reject(new Error("Enhanced OCR timed out after 3 minutes"));
    }, 180000);

    ocr.on("close", code => {
      clearTimeout(timeout);
      if (code === 0) {
        resolve();
      } else {
        // Check if it's the "already has text" error and handle gracefully
        if (stderr.includes("already has text")) {
          console.warn("⚠️ PDF already contains text, OCR may not be needed");
          reject(new Error("PDF already contains extractable text"));
        } else {
          reject(new Error(`Enhanced OCR failed with code ${code}: ${stderr}`));
        }
      }
    });

    ocr.on("error", err => {
      clearTimeout(timeout);
      reject(new Error(`Enhanced OCR process error: ${err.message}`));
    });
  });
}

/**
 * Clean OCR text specifically for drilling content
 */
function cleanOCRDrillingText(text) {
  let cleaned = text;

  // Fix common OCR errors in drilling terminology
  const drillingCorrections = {
    // Motor/Stator terms
    Statar: "Stator",
    Matar: "Motor",
    Flaat: "Float",
    UBHO: "UBHO", // Ensure consistency
    NMOC: "NMDC",

    // Units and measurements
    klhs: "klbs",
    "rpm ": "rpm ",
    "gpm ": "gpm ",
    "psi ": "psi ",
    Usft: "usft",
    degF: "degF",

    // Technical terms
    ROP: "ROP",
    WOB: "WOB",
    TFA: "TFA",
    BHA: "BHA",
    DDl: "DD1", // Common OCR error

    // Fix spacing issues around numbers
    "(\\d+)\\s*\\.\\s*(\\d+)": "$1.$2", // Fix decimal points
    "(\\d+)\\s+(klbs|rpm|gpm|psi|usft|degF)": "$1 $2", // Fix unit spacing
  };

  for (const [error, correction] of Object.entries(drillingCorrections)) {
    const regex = new RegExp(error, "gi");
    cleaned = cleaned.replace(regex, correction);
  }

  // Fix table alignment issues
  cleaned = cleaned.replace(/\s{2,}/g, " "); // Normalize spacing
  cleaned = cleaned.replace(/\n\s*\n\s*\n/g, "\n\n"); // Clean line breaks

  return cleaned.trim();
}

/**
 * Assess content quality specifically for drilling reports
 */
function assessDrillingContentQuality(text, documentType) {
  if (!text || text.length < 50) {
    return { score: 0, metrics: { reason: "insufficient_text" } };
  }

  const metrics = {};
  let score = 0;

  // Check for drilling-specific terminology (40% of score)
  const drillingTerms = [
    /motor|stator|bit|bha/gi,
    /rop|wob|tfa|differential/gi,
    /drilling|circulation|slide|rotary/gi,
    /nmdc|ubho|float.*sub|shock.*sub/gi,
  ];

  let termScore = 0;
  drillingTerms.forEach(pattern => {
    const matches = text.match(pattern);
    if (matches) termScore += Math.min(matches.length, 5);
  });
  metrics.drillingTermsFound = termScore;
  score += Math.min(termScore / 10, 0.4);

  // Check for numerical data (30% of score)
  const numericalPatterns = [
    /\d+\.?\d*\s*(?:klbs|rpm|gpm|psi|usft|degf)/gi,
    /\d+\.?\d*\s*(?:hrs|%)/gi,
    /[+-]?\d+\.?\d*(?:\s*stator\s*fit)/gi,
  ];

  let numericalScore = 0;
  numericalPatterns.forEach(pattern => {
    const matches = text.match(pattern);
    if (matches) numericalScore += matches.length;
  });
  metrics.numericalDataPoints = numericalScore;
  score += Math.min(numericalScore / 20, 0.3);

  // Check for structured content (20% of score)
  const structureIndicators = [
    /BHA Performance Report|Motor data|Bit Data/gi,
    /SN[\s\S]*CNX TOP[\s\S]*Length/gi,
    /Make[\s\S]*Model[\s\S]*Grade/gi,
  ];

  let structureScore = 0;
  structureIndicators.forEach(pattern => {
    if (pattern.test(text)) structureScore += 1;
  });
  metrics.structureIndicators = structureScore;
  score += Math.min(structureScore / 3, 0.2);

  // Check for document type specific content (10% of score)
  if (documentType) {
    const typePatterns = {
      BHA: /BHA.*Details|Assembly|Components/gi,
      MMR: /Motor.*Measurement|Stator.*Fit|Motor.*Performance/gi,
      RVEN: /Run.*Evaluation|Vendor.*Notes|Performance.*Summary/gi,
    };

    const typePattern = typePatterns[documentType.toUpperCase()];
    if (typePattern && typePattern.test(text)) {
      score += 0.1;
      metrics.documentTypeMatch = true;
    }
  }

  metrics.finalScore = score;
  metrics.textLength = text.length;

  return { score: Math.min(score, 1.0), metrics };
}

/**
 * Detect document type from content
 */
function detectDocumentType(text) {
  const typePatterns = {
    BHA: /BHA.*Performance.*Report|Bottom.*Hole.*Assembly/gi,
    MMR: /Motor.*Measurement.*Report|Motor.*Performance.*Report/gi,
    RVEN: /Run.*Evaluation|Vendor.*Notes|RVEN/gi,
  };

  for (const [type, pattern] of Object.entries(typePatterns)) {
    if (pattern.test(text)) {
      return type;
    }
  }

  return null;
}

/**
 * Enhance text with drilling-specific terminology normalization
 */
function enhanceDrillingText(text, documentType) {
  let enhanced = text;

  // Add context markers for better chunking
  enhanced = enhanced.replace(
    /(Motor data|Bit Data|Drilling Parameters|BHA Details)/gi,
    "\n\n### $1 ###\n"
  );

  // Normalize technical terminology
  const normalizations = {
    "Rate of Penetration": "ROP",
    "Weight on Bit": "WOB",
    "Total Flow Area": "TFA",
    "Differential Pressure": "Diff Press",
    "Circulation Hours": "Circ Hrs",
    "Drilling Hours": "Drill Hrs",
  };

  for (const [long, short] of Object.entries(normalizations)) {
    const regex = new RegExp(long, "gi");
    enhanced = enhanced.replace(regex, short);
  }

  return enhanced;
}

/**
 * Count tables in extracted text
 */
function countTables(text) {
  const tableIndicators = [
    /SN[\s\S]*CNX TOP[\s\S]*Length/gi,
    /Description[\s\S]*Weight[\s\S]*Total Length/gi,
    /Run Data[\s\S]*Motor data[\s\S]*Drilling Parameters/gi,
  ];

  let count = 0;
  tableIndicators.forEach(pattern => {
    const matches = text.match(pattern);
    if (matches) count += matches.length;
  });

  return count;
}

/**
 * Count sections in extracted text
 */
function countSections(text) {
  const sectionHeaders = text.match(
    /(?:Motor data|Bit Data|Drilling Parameters|BHA Details|Additional Comments)/gi
  );
  return sectionHeaders ? sectionHeaders.length : 0;
}

/**
 * Timeout wrapper for extraction strategies
 */
async function withTimeout(promise, ms, operationName) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(
        () => reject(new Error(`${operationName} timed out after ${ms}ms`)),
        ms
      )
    ),
  ]);
}

module.exports = extractFromPDF;
