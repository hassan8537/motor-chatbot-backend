const { RecursiveCharacterTextSplitter } = require("langchain/text_splitter");

/**
 * Enhanced generalized text chunking optimized for drilling reports (BHA, MMR, RVEN)
 * @param {string} text - Full input text from drilling reports
 * @returns {Promise<string[]>} - Returns array of enhanced chunk strings
 */
async function chunkText(text) {
  const chunkSize = Number(process.env.CHUNK_SIZE || 1350);
  const chunkOverlap = Number(process.env.CHUNK_OVERLAP || 250);

  console.log(
    `📋 Enhanced drilling report chunking with size: ${chunkSize}, overlap: ${chunkOverlap}`
  );

  if (chunkOverlap >= chunkSize) {
    throw new Error(
      "Text chunking configuration error: chunkOverlap must be smaller than chunkSize"
    );
  }

  if (!text || typeof text !== "string") {
    throw new Error("Invalid text input: must be a non-empty string");
  }

  const cleanText = text.trim();

  // Handle very short text
  if (cleanText.length < 100) {
    console.warn(
      "⚠️ Text too short to chunk reliably. Returning enhanced single chunk."
    );
    return [enhanceChunkWithDrillingContext(cleanText)];
  }

  try {
    // Pre-process text to preserve important drilling data relationships
    const preprocessedText = preprocessDrillingText(cleanText);

    // Use drilling-aware separators
    const drillingSeparators = getDrillingAwareSeparators();

    const splitter = new RecursiveCharacterTextSplitter({
      chunkSize,
      chunkOverlap,
      separators: drillingSeparators,
    });

    const docs = await splitter.createDocuments([preprocessedText]);

    if (!docs || docs.length === 0) {
      console.warn("⚠️ Splitter returned no chunks. Using enhanced full text.");
      return [enhanceChunkWithDrillingContext(cleanText)];
    }

    // Process chunks with drilling-specific enhancements
    let chunks = docs
      .map(doc => doc.pageContent.trim())
      .filter(chunk => chunk.length >= 50) // Higher minimum for technical content
      .map(chunk => enhanceChunkWithDrillingContext(chunk))
      .filter(chunk => isValidDrillingChunk(chunk));

    // If filtering removed all chunks, use original with basic enhancement
    if (chunks.length === 0) {
      console.warn(
        "⚠️ All enhanced chunks filtered out. Using basic enhanced chunks."
      );
      chunks = docs.map(doc =>
        enhanceChunkWithDrillingContext(doc.pageContent.trim())
      );
    }

    // Ensure we have valid chunks
    const validChunks = chunks.filter(chunk => {
      return chunk && typeof chunk === "string" && chunk.trim().length > 0;
    });

    if (validChunks.length === 0) {
      console.warn(
        "⚠️ No valid chunks created. Returning enhanced original text."
      );
      return [enhanceChunkWithDrillingContext(cleanText)];
    }

    // Add structured data summary chunk if significant metrics found
    const structuredSummary = extractStructuredDataSummary(cleanText);
    if (structuredSummary) {
      validChunks.unshift(structuredSummary);
    }

    console.log(
      `✅ Enhanced drilling chunking complete: ${
        validChunks.length
      } chunks created (avg ${Math.round(
        cleanText.length / validChunks.length
      )} chars)`
    );

    // Enhanced debugging with drilling context
    validChunks.forEach((chunk, i) => {
      const type = identifyDrillingContentType(chunk);
      const metrics = countTechnicalMetrics(chunk);
      console.log(
        `📄 Chunk ${i}: ${
          chunk.length
        } chars - Type: ${type} - Metrics: ${metrics} - "${chunk.substring(
          0,
          50
        )}..."`
      );
    });

    return validChunks;
  } catch (error) {
    console.error("❌ Enhanced text chunking error:", error.message);
    console.warn("⚠️ Falling back to enhanced simple text splitting");

    // Enhanced fallback
    return enhancedSimpleTextSplit(cleanText, chunkSize, chunkOverlap);
  }
}

/**
 * Get drilling-aware separators that preserve technical relationships
 */
function getDrillingAwareSeparators() {
  return [
    // Major section breaks (highest priority)
    "\n\n=== ",
    "\n\nBHA Performance Report",
    "\n\nMotor Performance Report",
    "\n\nRun Data",
    "\n\nMotor data",
    "\n\nMotor Data",
    "\n\nBit Data",
    "\n\nDrilling Parameters",
    "\n\nBHA Details",
    "\n\nAdditional Comments",
    "\n\nSensor Offsets",
    "\n\nDirectional Performance",

    // Table and data structure breaks
    "\n\nDescription\t",
    "\n\nSN\t",
    "\n\nCNX TOP\t",

    // Sub-section breaks
    "\n\nFlow Range",
    "\n\nMax DiffP",
    "\n\nStator Vendor",
    "\n\nBearing Gap",
    "\n\nTotal Drilled",

    // Standard document breaks
    "\n\n",
    "\n",

    // Sentence breaks (preserve technical phrases)
    ". ",
    "! ",
    "? ",
    "; ",

    // Last resort
    " ",
    "",
  ];
}

/**
 * Pre-process text to preserve drilling data relationships
 */
function preprocessDrillingText(text) {
  let processed = text;

  // Preserve important technical relationships by adding context markers
  const preservePatterns = [
    // Motor/Stator configurations
    { pattern: /(Stator Fit[:\s]+[+-]?\d+\.?\d*)/gi, replacement: "🔧 $1" },
    { pattern: /(TFA[:\s]+\d+\.?\d*\s*\([^)]+\))/gi, replacement: "🔩 $1" },
    { pattern: /(Make[:\s]+[A-Za-z\s]+(?=\n|Model))/gi, replacement: "🏭 $1" },

    // Performance metrics
    { pattern: /(Avg ROP[:\s]+\d+\.?\d*)/gi, replacement: "⚡ $1" },
    { pattern: /(WOB[^:]*[:\s]+\d+\.?\d*)/gi, replacement: "🔽 $1" },
    { pattern: /(RPM[:\s]+\d+\.?\d*)/gi, replacement: "🔄 $1" },

    // Assembly data
    { pattern: /(Weight[:\s]+\d+\.?\d*\s*lb\/ft)/gi, replacement: "⚖️ $1" },
    { pattern: /(Length[:\s]+\d+\.?\d*\s*[Uu]sft)/gi, replacement: "📏 $1" },

    // Operational data
    { pattern: /(Total Drilled[:\s]+\d+\.?\d*)/gi, replacement: "🎯 $1" },
    { pattern: /(Drill Hrs[:\s]+\d+\.?\d*)/gi, replacement: "⏱️ $1" },
  ];

  preservePatterns.forEach(({ pattern, replacement }) => {
    processed = processed.replace(pattern, replacement);
  });

  return processed;
}

/**
 * Enhance individual chunks with drilling context and extracted metrics
 */
function enhanceChunkWithDrillingContext(chunk) {
  if (!chunk || chunk.trim().length === 0) {
    return chunk;
  }

  // Remove preprocessing markers
  let cleanChunk = chunk.replace(/[🔧🔩🏭⚡🔽🔄⚖️📏🎯⏱️]\s*/g, "");

  // Identify content type and add appropriate context
  const contentType = identifyDrillingContentType(cleanChunk);
  const contextPrefix = getDrillingContextPrefix(contentType);

  // Extract inline metrics for this chunk
  const metrics = extractChunkMetrics(cleanChunk);
  const metricsContext =
    metrics.length > 0 ? ` [METRICS: ${metrics.join(", ")}]` : "";

  // Combine context, chunk, and metrics
  return `${contextPrefix}${cleanChunk}${metricsContext}`;
}

/**
 * Identify drilling content type from chunk content
 */
function identifyDrillingContentType(chunk) {
  // Define content type patterns (order matters - most specific first)
  const typePatterns = [
    {
      type: "MOTOR_STATOR_SPECS",
      patterns: [
        /stator fit|stator vendor|lobes|stages|bend angle/i,
        /make.*rival|make.*tag|make.*dynamax/i,
      ],
    },
    {
      type: "BIT_SPECIFICATIONS",
      patterns: [
        /bit.*tfa|pdc|baker|ulterra|reed/i,
        /model.*dd\d+|model.*cf\d+|model.*xp\d+/i,
      ],
    },
    {
      type: "BHA_ASSEMBLY",
      patterns: [
        /cnx top|cnx btm|weight.*lb\/ft|length.*usft/i,
        /float sub|ubho|nmdc|shock sub/i,
      ],
    },
    {
      type: "PERFORMANCE_DATA",
      patterns: [
        /wob|rop|rpm|differential pressure|avg diff press/i,
        /slide.*%|rotary.*%|drilling hours/i,
      ],
    },
    {
      type: "OPERATIONAL_METRICS",
      patterns: [
        /total drilled|circulation|pickup weight|so wt|pu wt/i,
        /depth in|depth out|inc in|inc out/i,
      ],
    },
    {
      type: "TECHNICAL_SUMMARY",
      patterns: [
        /additional comments|motor drained|bit graded|surface findings/i,
      ],
    },
  ];

  for (const { type, patterns } of typePatterns) {
    if (patterns.some(pattern => pattern.test(chunk))) {
      return type;
    }
  }

  return "DRILLING_GENERAL";
}

/**
 * Get context prefix based on content type
 */
function getDrillingContextPrefix(contentType) {
  const prefixes = {
    MOTOR_STATOR_SPECS: "MOTOR & STATOR SPECIFICATIONS: ",
    BIT_SPECIFICATIONS: "BIT CONFIGURATION & SPECS: ",
    BHA_ASSEMBLY: "BHA ASSEMBLY DETAILS: ",
    PERFORMANCE_DATA: "DRILLING PERFORMANCE METRICS: ",
    OPERATIONAL_METRICS: "OPERATIONAL DATA & MEASUREMENTS: ",
    TECHNICAL_SUMMARY: "TECHNICAL SUMMARY & NOTES: ",
    DRILLING_GENERAL: "DRILLING REPORT DATA: ",
  };

  return prefixes[contentType] || prefixes.DRILLING_GENERAL;
}

/**
 * Extract key metrics from individual chunks - enhanced for LLM queries
 */
function extractChunkMetrics(chunk) {
  const metrics = [];

  // Enhanced metric patterns targeting specific query types
  const metricPatterns = [
    // Weight & Force
    { pattern: /(\d+\.?\d*)\s*klbs/gi, type: "WEIGHT" },
    { pattern: /WOB[^:]*[:\s]+(\d+\.?\d*)/gi, type: "WOB" },
    { pattern: /PU\s+WT[:\s]+(\d+\.?\d*)/gi, type: "PICKUP_WT" },

    // Performance
    { pattern: /(\d+\.?\d*)\s*usft\/hr/gi, type: "ROP" },
    { pattern: /Avg ROP[:\s]+(\d+\.?\d*)/gi, type: "AVG_ROP" },
    { pattern: /Slide ROP[:\s]+(\d+\.?\d*)/gi, type: "SLIDE_ROP" },
    { pattern: /Rot ROP[:\s]+(\d+\.?\d*)/gi, type: "ROT_ROP" },

    // Drilling data
    { pattern: /Total Drilled[:\s]+(\d+\.?\d*)/gi, type: "TOTAL_DRILLED" },
    { pattern: /Rotary Drilled[:\s]+(\d+\.?\d*)/gi, type: "ROTARY_DRILLED" },
    { pattern: /Slide Drilled[:\s]+(\d+\.?\d*)/gi, type: "SLIDE_DRILLED" },

    // Time data
    { pattern: /Drill Hrs[:\s]+(\d+\.?\d*)/gi, type: "DRILL_HRS" },
    { pattern: /Circ Hrs[:\s]+(\d+\.?\d*)/gi, type: "CIRC_HRS" },
    { pattern: /Slide Hours[:\s]+(\d+\.?\d*)/gi, type: "SLIDE_HRS" },

    // Technical specs
    { pattern: /(\d+\.?\d*)\s*rpm/gi, type: "RPM" },
    { pattern: /(\d+\.?\d*)\s*gpm/gi, type: "FLOW" },
    { pattern: /(\d+\.?\d*)\s*psi/gi, type: "PRESSURE" },
    { pattern: /Diff Press[:\s]+(\d+\.?\d*)/gi, type: "DIFF_PRESS" },
    { pattern: /Max DiffP[:\s]+(\d+\.?\d*)/gi, type: "MAX_DIFF_P" },
    { pattern: /Max Torque[:\s]+(\d+\.?\d*)/gi, type: "MAX_TORQUE" },

    // Percentages
    { pattern: /%Slide[:\s]+(\d+\.?\d*)/gi, type: "SLIDE_PERCENT" },
    { pattern: /%Rotary[:\s]+(\d+\.?\d*)/gi, type: "ROTARY_PERCENT" },

    // Dimensions
    { pattern: /(\d+\.?\d*)\s*usft/gi, type: "LENGTH" },
    { pattern: /Total Length[:\s]+(\d+\.?\d*)/gi, type: "TOTAL_LENGTH" },
    { pattern: /Fishneck OD[:\s]+(\d+\.?\d*)/gi, type: "FISHNECK_OD" },

    // Motor specs
    { pattern: /([+-]?\d+\.?\d*)\s*(?:stator fit)/gi, type: "STATOR_FIT" },
    { pattern: /TFA[:\s]+(\d+\.?\d*)/gi, type: "TFA" },

    // Hole sizes
    { pattern: /(\d+\.?\d*)["\s]*(?:hole|section)/gi, type: "HOLE_SIZE" },
  ];

  for (const { pattern, type } of metricPatterns) {
    const matches = [...chunk.matchAll(pattern)];
    matches.forEach(match => {
      const value = match[1];
      const fullMatch = match[0];
      if (value && !isNaN(parseFloat(value))) {
        const metric = `${type}:${value}`;
        if (
          !metrics.some(m => m.startsWith(`${type}:`)) &&
          metrics.length < 10
        ) {
          metrics.push(metric);
        }
      }
    });
  }

  return metrics;
}

/**
 * Count technical metrics in chunk for debugging
 */
function countTechnicalMetrics(chunk) {
  const patterns = [
    /\d+\.?\d*\s*(?:klbs|rpm|gpm|psi|usft\/hr|degf|%|usft)/gi,
    /stator fit|tfa|wob|rop/gi,
    /make|model|grade|sn:/gi,
  ];

  let count = 0;
  patterns.forEach(pattern => {
    const matches = chunk.match(pattern);
    if (matches) count += matches.length;
  });

  return count;
}

/**
 * Validate that chunk contains meaningful drilling content
 */
function isValidDrillingChunk(chunk) {
  if (!chunk || chunk.length < 30) return false;

  // Check for drilling-relevant content
  const drillingIndicators = [
    /\d+\.?\d*\s*(?:klbs|rpm|gpm|psi|usft|degf)/i, // Technical units
    /motor|bit|bha|stator|drilling|performance/i, // Drilling terms
    /make|model|grade|vendor|specs/i, // Equipment terms
    /wob|rop|tfa|differential|pressure/i, // Performance terms
  ];

  return drillingIndicators.some(pattern => pattern.test(chunk));
}

/**
 * Extract comprehensive structured data summary optimized for LLM queries
 */
function extractStructuredDataSummary(text) {
  // Enhanced patterns to match all query requirements
  const summaryPatterns = {
    // Motor & Stator data
    motorMake: /Make[:\s]+([A-Za-z\s-]+)(?=\n|Model|$)/gi,
    statorVendor: /Stator Vendor[:\s]+([A-Za-z\s-]+)(?=\n|$)/gi,
    statorFit: /Stator Fit[:\s]+([+-]?\d+\.?\d*)/gi,
    motorConfig: /(\d+\/\d+)\s+(?:lobes|stages)/gi,
    maxDiffPress: /Max DiffP[:\s]+(\d+\.?\d*)/gi,
    maxTorque: /Max Torque[:\s]+(\d+\.?\d*)/gi,

    // Bit & BHA data
    bitModel: /(?:Bit\s+)?Model[:\s]+([A-Za-z0-9\-_]+)/gi,
    bitMake: /(?:Bit\s+)?Make[:\s]+([A-Za-z\s-]+)(?=\n|Model|$)/gi,
    tfa: /TFA[:\s]+(\d+\.?\d*)\s*\(([^)]+)\)/gi,
    holeSize: /(\d+\.?\d*)["\s]*(?:hole|section)/gi,

    // Performance metrics
    avgROP: /Avg ROP[:\s]+(\d+\.?\d*)/gi,
    slideROP: /Slide ROP[:\s]+(\d+\.?\d*)/gi,
    rotROP: /Rot ROP[:\s]+(\d+\.?\d*)/gi,
    totalDrilled: /Total Drilled[:\s]+(\d+\.?\d*)/gi,
    rotaryDrilled: /Rotary Drilled[:\s]+(\d+\.?\d*)/gi,
    slideDrilled: /Slide Drilled[:\s]+(\d+\.?\d*)/gi,

    // Operational data
    drillingHours: /(?:Total\s+)?Drill\s+Hrs[:\s]+(\d+\.?\d*)/gi,
    circHours: /(?:Off\s+Btm\s+)?Circ\s+Hrs[:\s]+(\d+\.?\d*)/gi,
    slideHours: /Slide Hours[:\s]+(\d+\.?\d*)/gi,
    slidePercent: /%Slide[:\s]+(\d+\.?\d*)/gi,

    // Weight & Pressure data
    pickupWeight: /PU\s+WT[:\s]+(\d+\.?\d*)/gi,
    wob: /WOB[^:]*[:\s]+(\d+\.?\d*)/gi,
    diffPress: /(?:Avg\s+)?Diff\s+Press[:\s]+(\d+\.?\d*)/gi,

    // BHA dimensions
    bhaLength: /Total Length[:\s]+(\d+\.?\d*)/gi,
    fishneckOD: /Fishneck OD[:\s]+(\d+\.?\d*)/gi,
    stabilizer: /(\d+\.?\d*)["\s]*(?:stab|stabilizer)/gi,

    // Depth data
    depthIn: /Depth In[:\s]+(\d+\.?\d*)/gi,
    depthOut: /Depth Out[:\s]+(\d+\.?\d*)/gi,

    // Motor nomenclature
    motorDesc:
      /(\d+\.?\d*[_"]\s*\d+\/\d+\s+\d+\.?\d*[^:]*(?:FBH|TS|NBS)[^:]*)/gi,
  };

  const extracted = [];

  for (const [key, pattern] of Object.entries(summaryPatterns)) {
    const matches = [...text.matchAll(pattern)];
    if (matches.length > 0) {
      const values = matches.map(m => m[1] || m[0]).filter(Boolean);
      if (values.length > 0) {
        // Clean and format values
        const cleanValues = values.map(v => v.trim()).slice(0, 3); // Limit to avoid overflow
        extracted.push(`${key.toUpperCase()}: ${cleanValues.join(", ")}`);
      }
    }
  }

  if (extracted.length >= 2) {
    // Lower threshold for more summaries
    return `DRILLING REPORT STRUCTURED DATA: ${extracted.join(" | ")}`;
  }

  return null;
}

/**
 * Enhanced fallback simple text splitting with drilling context
 */
function enhancedSimpleTextSplit(text, chunkSize, chunkOverlap) {
  const chunks = [];
  let start = 0;

  while (start < text.length) {
    let end = start + chunkSize;

    // Use drilling-aware breaking points
    if (end < text.length) {
      const breakPoints = [
        "\n\nMotor data",
        "\n\nBit Data",
        "\n\nDrilling Parameters",
        "\n\nBHA Details",
        "\n\n",
        "\n",
        ". ",
        "! ",
        "? ",
        " ",
      ];

      for (const breakPoint of breakPoints) {
        const lastBreak = text.lastIndexOf(breakPoint, end);
        if (lastBreak > start + chunkSize * 0.4) {
          end = lastBreak + breakPoint.length;
          break;
        }
      }
    }

    const chunk = text.substring(start, end).trim();
    if (chunk.length > 0) {
      chunks.push(enhanceChunkWithDrillingContext(chunk));
    }

    start = end - chunkOverlap;
  }

  console.log(
    `✅ Enhanced simple chunking complete: ${chunks.length} drilling-aware chunks created`
  );
  return chunks.length > 0 ? chunks : [enhanceChunkWithDrillingContext(text)];
}

module.exports = chunkText;
