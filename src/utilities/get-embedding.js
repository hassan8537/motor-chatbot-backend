const openaiClient = require("../config/openai");

/**
 * Enhanced embedding generation optimized for drilling reports and technical queries
 * @param {string | string[]} inputText - Text string or array of text strings.
 * @param {Object} [options]
 * @param {string} [options.model="text-embedding-3-small"] - OpenAI embedding model.
 * @param {number} [options.maxLength=8191] - Truncation limit per input.
 * @param {boolean} [options.enhanceDrillingContext=true] - Add drilling-specific context enhancement.
 * @param {number} [options.retries=2] - Number of retry attempts on failure.
 * @returns {Promise<number[] | number[][]>} - Single or batch embedding(s).
 */
async function getEmbedding(inputText, options = {}) {
  const {
    model = "text-embedding-3-small",
    maxLength = 8191,
    enhanceDrillingContext = true,
    retries = 2,
  } = options;

  const inputArray = Array.isArray(inputText) ? inputText : [inputText];

  // Enhanced preprocessing for drilling reports
  const processedInput = inputArray.map(text =>
    enhanceDrillingContext
      ? enhanceTextForDrillingEmbedding(text, maxLength)
      : sanitizeText(text, maxLength)
  );

  // Retry logic for robust embedding generation
  for (let attempt = 1; attempt <= retries + 1; attempt++) {
    try {
      console.log(
        `🧠 Generating embeddings for ${processedInput.length} chunk(s) (attempt ${attempt})`
      );

      const response = await openaiClient.embeddings.create({
        model,
        input: processedInput,
      });

      const embeddings = response.data.map(obj => obj.embedding);

      // Validate embeddings
      validateEmbeddings(embeddings, processedInput);

      console.log(
        `✅ Successfully generated ${embeddings.length} embedding(s)`
      );
      return Array.isArray(inputText) ? embeddings : embeddings[0];
    } catch (error) {
      const isLastAttempt = attempt === retries + 1;

      if (isLastAttempt) {
        console.error(`❌ Final embedding attempt failed: ${error.message}`);
        throw new Error(
          `Embedding generation failed after ${retries + 1} attempts: ${
            error.message
          }`
        );
      }

      console.warn(
        `⚠️ Embedding attempt ${attempt} failed, retrying... Error: ${error.message}`
      );

      // Exponential backoff delay
      const delay = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}

/**
 * Enhanced text preprocessing specifically for drilling reports
 */
function enhanceTextForDrillingEmbedding(text, maxLength) {
  if (!text || typeof text !== "string") {
    return "";
  }

  let enhanced = text.trim();

  // Step 1: Normalize drilling terminology for better embedding consistency
  enhanced = normalizeDrillingTerminology(enhanced);

  // Step 2: Add semantic context for better query matching
  enhanced = addSemanticContext(enhanced);

  // Step 3: Preserve critical technical relationships
  enhanced = preserveTechnicalRelationships(enhanced);

  // Step 4: Clean and truncate while preserving important data
  enhanced = smartTruncate(enhanced, maxLength);

  return enhanced;
}

/**
 * Normalize drilling terminology for consistent embeddings
 */
function normalizeDrillingTerminology(text) {
  const terminology = {
    // Motor/Stator standardization
    "stator fit": "stator_fit",
    "stator vendor": "stator_vendor",
    "motor make": "motor_make",
    "motor configuration": "motor_config",

    // Performance metrics
    "avg rop": "average_rate_of_penetration",
    "slide rop": "slide_rate_of_penetration",
    "rot rop": "rotary_rate_of_penetration",
    wob: "weight_on_bit",
    "diff press": "differential_pressure",
    "max diffp": "maximum_differential_pressure",

    // BHA components
    "float sub": "float_sub_component",
    ubho: "under_balanced_hydraulic_oscillator",
    nmdc: "non_magnetic_drill_collar",
    "shock sub": "shock_sub_component",

    // Operational data
    "circ hrs": "circulation_hours",
    "drill hrs": "drilling_hours",
    "pickup weight": "pickup_weight_measurement",
    "total drilled": "total_drilled_footage",

    // Technical specifications
    tfa: "total_flow_area",
    "fishneck od": "fishneck_outer_diameter",
    "hole size": "hole_diameter_size",
  };

  let normalized = text;
  for (const [term, replacement] of Object.entries(terminology)) {
    const regex = new RegExp(term, "gi");
    normalized = normalized.replace(regex, replacement);
  }

  return normalized;
}

/**
 * Add semantic context to improve query matching
 */
function addSemanticContext(text) {
  // Detect content type and add relevant context
  const contentTypes = [
    {
      patterns: [/stator_fit|stator_vendor|motor_make|lobes|stages/i],
      context: "drilling_motor_specifications_equipment",
    },
    {
      patterns: [/bit.*model|tfa|pdc|baker.*hughes|ulterra/i],
      context: "drilling_bit_specifications_equipment",
    },
    {
      patterns: [
        /weight_on_bit|rate_of_penetration|differential_pressure|torque/i,
      ],
      context: "drilling_performance_metrics_data",
    },
    {
      patterns: [/circulation_hours|drilling_hours|total_drilled|footage/i],
      context: "drilling_operational_measurements",
    },
    {
      patterns: [/bha.*details|assembly|float_sub|drill_collar/i],
      context: "bottom_hole_assembly_components",
    },
  ];

  for (const { patterns, context } of contentTypes) {
    if (patterns.some(pattern => pattern.test(text))) {
      return `${context} ${text}`;
    }
  }

  return `drilling_report_data ${text}`;
}

/**
 * Preserve critical technical relationships during processing
 */
function preserveTechnicalRelationships(text) {
  // Preserve motor configurations like "6/7" or "7/8"
  text = text.replace(/(\d+)\/(\d+)(?=\s)/g, "$1_over_$2_configuration");

  // Preserve measurements with units
  text = text.replace(
    /(\d+\.?\d*)\s*(klbs|rpm|gpm|psi|usft|degf)/gi,
    "$1_$2_measurement"
  );

  // Preserve technical codes
  text = text.replace(/([A-Z]{2,}[-_]\d+[-_][A-Z0-9]+)/g, "technical_code_$1");

  // Preserve bit/motor model numbers
  text = text.replace(/(DD\d+|CF\d+|XP\d+|U\d+)/gi, "equipment_model_$1");

  return text;
}

/**
 * Smart truncation that preserves important technical data
 */
function smartTruncate(text, maxLength) {
  if (text.length <= maxLength) {
    return text;
  }

  // Try to truncate at natural boundaries while preserving key data
  const boundaries = [
    " | ", // Structured data separators
    "METRICS:", // Metric boundaries
    ". ", // Sentence boundaries
    " ", // Word boundaries
  ];

  for (const boundary of boundaries) {
    const lastBoundary = text.lastIndexOf(boundary, maxLength);
    if (lastBoundary > maxLength * 0.8) {
      // Don't truncate too aggressively
      return text.substring(0, lastBoundary + boundary.length).trim();
    }
  }

  // Fallback: hard truncate but add continuation indicator
  return text.substring(0, maxLength - 10).trim() + " [CONT...]";
}

/**
 * Basic text sanitization for non-enhanced mode
 */
function sanitizeText(text, maxLength) {
  if (!text || typeof text !== "string") {
    return "";
  }

  return text.replace(/\s+/g, " ").trim().slice(0, maxLength);
}

/**
 * Validate generated embeddings
 */
function validateEmbeddings(embeddings, originalTexts) {
  if (!Array.isArray(embeddings) || embeddings.length === 0) {
    throw new Error("No embeddings generated");
  }

  if (embeddings.length !== originalTexts.length) {
    throw new Error(
      `Embedding count mismatch: got ${embeddings.length}, expected ${originalTexts.length}`
    );
  }

  for (let i = 0; i < embeddings.length; i++) {
    const embedding = embeddings[i];

    if (!Array.isArray(embedding)) {
      throw new Error(`Invalid embedding at index ${i}: not an array`);
    }

    if (embedding.length === 0) {
      throw new Error(`Empty embedding at index ${i}`);
    }

    // Check for valid numeric values
    if (embedding.some(val => !Number.isFinite(val))) {
      throw new Error(
        `Invalid embedding values at index ${i}: contains non-finite numbers`
      );
    }
  }

  console.log(
    `✅ Embeddings validated: ${embeddings.length} valid embeddings of dimension ${embeddings[0].length}`
  );
}

/**
 * Batch embedding generation with optimized processing
 */
async function getBatchEmbeddings(textArray, options = {}) {
  const batchSize = options.batchSize || 100;
  const results = [];

  console.log(
    `🧠 Processing ${textArray.length} texts in batches of ${batchSize}`
  );

  for (let i = 0; i < textArray.length; i += batchSize) {
    const batch = textArray.slice(i, i + batchSize);
    console.log(
      `📦 Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(
        textArray.length / batchSize
      )}`
    );

    const batchEmbeddings = await getEmbedding(batch, options);
    results.push(...batchEmbeddings);

    // Small delay between batches to respect rate limits
    if (i + batchSize < textArray.length) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }

  console.log(
    `✅ Batch processing complete: ${results.length} embeddings generated`
  );
  return results;
}

/**
 * Get embedding with caching support (for development/testing)
 */
async function getEmbeddingWithCache(inputText, options = {}) {
  const cacheKey = generateCacheKey(inputText, options);

  // In production, you might want to implement Redis/memory caching here
  // For now, just pass through to regular embedding generation
  return getEmbedding(inputText, options);
}

/**
 * Generate cache key for embedding requests
 */
function generateCacheKey(inputText, options) {
  const text = Array.isArray(inputText) ? inputText.join("|") : inputText;
  const optionsStr = JSON.stringify(options);

  // Simple hash function for cache key
  let hash = 0;
  const keyString = text + optionsStr;
  for (let i = 0; i < keyString.length; i++) {
    const char = keyString.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash = hash & hash; // Convert to 32-bit integer
  }

  return `embedding_${Math.abs(hash)}`;
}

// Export main function and utilities
module.exports = getEmbedding;
module.exports.getBatchEmbeddings = getBatchEmbeddings;
module.exports.getEmbeddingWithCache = getEmbeddingWithCache;
