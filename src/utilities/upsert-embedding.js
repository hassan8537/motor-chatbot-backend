const qdrantClient = require("../config/qdrant");

/**
 * Enhanced upserts embedding vectors into a Qdrant collection with drilling-specific optimizations
 * @param {Object} params
 * @param {string} params.collectionName - Name of the Qdrant collection.
 * @param {Array<{id: string | number, vector: number[], payload?: Object}>} params.points
 *        - Points to insert. Each must include an id, vector, and optional payload.
 * @param {boolean} [params.wait=true] - Whether to wait for operation completion.
 * @param {boolean} [params.enhancePayload=true] - Add drilling-specific payload enhancements.
 * @param {number} [params.batchSize=100] - Batch size for large upserts.
 * @param {number} [params.retries=3] - Number of retry attempts on failure.
 * @param {boolean} [params.validate=true] - Validate points before upserting.
 * @returns {Promise<Object>} - Enhanced Qdrant response with operation details.
 */
async function upsertEmbedding({
  collectionName,
  points,
  wait = true,
  enhancePayload = true,
  batchSize = 100,
  retries = 3,
  validate = true,
}) {
  // Input validation
  if (!collectionName || typeof collectionName !== "string") {
    throw new Error("❌ 'collectionName' must be a non-empty string.");
  }

  if (!Array.isArray(points) || points.length === 0) {
    throw new Error("❌ 'points' must be a non-empty array.");
  }

  console.log(
    `📤 Starting enhanced upsert for ${points.length} points to '${collectionName}'`
  );

  try {
    // Validate points if enabled
    if (validate) {
      validatePoints(points);
    }

    // Enhance payloads with drilling-specific metadata if enabled
    const processedPoints = enhancePayload
      ? points.map(point => enhancePointPayload(point))
      : points;

    // Process in batches if necessary
    if (processedPoints.length > batchSize) {
      return await batchUpsert({
        collectionName,
        points: processedPoints,
        wait,
        batchSize,
        retries,
      });
    }

    // Single batch upsert
    return await performUpsert({
      collectionName,
      points: processedPoints,
      wait,
      retries,
    });
  } catch (error) {
    console.error(`❌ Upsert operation failed: ${error.message}`);
    throw error;
  }
}

/**
 * Validate points structure and content
 */
function validatePoints(points) {
  console.log(`✅ Validating ${points.length} points...`);

  for (let i = 0; i < points.length; i++) {
    const point = points[i];

    // Check required fields
    if (!point.id) {
      throw new Error(`❌ Point ${i}: Missing required 'id' field`);
    }

    if (!Array.isArray(point.vector) || point.vector.length === 0) {
      throw new Error(`❌ Point ${i}: 'vector' must be a non-empty array`);
    }

    // Validate vector dimensions and values
    if (point.vector.some(val => !Number.isFinite(val))) {
      throw new Error(
        `❌ Point ${i}: Vector contains invalid values (NaN or Infinity)`
      );
    }

    // Check vector dimension consistency (assuming first point sets the standard)
    if (i === 0) {
      console.log(`📊 Vector dimension: ${point.vector.length}`);
    } else if (point.vector.length !== points[0].vector.length) {
      throw new Error(
        `❌ Point ${i}: Vector dimension mismatch (${point.vector.length} vs ${points[0].vector.length})`
      );
    }

    // Validate payload if present
    if (point.payload && typeof point.payload !== "object") {
      throw new Error(`❌ Point ${i}: 'payload' must be an object`);
    }
  }

  console.log(`✅ All ${points.length} points validated successfully`);
}

/**
 * Enhance point payload with drilling-specific metadata and search optimization
 */
function enhancePointPayload(point) {
  const originalPayload = point.payload || {};
  const content = originalPayload.content || "";

  // Extract drilling-specific metadata
  const drillingMetadata = extractDrillingMetadata(content);

  // Add search optimization fields
  const searchOptimization = createSearchOptimizationFields(
    content,
    originalPayload
  );

  // Create enhanced payload
  const enhancedPayload = {
    ...originalPayload,

    // Original content preserved
    content,

    // Drilling-specific metadata
    ...drillingMetadata,

    // Search optimization
    ...searchOptimization,

    // Processing metadata
    processingInfo: {
      enhanced: true,
      enhancedAt: new Date().toISOString(),
      originalPayloadKeys: Object.keys(originalPayload),
      contentLength: content.length,
      hasStructuredData: content.includes("STRUCTURED DATA"),
      hasTechnicalMetrics: /\d+\.?\d*\s*(?:klbs|rpm|gpm|psi|usft\/hr)/i.test(
        content
      ),
    },
  };

  return {
    ...point,
    payload: enhancedPayload,
  };
}

/**
 * Extract drilling-specific metadata from content
 */
function extractDrillingMetadata(content) {
  const metadata = {};

  // Hole size detection
  const holeSizeMatch = content.match(/(\d+\.?\d*)["\s]*(?:hole|section)/i);
  if (holeSizeMatch) {
    metadata.holeSize = parseFloat(holeSizeMatch[1]);
    metadata.holeSizeCategory = categorizeHoleSize(metadata.holeSize);
  }

  // Motor specifications
  const motorMakeMatch = content.match(/motor_make[:\s]*([A-Za-z\s-]+)/i);
  if (motorMakeMatch) {
    metadata.motorMake = motorMakeMatch[1].trim();
  }

  const statorVendorMatch = content.match(/stator_vendor[:\s]*([A-Za-z\s-]+)/i);
  if (statorVendorMatch) {
    metadata.statorVendor = statorVendorMatch[1].trim();
  }

  const statorFitMatch = content.match(/stator_fit[:\s]*([+-]?\d+\.?\d*)/i);
  if (statorFitMatch) {
    metadata.statorFit = parseFloat(statorFitMatch[1]);
  }

  // Bit specifications
  const bitModelMatch = content.match(/equipment_model_([A-Za-z0-9\-_]+)/i);
  if (bitModelMatch) {
    metadata.bitModel = bitModelMatch[1];
  }

  const tfaMatch = content.match(/TFA[:\s]*(\d+\.?\d*)/i);
  if (tfaMatch) {
    metadata.tfa = parseFloat(tfaMatch[1]);
  }

  // Performance metrics
  const avgROPMatch = content.match(
    /average_rate_of_penetration[:\s]*(\d+\.?\d*)/i
  );
  if (avgROPMatch) {
    metadata.avgROP = parseFloat(avgROPMatch[1]);
    metadata.ropCategory = categorizeROP(metadata.avgROP);
  }

  const wobMatch = content.match(/weight_on_bit[:\s]*(\d+\.?\d*)/i);
  if (wobMatch) {
    metadata.wob = parseFloat(wobMatch[1]);
  }

  const totalDrilledMatch = content.match(/total_drilled[:\s]*(\d+\.?\d*)/i);
  if (totalDrilledMatch) {
    metadata.totalDrilled = parseFloat(totalDrilledMatch[1]);
  }

  // Operational data
  const drillingHoursMatch = content.match(/drilling_hours[:\s]*(\d+\.?\d*)/i);
  if (drillingHoursMatch) {
    metadata.drillingHours = parseFloat(drillingHoursMatch[1]);
  }

  const circHoursMatch = content.match(/circulation_hours[:\s]*(\d+\.?\d*)/i);
  if (circHoursMatch) {
    metadata.circulationHours = parseFloat(circHoursMatch[1]);
  }

  return metadata;
}

/**
 * Create search optimization fields
 */
function createSearchOptimizationFields(content, originalPayload) {
  const optimization = {};

  // Content type classification
  optimization.contentType = classifyContentType(content);

  // Technical density score (helps with relevance ranking)
  optimization.technicalDensity = calculateTechnicalDensity(content);

  // Searchable keywords (extracted from content)
  optimization.searchableKeywords = extractSearchableKeywords(content);

  // Numerical data indicators
  optimization.hasNumericalData = /\d+\.?\d*/.test(content);
  optimization.numericalDataCount = (content.match(/\d+\.?\d*/g) || []).length;

  // Document section type (from original payload or inferred)
  optimization.documentSection =
    originalPayload.sectionType || inferDocumentSection(content);

  // Data completeness score
  optimization.dataCompletenessScore = calculateDataCompleteness(content);

  return optimization;
}

/**
 * Classify content type for better search targeting
 */
function classifyContentType(content) {
  const classifications = [
    {
      type: "MOTOR_STATOR_SPECS",
      patterns: [/motor_make|stator_vendor|stator_fit/i],
    },
    {
      type: "BIT_SPECIFICATIONS",
      patterns: [/equipment_model|total_flow_area|bit.*configuration/i],
    },
    {
      type: "PERFORMANCE_METRICS",
      patterns: [/rate_of_penetration|weight_on_bit|differential_pressure/i],
    },
    {
      type: "OPERATIONAL_DATA",
      patterns: [/drilling_hours|circulation_hours|total_drilled/i],
    },
    {
      type: "BHA_ASSEMBLY",
      patterns: [/bottom_hole_assembly|drill_collar|float_sub/i],
    },
    { type: "STRUCTURED_SUMMARY", patterns: [/STRUCTURED DATA|METRICS:/i] },
  ];

  for (const { type, patterns } of classifications) {
    if (patterns.some(pattern => pattern.test(content))) {
      return type;
    }
  }

  return "GENERAL_DRILLING";
}

/**
 * Calculate technical density score
 */
function calculateTechnicalDensity(content) {
  const technicalIndicators = [
    /\d+\.?\d*\s*(?:klbs|rpm|gpm|psi|usft\/hr|degf)/gi,
    /motor_make|stator_vendor|equipment_model/gi,
    /rate_of_penetration|weight_on_bit|differential_pressure/gi,
    /drilling_hours|circulation_hours|total_drilled/gi,
  ];

  let totalMatches = 0;
  technicalIndicators.forEach(pattern => {
    const matches = content.match(pattern);
    if (matches) totalMatches += matches.length;
  });

  // Normalize by content length (technical terms per 100 characters)
  return Math.min((totalMatches / content.length) * 100, 10); // Cap at 10
}

/**
 * Extract searchable keywords for enhanced matching
 */
function extractSearchableKeywords(content) {
  const keywords = new Set();

  // Extract technical terms
  const technicalTerms = content.match(
    /(?:motor_make|stator_vendor|equipment_model|rate_of_penetration|weight_on_bit)[_\s]*[A-Za-z0-9\-]+/gi
  );
  if (technicalTerms) {
    technicalTerms.forEach(term => keywords.add(term.toLowerCase()));
  }

  // Extract numerical values with units
  const measurements = content.match(
    /\d+\.?\d*\s*(?:klbs|rpm|gpm|psi|usft\/hr|degf)/gi
  );
  if (measurements) {
    measurements.forEach(measurement =>
      keywords.add(measurement.toLowerCase())
    );
  }

  return Array.from(keywords).slice(0, 20); // Limit to 20 keywords
}

/**
 * Infer document section from content
 */
function inferDocumentSection(content) {
  if (content.includes("MOTOR & STATOR SPECIFICATIONS")) return "motor_data";
  if (content.includes("BIT CONFIGURATION & SPECS")) return "bit_data";
  if (content.includes("DRILLING PERFORMANCE METRICS"))
    return "performance_data";
  if (content.includes("BHA ASSEMBLY DETAILS")) return "bha_details";
  if (content.includes("OPERATIONAL DATA")) return "operational_data";
  return "general";
}

/**
 * Calculate data completeness score
 */
function calculateDataCompleteness(content) {
  const expectedFields = [
    /motor_make|stator_vendor/i,
    /rate_of_penetration|weight_on_bit/i,
    /drilling_hours|circulation_hours/i,
    /\d+\.?\d*\s*(?:klbs|rpm|gpm)/i,
  ];

  const presentFields = expectedFields.filter(pattern =>
    pattern.test(content)
  ).length;
  return (presentFields / expectedFields.length) * 100; // Percentage
}

/**
 * Categorize hole size
 */
function categorizeHoleSize(size) {
  if (size >= 12) return "LARGE"; // 12.25" etc.
  if (size >= 8) return "MEDIUM"; // 8.5", 9.875" etc.
  if (size >= 6) return "SMALL"; // 6", 6.75" etc.
  return "UNKNOWN";
}

/**
 * Categorize ROP performance
 */
function categorizeROP(rop) {
  if (rop >= 100) return "HIGH";
  if (rop >= 50) return "MEDIUM";
  if (rop >= 20) return "LOW";
  return "VERY_LOW";
}

/**
 * Perform single upsert operation with retry logic
 */
async function performUpsert({ collectionName, points, wait, retries }) {
  for (let attempt = 1; attempt <= retries + 1; attempt++) {
    try {
      console.log(`📤 Upserting ${points.length} points (attempt ${attempt})`);

      const startTime = Date.now();

      const response = await qdrantClient.upsert(collectionName, {
        wait,
        points: points.map(({ id, vector, payload }) => ({
          id,
          vector,
          payload: payload || {},
        })),
      });

      const duration = Date.now() - startTime;

      console.log(
        `✅ Successfully upserted ${points.length} point(s) to '${collectionName}' in ${duration}ms`
      );

      return {
        ...response,
        operationInfo: {
          pointsProcessed: points.length,
          processingTimeMs: duration,
          averageTimePerPoint: duration / points.length,
          enhanced: true,
          attempt,
        },
      };
    } catch (error) {
      const isLastAttempt = attempt === retries + 1;

      if (isLastAttempt) {
        throw new Error(
          `Upsert failed after ${retries + 1} attempts: ${error.message}`
        );
      }

      console.warn(
        `⚠️ Upsert attempt ${attempt} failed, retrying... Error: ${error.message}`
      );

      // Exponential backoff
      const delay = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}

/**
 * Handle large batch upserts by splitting into smaller batches
 */
async function batchUpsert({
  collectionName,
  points,
  wait,
  batchSize,
  retries,
}) {
  console.log(
    `📦 Processing ${points.length} points in batches of ${batchSize}`
  );

  const batches = [];
  for (let i = 0; i < points.length; i += batchSize) {
    batches.push(points.slice(i, i + batchSize));
  }

  const results = [];
  let totalProcessed = 0;
  const startTime = Date.now();

  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    console.log(
      `📤 Processing batch ${i + 1}/${batches.length} (${batch.length} points)`
    );

    try {
      const batchResult = await performUpsert({
        collectionName,
        points: batch,
        wait,
        retries,
      });

      results.push(batchResult);
      totalProcessed += batch.length;

      console.log(
        `✅ Batch ${i + 1} completed. Progress: ${totalProcessed}/${
          points.length
        }`
      );

      // Small delay between batches to prevent overwhelming the server
      if (i < batches.length - 1) {
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    } catch (error) {
      console.error(`❌ Batch ${i + 1} failed: ${error.message}`);
      throw new Error(
        `Batch upsert failed at batch ${i + 1}: ${error.message}`
      );
    }
  }

  const totalDuration = Date.now() - startTime;

  console.log(
    `🎉 Batch upsert completed: ${totalProcessed} points in ${totalDuration}ms`
  );

  return {
    success: true,
    batchResults: results,
    operationInfo: {
      totalPointsProcessed: totalProcessed,
      totalBatches: batches.length,
      totalProcessingTimeMs: totalDuration,
      averageTimePerBatch: totalDuration / batches.length,
      averageTimePerPoint: totalDuration / totalProcessed,
      enhanced: true,
    },
  };
}

/**
 * Utility function to create optimized collection if it doesn't exist
 */
async function ensureCollection(collectionName, vectorDimension = 1536) {
  try {
    // Check if collection exists
    const collections = await qdrantClient.getCollections();
    const exists = collections.collections.some(c => c.name === collectionName);

    if (!exists) {
      console.log(`🏗️ Creating optimized collection: ${collectionName}`);

      await qdrantClient.createCollection(collectionName, {
        vectors: {
          size: vectorDimension,
          distance: "Cosine",
        },
        // Optimized settings for drilling data
        optimizers_config: {
          default_segment_number: 2,
          max_segment_size: 20000,
          memmap_threshold: 50000,
          indexing_threshold: 10000,
          flush_interval_sec: 30,
        },
        // Create indexes for common drilling metadata fields
        payload_schema: {
          holeSize: { type: "float", index: true },
          motorMake: { type: "keyword", index: true },
          statorVendor: { type: "keyword", index: true },
          contentType: { type: "keyword", index: true },
          documentSection: { type: "keyword", index: true },
        },
      });

      console.log(
        `✅ Collection '${collectionName}' created with drilling optimizations`
      );
    }

    return true;
  } catch (error) {
    console.error(`❌ Failed to ensure collection: ${error.message}`);
    throw error;
  }
}

// Export main function and utilities
module.exports = upsertEmbedding;
module.exports.ensureCollection = ensureCollection;
