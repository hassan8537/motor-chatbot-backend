const qdrantClient = require("../config/qdrant");

/**
 * Enhanced semantic vector search optimized for drilling report queries
 * @param {Object} params
 * @param {string} params.collectionName - Name of the Qdrant collection.
 * @param {number[]} params.embedding - Embedding vector to search by.
 * @param {number} [params.limit=20] - Number of top results to return (increased for better aggregation).
 * @param {number} [params.scoreThreshold=0.6] - Minimum similarity score threshold.
 * @param {Object} [params.filter] - Qdrant filter conditions for targeted search.
 * @param {boolean} [params.enableReranking=true] - Apply drilling-specific result reranking.
 * @param {string} [params.queryType] - Type of query for optimization ('aggregation', 'specific', 'comparison').
 * @param {number} [params.retries=2] - Number of retry attempts on failure.
 * @returns {Promise<Array>} - Array of enhanced result objects with payload, score, and relevance info.
 */
async function searchResults({
  collectionName,
  embedding,
  limit = 4,
  scoreThreshold = 0.6,
  filter = null,
  enableReranking = true,
  queryType = "general",
  retries = 2,
}) {
  // Validate inputs
  if (!collectionName || typeof collectionName !== "string") {
    throw new Error("Collection name must be a non-empty string");
  }

  if (!Array.isArray(embedding) || embedding.length === 0) {
    throw new Error("Embedding must be a non-empty array");
  }

  // Adjust search parameters based on query type
  const searchParams = optimizeSearchParams({
    limit,
    scoreThreshold,
    queryType,
  });

  for (let attempt = 1; attempt <= retries + 1; attempt++) {
    try {
      console.log(
        `🔍 Searching Qdrant collection '${collectionName}' (attempt ${attempt})`
      );
      console.log(
        `📊 Parameters: limit=${searchParams.limit}, threshold=${searchParams.scoreThreshold}, type=${queryType}`
      );

      const searchOptions = {
        vector: embedding,
        limit: searchParams.limit,
        with_payload: true,
        score_threshold: searchParams.scoreThreshold,
      };

      // Add filter if provided
      if (filter) {
        searchOptions.filter = filter;
        console.log(`🎯 Applying filter:`, filter);
      }

      const results = await qdrantClient.search(collectionName, searchOptions);

      if (!results || results.length === 0) {
        console.log(
          `⚠️ No results found above threshold ${searchParams.scoreThreshold}`
        );

        // Retry with lower threshold for aggregation queries
        if (queryType === "aggregation" && searchParams.scoreThreshold > 0.4) {
          console.log(`🔄 Retrying with lower threshold for aggregation query`);
          return searchResults({
            collectionName,
            embedding,
            limit,
            scoreThreshold: 0.4,
            filter,
            enableReranking,
            queryType,
            retries: 0, // Prevent infinite recursion
          });
        }

        return [];
      }

      console.log(
        `✅ Qdrant returned ${results.length} result(s) above threshold`
      );

      // Process and enhance results
      let processedResults = results.map((r, idx) => ({
        id: r.id,
        score: r.score,
        payload: r.payload || {},
        rank: idx + 1,
        relevanceCategory: categorizeRelevance(r.score),
      }));

      // Apply drilling-specific reranking if enabled
      if (enableReranking) {
        processedResults = applyDrillingReranking(processedResults, queryType);
      }

      // Add aggregation helpers for statistical queries
      if (queryType === "aggregation") {
        processedResults = enhanceForAggregation(processedResults);
      }

      // Log result summary
      logSearchSummary(processedResults, queryType);

      return processedResults;
    } catch (error) {
      const isLastAttempt = attempt === retries + 1;

      if (isLastAttempt) {
        console.error(`❌ Final search attempt failed: ${error.message}`);
        throw new Error(
          `Qdrant search failed after ${retries + 1} attempts: ${error.message}`
        );
      }

      console.warn(
        `⚠️ Search attempt ${attempt} failed, retrying... Error: ${error.message}`
      );

      // Exponential backoff delay
      const delay = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}

/**
 * Optimize search parameters based on query type
 */
function optimizeSearchParams({ limit, scoreThreshold, queryType }) {
  const optimizations = {
    aggregation: {
      limit: Math.max(limit, 50), // Need more results for statistical analysis
      scoreThreshold: Math.min(scoreThreshold, 0.5), // Lower threshold for broader data collection
    },
    specific: {
      limit: Math.min(limit, 15), // Fewer, higher quality results
      scoreThreshold: Math.max(scoreThreshold, 0.7), // Higher threshold for precision
    },
    comparison: {
      limit: Math.max(limit, 30), // More results for comparative analysis
      scoreThreshold: 0.6, // Balanced threshold
    },
    general: {
      limit,
      scoreThreshold,
    },
  };

  return optimizations[queryType] || optimizations.general;
}

/**
 * Categorize result relevance based on similarity score
 */
function categorizeRelevance(score) {
  if (score >= 0.9) return "HIGHLY_RELEVANT";
  if (score >= 0.8) return "VERY_RELEVANT";
  if (score >= 0.7) return "RELEVANT";
  if (score >= 0.6) return "SOMEWHAT_RELEVANT";
  return "MARGINALLY_RELEVANT";
}

/**
 * Apply drilling-specific reranking to improve result quality
 */
function applyDrillingReranking(results, queryType) {
  console.log(`🔄 Applying drilling-specific reranking for ${queryType} query`);

  return results
    .map(result => {
      let boostScore = 0;
      const content = result.payload.content || "";
      const sectionType = result.payload.sectionType || "";

      // Boost based on content type relevance
      const contentBoosts = {
        // High-value technical content
        motor_specifications: 0.15,
        bit_specifications: 0.15,
        performance_metrics: 0.12,
        bha_assembly: 0.1,
        operational_performance: 0.08,
        general: 0.0,
      };

      // Apply section type boost
      for (const [section, boost] of Object.entries(contentBoosts)) {
        if (
          sectionType.includes(section) ||
          content.toLowerCase().includes(section)
        ) {
          boostScore += boost;
          break;
        }
      }

      // Boost for structured data summaries (contain multiple metrics)
      if (content.includes("STRUCTURED DATA") || content.includes("METRICS:")) {
        boostScore += 0.1;
      }

      // Boost for technical specifications with numerical data
      const technicalPatterns = [
        /\d+\.?\d*\s*(?:klbs|rpm|gpm|psi|usft\/hr)/i,
        /stator_fit|motor_make|avg_rop|total_drilled/i,
        /TFA|WOB|ROP|BHA/i,
      ];

      const technicalMatches = technicalPatterns.reduce((count, pattern) => {
        return count + (pattern.test(content) ? 1 : 0);
      }, 0);

      boostScore += Math.min(technicalMatches * 0.03, 0.12); // Max 0.12 boost

      // Query type specific boosts
      if (queryType === "aggregation") {
        // Boost results with multiple data points
        const dataPointCount = (content.match(/\d+\.?\d*/g) || []).length;
        boostScore += Math.min(dataPointCount * 0.01, 0.08);
      }

      // Apply boost to score (capped to not exceed 1.0)
      const boostedScore = Math.min(result.score + boostScore, 1.0);

      return {
        ...result,
        originalScore: result.score,
        score: boostedScore,
        boostApplied: boostScore,
        technicalRelevance: technicalMatches,
      };
    })
    .sort((a, b) => b.score - a.score); // Re-sort by boosted scores
}

/**
 * Enhance results for aggregation queries
 */
function enhanceForAggregation(results) {
  console.log(`📊 Enhancing ${results.length} results for aggregation`);

  return results.map(result => {
    const content = result.payload.content || "";

    // Extract numerical values for aggregation
    const numericalValues = extractNumericalValues(content);

    // Identify data categories
    const dataCategories = identifyDataCategories(content);

    // Extract key identifiers (motor makes, bit models, etc.)
    const identifiers = extractIdentifiers(content);

    return {
      ...result,
      aggregationData: {
        numericalValues,
        dataCategories,
        identifiers,
        hasStructuredData: content.includes("STRUCTURED DATA"),
        metricCount: numericalValues.length,
      },
    };
  });
}

/**
 * Extract numerical values from content for aggregation
 */
function extractNumericalValues(content) {
  const patterns = {
    statorFit: /STATOR_FIT[:\s]*([+-]?\d+\.?\d*)/gi,
    avgROP: /AVG_ROP[:\s]*(\d+\.?\d*)/gi,
    wob: /WOB[:\s]*(\d+\.?\d*)/gi,
    totalDrilled: /TOTAL_DRILLED[:\s]*(\d+\.?\d*)/gi,
    drillingHours: /DRILL_HRS[:\s]*(\d+\.?\d*)/gi,
    circHours: /CIRC_HRS[:\s]*(\d+\.?\d*)/gi,
    diffPress: /DIFF_PRESS[:\s]*(\d+\.?\d*)/gi,
    slidePercent: /SLIDE_PERCENT[:\s]*(\d+\.?\d*)/gi,
    pickupWeight: /PICKUP_WT[:\s]*(\d+\.?\d*)/gi,
  };

  const values = {};
  for (const [key, pattern] of Object.entries(patterns)) {
    const matches = [...content.matchAll(pattern)];
    if (matches.length > 0) {
      values[key] = matches.map(m => parseFloat(m[1])).filter(n => !isNaN(n));
    }
  }

  return values;
}

/**
 * Identify data categories in content
 */
function identifyDataCategories(content) {
  const categories = [];

  const categoryPatterns = {
    motorSpecs: /motor_make|stator_vendor|stator_fit/i,
    bitSpecs: /bit.*model|tfa|equipment_model/i,
    performance: /rate_of_penetration|weight_on_bit|differential_pressure/i,
    operational: /drilling_hours|circulation_hours|total_drilled/i,
    assembly: /bha.*assembly|drill_collar|float_sub/i,
  };

  for (const [category, pattern] of Object.entries(categoryPatterns)) {
    if (pattern.test(content)) {
      categories.push(category);
    }
  }

  return categories;
}

/**
 * Extract key identifiers (makes, models, etc.)
 */
function extractIdentifiers(content) {
  const identifiers = {};

  const patterns = {
    motorMake: /motor_make[:\s]*([A-Za-z\s]+)/gi,
    statorVendor: /stator_vendor[:\s]*([A-Za-z\s-]+)/gi,
    bitModel: /equipment_model_([A-Za-z0-9]+)/gi,
    holeSize: /(\d+\.?\d*)["\s]*(?:hole|section)/gi,
  };

  for (const [key, pattern] of Object.entries(patterns)) {
    const matches = [...content.matchAll(pattern)];
    if (matches.length > 0) {
      identifiers[key] = matches.map(m => m[1].trim()).filter(Boolean);
    }
  }

  return identifiers;
}

/**
 * Log comprehensive search result summary
 */
function logSearchSummary(results, queryType) {
  console.log(`📋 Search Summary (${queryType} query):`);
  console.log(`   Total results: ${results.length}`);

  if (results.length > 0) {
    const scoreRange = {
      min: Math.min(...results.map(r => r.score)),
      max: Math.max(...results.map(r => r.score)),
      avg: results.reduce((sum, r) => sum + r.score, 0) / results.length,
    };

    console.log(
      `   Score range: ${scoreRange.min.toFixed(3)} - ${scoreRange.max.toFixed(
        3
      )} (avg: ${scoreRange.avg.toFixed(3)})`
    );

    // Relevance distribution
    const relevanceDistribution = results.reduce((dist, r) => {
      dist[r.relevanceCategory] = (dist[r.relevanceCategory] || 0) + 1;
      return dist;
    }, {});

    console.log(`   Relevance distribution:`, relevanceDistribution);

    // Content type distribution
    const contentTypes = results.reduce((types, r) => {
      const sectionType = r.payload.sectionType || "unknown";
      types[sectionType] = (types[sectionType] || 0) + 1;
      return types;
    }, {});

    console.log(`   Content types:`, contentTypes);
  }
}

/**
 * Advanced filtering helper for complex queries
 */
function createAdvancedFilter(conditions) {
  const filter = { must: [] };

  // Add conditions based on different criteria
  if (conditions.holeSize) {
    filter.must.push({
      match: {
        key: "payload.holeSize",
        value: conditions.holeSize,
      },
    });
  }

  if (conditions.sectionType) {
    filter.must.push({
      match: {
        key: "payload.sectionType",
        value: conditions.sectionType,
      },
    });
  }

  if (conditions.dateRange) {
    filter.must.push({
      range: {
        key: "payload.createdAt",
        gte: conditions.dateRange.start,
        lte: conditions.dateRange.end,
      },
    });
  }

  return filter.must.length > 0 ? filter : null;
}

/**
 * Hybrid search combining vector similarity with keyword matching
 */
async function hybridSearch({
  collectionName,
  embedding,
  keywords = [],
  limit = 20,
  hybridWeight = 0.7, // Weight for vector search vs keyword search
}) {
  try {
    // Get vector search results
    const vectorResults = await searchResults({
      collectionName,
      embedding,
      limit: limit * 2, // Get more for hybrid ranking
      enableReranking: false, // We'll do custom ranking
    });

    if (keywords.length === 0) {
      return vectorResults.slice(0, limit);
    }

    // Apply keyword boosting
    const hybridResults = vectorResults.map(result => {
      const content = (result.payload.content || "").toLowerCase();
      let keywordScore = 0;

      keywords.forEach(keyword => {
        const keywordLower = keyword.toLowerCase();
        const matches = (content.match(new RegExp(keywordLower, "g")) || [])
          .length;
        keywordScore += matches * 0.1; // Each match adds 0.1
      });

      // Combine vector and keyword scores
      const hybridScore =
        result.score * hybridWeight + keywordScore * (1 - hybridWeight);

      return {
        ...result,
        hybridScore,
        keywordScore,
        vectorScore: result.score,
      };
    });

    // Sort by hybrid score and return top results
    return hybridResults
      .sort((a, b) => b.hybridScore - a.hybridScore)
      .slice(0, limit);
  } catch (error) {
    console.error("❌ Hybrid search failed:", error);
    throw error;
  }
}

// Export main function and utilities
module.exports = searchResults;
module.exports.createAdvancedFilter = createAdvancedFilter;
module.exports.hybridSearch = hybridSearch;
