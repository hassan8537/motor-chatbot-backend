const { PutCommand, BatchWriteCommand } = require("@aws-sdk/lib-dynamodb");
const { v4: uuidv4 } = require("uuid");
const { docClient } = require("../config/aws");

// Constants
const MAX_RETRY_ATTEMPTS = 3;
const RETRY_DELAY_MS = 1000;
const BATCH_SIZE = 25; // DynamoDB batch write limit

/**
 * Utility function for delays with exponential backoff
 * @param {number} ms - Base delay in milliseconds
 * @param {number} attempt - Current attempt number
 * @returns {Promise} - Promise that resolves after delay
 */
const delay = (ms, attempt = 1) => {
  return new Promise(resolve =>
    setTimeout(resolve, ms * Math.pow(2, attempt - 1))
  );
};

/**
 * Validates common parameters for database operations
 * @param {Object} params - Parameters to validate
 * @returns {Object} - Validation result
 */
const validateCommonParams = ({ userId, timestamp, tableName }) => {
  const errors = [];

  if (!userId?.trim()) {
    errors.push("UserId is required and cannot be empty");
  }

  if (!timestamp) {
    errors.push("Timestamp is required");
  } else {
    try {
      new Date(timestamp);
    } catch (error) {
      errors.push("Invalid timestamp format");
    }
  }

  if (!tableName?.trim()) {
    errors.push("TableName is required and cannot be empty");
  }

  return {
    isValid: errors.length === 0,
    errors,
  };
};

/**
 * Execute DynamoDB operation with retry logic
 * @param {Function} operation - DynamoDB operation to execute
 * @param {string} operationName - Name of operation for logging
 * @param {number} maxRetries - Maximum retry attempts
 * @returns {Promise<Object>} - Operation result
 */
const executeWithRetry = async (
  operation,
  operationName,
  maxRetries = MAX_RETRY_ATTEMPTS
) => {
  let lastError;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const result = await operation();

      if (attempt > 1) {
        console.log(`${operationName} succeeded on attempt ${attempt}`);
      }

      return { success: true, result };
    } catch (error) {
      lastError = error;
      console.error(
        `${operationName} attempt ${attempt} failed:`,
        error.message
      );

      // Don't retry on validation errors
      if (error.name === "ValidationException" || error.statusCode === 400) {
        break;
      }

      if (attempt < maxRetries) {
        await delay(RETRY_DELAY_MS, attempt);
      }
    }
  }

  return {
    success: false,
    error: lastError,
    message: `${operationName} failed after ${maxRetries} attempts: ${lastError.message}`,
  };
};

/**
 * Enhanced save query function optimized for drilling search service
 * @param {Object} payload - Enhanced data to save with drilling optimizations
 * @param {string} payload.userId - User identifier
 * @param {string} payload.queryText - User's query text
 * @param {string} payload.answer - AI response text
 * @param {string} payload.model - AI model used
 * @param {number} payload.temperature - Model temperature setting
 * @param {number} [payload.totalTokens] - Total tokens used
 * @param {string} payload.timestamp - ISO timestamp
 * @param {string} payload.tableName - DynamoDB table name
 * @param {Array} [payload.sources] - Array of source documents/chunks used
 * @param {Object} [payload.metrics] - Performance and processing metrics
 * @param {Object} [payload.metadata] - Additional metadata including drilling-specific fields
 * @returns {Promise<Object>} - Enhanced save result
 */
const saveQuery = async ({
  userId,
  queryText,
  model,
  temperature,
  totalTokens = null,
  answer,
  timestamp,
  tableName,
  sources = [],
  metrics = {},
  metadata = {},
}) => {
  // Enhanced validation for drilling queries
  const commonValidation = validateCommonParams({
    userId,
    timestamp,
    tableName,
  });
  if (!commonValidation.isValid) {
    return {
      success: false,
      error: new Error(
        `Drilling query validation failed: ${commonValidation.errors.join(
          ", "
        )}`
      ),
    };
  }

  const additionalErrors = [];
  if (!queryText?.trim()) additionalErrors.push("Query text is required");
  if (!answer?.trim()) additionalErrors.push("Answer is required");
  if (!model?.trim()) additionalErrors.push("Model is required");
  if (typeof temperature !== "number")
    additionalErrors.push("Temperature must be a number");

  if (additionalErrors.length > 0) {
    return {
      success: false,
      error: new Error(
        `Drilling query validation failed: ${additionalErrors.join(", ")}`
      ),
    };
  }

  const queryId = uuidv4();
  const PK = `QUERY#${queryId}`;
  const SK = `USER#${userId}#QUERY#${queryId}`;

  // Enhanced text statistics
  const queryWordCount = queryText.trim().split(/\s+/).length;
  const answerWordCount = answer.trim().split(/\s+/).length;

  // Analyze query characteristics
  const queryAnalysis = analyzeQueryCharacteristics(queryText);

  // Enhanced source validation and sanitization
  const sanitizedSources = Array.isArray(sources)
    ? sources.map((source, index) => ({
        FileName: source.FileName || source.fileName || `Document ${index + 1}`,
        ChunkIndex: source.ChunkIndex || source.chunkIndex || index,
        Score: source.Score || source.score || "0.00",
        ContentType: source.ContentType || source.contentType || "general",
        HasStructuredData:
          source.HasStructuredData || source.hasStructuredData || false,
        ...source, // Include any additional source properties
      }))
    : [];

  // Enhanced metrics validation and sanitization
  const sanitizedMetrics = {
    totalRequestTimeMs: metrics.totalRequestTimeMs || 0,
    cached: metrics.cached || false,
    embeddingFromCache: metrics.embeddingFromCache || false,
    resultsCount: metrics.resultsCount || 0,
    tokensUsed: metrics.tokensUsed || totalTokens || 0,
    // Drilling-specific metrics from enhanced search service
    processingVersion: metrics.processingVersion || "2.0-drilling-optimized",
    queryComplexity: metrics.queryComplexity || 1,
    technicalTermsFound: metrics.technicalTermsFound || 0,
    embeddingEnhancements: metrics.embeddingEnhancements || 0,
    ...metrics, // Include any additional metrics
  };

  // Enhanced metadata processing
  const enhancedMetadata = {
    // Query classification
    queryType: metadata.queryType || queryAnalysis.type,
    isDrillingQuery: metadata.isDrillingQuery || queryAnalysis.isDrilling,
    isAggregationQuery:
      metadata.isAggregationQuery || queryAnalysis.isAggregation,
    isComparisonQuery: metadata.isComparisonQuery || queryAnalysis.isComparison,

    // Technical analysis
    technicalTermsCount:
      metadata.technicalTermsCount || queryAnalysis.technicalTermsCount,
    complexityScore: metadata.complexityScore || queryAnalysis.complexityScore,

    // Processing metadata
    processingVersion: metadata.processingVersion || "2.0-drilling-optimized",
    searchOptimized: metadata.searchOptimized || true,

    // Response quality indicators
    hasStructuredSources: sanitizedSources.some(s => s.HasStructuredData),
    averageSourceScore: calculateAverageSourceScore(sanitizedSources),

    ...metadata, // Include any additional metadata
  };

  // Build enhanced item with drilling optimizations
  const item = {
    // Core fields
    PK,
    SK,
    EntityType: "Chat",
    QueryId: queryId,
    UserId: userId,
    Query: queryText.trim(),
    Answer: answer.trim(),
    Model: model,
    Temperature: temperature,
    TotalTokens: totalTokens,

    // Enhanced statistics
    QueryWordCount: queryWordCount,
    AnswerWordCount: answerWordCount,
    QueryLength: queryText.trim().length,
    AnswerLength: answer.trim().length,

    // Enhanced source information
    sources: sanitizedSources,
    SourcesCount: sanitizedSources.length,
    StructuredSourcesCount: sanitizedSources.filter(s => s.HasStructuredData)
      .length,

    // Enhanced metrics
    metrics: sanitizedMetrics,

    // Query classification and analysis
    QueryType: enhancedMetadata.queryType,
    IsDrillingQuery: enhancedMetadata.isDrillingQuery,
    IsAggregationQuery: enhancedMetadata.isAggregationQuery,
    IsComparisonQuery: enhancedMetadata.isComparisonQuery,

    // Technical analysis
    TechnicalTermsCount: enhancedMetadata.technicalTermsCount,
    ComplexityScore: enhancedMetadata.complexityScore,

    // Response quality metrics
    HasStructuredSources: enhancedMetadata.hasStructuredSources,
    AverageSourceScore: enhancedMetadata.averageSourceScore,

    // Processing metadata
    ProcessingVersion: enhancedMetadata.processingVersion,
    SearchOptimized: enhancedMetadata.searchOptimized,

    // Searchable fields for better discovery
    SearchableTerms: extractSearchableTerms(queryText, answer),

    // Timestamps and TTL
    CreatedAt: timestamp,
    UpdatedAt: timestamp,
    TTL:
      Math.floor(Date.now() / 1000) +
      (enhancedMetadata.isDrillingQuery ? 18 : 12) * 30 * 24 * 60 * 60, // 18 months for drilling queries, 12 for others

    // Additional metadata
    ...enhancedMetadata,
  };

  // Add GSI fields for drilling queries
  if (enhancedMetadata.isDrillingQuery) {
    item.GSI1PK = `DRILLING#${enhancedMetadata.queryType.toUpperCase()}`;
    item.GSI1SK = `USER#${userId}#${timestamp}`;
    item.GSI2PK = `USER#${userId}#DRILLING`;
    item.GSI2SK = `COMPLEXITY#${enhancedMetadata.complexityScore}#${timestamp}`;
  }

  console.log("💾 Saving enhanced drilling query:", {
    queryId,
    queryType: enhancedMetadata.queryType,
    isDrillingQuery: enhancedMetadata.isDrillingQuery,
    sourcesCount: sanitizedSources.length,
    structuredSources: item.StructuredSourcesCount,
    technicalTerms: enhancedMetadata.technicalTermsCount,
    complexity: enhancedMetadata.complexityScore,
    metricsKeys: Object.keys(sanitizedMetrics),
  });

  const operation = () =>
    docClient.send(
      new PutCommand({
        TableName: tableName,
        Item: item,
        ConditionExpression: "attribute_not_exists(PK)", // Prevent duplicates
      })
    );

  const result = await executeWithRetry(
    operation,
    "Save Enhanced Drilling Query"
  );

  if (result.success) {
    console.log(`✅ Enhanced drilling query saved successfully:`, {
      queryId,
      type: enhancedMetadata.queryType,
      drilling: enhancedMetadata.isDrillingQuery,
      tokens: totalTokens,
      sources: sanitizedSources.length,
      complexity: enhancedMetadata.complexityScore,
    });

    return {
      success: true,
      queryId,
      data: {
        queryId,
        userId,
        model,
        totalTokens,
        queryWordCount,
        answerWordCount,
        sourcesCount: sanitizedSources.length,
        structuredSourcesCount: item.StructuredSourcesCount,
        queryType: enhancedMetadata.queryType,
        isDrillingQuery: enhancedMetadata.isDrillingQuery,
        technicalTermsCount: enhancedMetadata.technicalTermsCount,
        complexityScore: enhancedMetadata.complexityScore,
        metrics: sanitizedMetrics,
        processingVersion: enhancedMetadata.processingVersion,
        searchOptimized: true,
      },
    };
  }

  console.error("❌ Failed to save enhanced drilling query:", result.message);
  return result;
};

// ========== UTILITY FUNCTIONS ==========

/**
 * Analyze query characteristics for enhanced classification
 */
function analyzeQueryCharacteristics(queryText) {
  const lowerQuery = queryText.toLowerCase();

  // Technical terms analysis
  const drillingTerms = [
    "motor",
    "stator",
    "bit",
    "bha",
    "rop",
    "wob",
    "tfa",
    "drilling",
    "circulation",
    "slide",
    "rotary",
    "footage",
    "nmdc",
    "ubho",
    "float sub",
    "shock sub",
    "differential pressure",
    "pickup weight",
    "total drilled",
    "drilling hours",
  ];

  const aggregationTerms = [
    "most used",
    "average",
    "total",
    "highest",
    "lowest",
    "common",
    "typical",
    "median",
    "sum of",
    "count of",
  ];

  const comparisonTerms = [
    "vs",
    "versus",
    "compare",
    "difference",
    "better than",
    "worse than",
    "higher than",
    "lower than",
  ];

  // Count technical terms
  const technicalTermsCount = drillingTerms.filter(term =>
    lowerQuery.includes(term)
  ).length;

  // Determine query characteristics
  const isDrilling = technicalTermsCount > 0;
  const isAggregation = aggregationTerms.some(term =>
    lowerQuery.includes(term)
  );
  const isComparison = comparisonTerms.some(term => lowerQuery.includes(term));

  // Determine query type
  let type = "general";
  if (isAggregation) type = "aggregation";
  else if (isComparison) type = "comparison";
  else if (isDrilling) type = "drilling";

  // Calculate complexity score
  let complexityScore = 1;
  if (isAggregation) complexityScore += 2;
  if (isComparison) complexityScore += 1;
  if (technicalTermsCount > 3) complexityScore += 1;
  if (queryText.length > 100) complexityScore += 1;
  complexityScore = Math.min(complexityScore, 5);

  return {
    type,
    isDrilling,
    isAggregation,
    isComparison,
    technicalTermsCount,
    complexityScore,
  };
}

/**
 * Calculate average source score
 */
function calculateAverageSourceScore(sources) {
  if (sources.length === 0) return 0;

  const totalScore = sources.reduce((sum, source) => {
    const score = parseFloat(source.Score) || 0;
    return sum + score;
  }, 0);

  return Math.round((totalScore / sources.length) * 100) / 100;
}

/**
 * Extract searchable terms from query and answer
 */
function extractSearchableTerms(queryText, answer) {
  const terms = new Set();

  // Extract from query
  const queryWords = queryText
    .toLowerCase()
    .replace(/[^\w\s]/g, " ")
    .split(/\s+/)
    .filter(word => word.length > 3);

  queryWords.forEach(word => terms.add(word));

  // Extract technical terms from answer
  const technicalTerms = [
    "motor",
    "stator",
    "bit",
    "bha",
    "rop",
    "wob",
    "tfa",
    "drilling",
    "circulation",
    "slide",
    "rotary",
  ];

  const answerLower = answer.toLowerCase();
  technicalTerms.forEach(term => {
    if (answerLower.includes(term)) {
      terms.add(term);
    }
  });

  return Array.from(terms).slice(0, 15); // Limit to 15 terms
}

/**
 * Enhanced save file details with drilling-specific metadata and optimizations
 * @param {Object} payload - Enhanced file data to save
 * @param {string} payload.userId - User identifier
 * @param {string} payload.fileName - Original file name
 * @param {string} payload.key - S3 object key
 * @param {string} payload.collectionName - Qdrant collection name
 * @param {string} payload.timestamp - ISO timestamp
 * @param {string} payload.tableName - DynamoDB table name
 * @param {string} payload.bucketName - S3 bucket name
 * @param {number} [payload.totalChunks] - Number of text chunks
 * @param {number} [payload.successfulChunks] - Successfully processed chunks
 * @param {number} [payload.textLength] - Extracted text length
 * @param {number} [payload.processingTimeMs] - Total processing time
 * @param {number} [payload.fileSize] - File size in bytes
 * @param {string} [payload.documentType] - Detected document type (BHA, MMR, RVEN)
 * @param {string} [payload.extractionMethod] - Text extraction method used
 * @param {number} [payload.embeddingSuccessRate] - Embedding processing success rate
 * @param {number} [payload.averageChunkLength] - Average chunk length
 * @param {boolean} [payload.isDrillingReport] - Whether this is a drilling report
 * @param {string} [payload.processingVersion] - Processing pipeline version
 * @param {Object} [payload.qualityMetrics] - Content quality assessment metrics
 * @param {Object} [payload.metadata] - Additional metadata
 * @returns {Promise<Object>} - Enhanced save result
 */
const saveFileDetails = async ({
  userId,
  fileName,
  key,
  collectionName,
  timestamp,
  tableName,
  bucketName,
  totalChunks = 0,
  successfulChunks = 0,
  textLength = 0,
  processingTimeMs = 0,
  fileSize = 0,
  // Enhanced drilling-specific fields
  documentType = null,
  extractionMethod = null,
  embeddingSuccessRate = null,
  averageChunkLength = null,
  isDrillingReport = false,
  processingVersion = "2.0-drilling-optimized",
  qualityMetrics = {},
  metadata = {},
}) => {
  console.log("💾 Saving enhanced drilling report details to DynamoDB...", {
    userId,
    fileName,
    key,
    collectionName,
    totalChunks,
    successfulChunks,
    documentType,
    isDrillingReport,
    processingVersion,
  });

  // Enhanced validation for drilling reports
  const commonValidation = validateCommonParams({
    userId,
    timestamp,
    tableName,
  });
  if (!commonValidation.isValid) {
    return {
      success: false,
      error: new Error(
        `Drilling report validation failed: ${commonValidation.errors.join(
          ", "
        )}`
      ),
    };
  }

  const additionalErrors = [];
  if (!fileName?.trim()) additionalErrors.push("File name is required");
  if (!key?.trim()) additionalErrors.push("S3 key is required");
  if (!collectionName?.trim())
    additionalErrors.push("Collection name is required");
  if (!bucketName?.trim()) additionalErrors.push("Bucket name is required");

  // Enhanced validation for drilling-specific fields
  if (isDrillingReport) {
    if (
      documentType &&
      !["BHA", "MMR", "RVEN", "DRILLING"].includes(documentType)
    ) {
      additionalErrors.push("Invalid document type for drilling report");
    }
    if (
      embeddingSuccessRate !== null &&
      (embeddingSuccessRate < 0 || embeddingSuccessRate > 1)
    ) {
      additionalErrors.push("Embedding success rate must be between 0 and 1");
    }
  }

  if (additionalErrors.length > 0) {
    return {
      success: false,
      error: new Error(
        `Drilling report validation failed: ${additionalErrors.join(", ")}`
      ),
    };
  }

  const fileId = uuidv4();
  const PK = `FILE#${fileId}`;
  const SK = `USER#${userId}#FILE#${fileId}`;

  // Enhanced file analysis
  const fileExtension = fileName.split(".").pop()?.toLowerCase() || "";
  const mimeType =
    fileExtension === "pdf" ? "application/pdf" : "application/octet-stream";

  // Enhanced drilling document type detection
  const detectedDocType = detectDrillingDocumentType(fileName, documentType);

  // Calculate enhanced processing statistics
  const processingSuccess =
    totalChunks > 0 ? (successfulChunks / totalChunks) * 100 : 0;
  const embeddingSuccessPercent =
    embeddingSuccessRate !== null ? embeddingSuccessRate * 100 : null;

  // Calculate processing efficiency metrics
  const processingEfficiency = calculateProcessingEfficiency({
    processingTimeMs,
    totalChunks,
    textLength,
    successfulChunks,
  });

  // Enhanced public URL generation
  const encodedKey = encodeURIComponent(key);
  const publicUrl = `https://${bucketName}.s3.amazonaws.com/${encodedKey}`;

  // Create enhanced drilling-specific metadata
  const drillingMetadata = createDrillingMetadata({
    documentType: detectedDocType,
    extractionMethod,
    embeddingSuccessRate,
    averageChunkLength,
    qualityMetrics,
    processingEfficiency,
  });

  // Build enhanced item with drilling optimizations
  const item = {
    // Core fields
    PK,
    SK,
    EntityType: "File",
    FileId: fileId,
    UserId: userId,
    FileName: fileName.trim(),
    FileExtension: fileExtension,
    MimeType: mimeType,
    S3Key: key,
    S3Bucket: bucketName,
    PublicUrl: publicUrl,
    Collection: collectionName,

    // Basic metrics
    FileSize: fileSize,
    TextLength: textLength,
    TotalChunks: totalChunks,
    SuccessfulChunks: successfulChunks,
    ProcessingTimeMs: processingTimeMs,

    // Enhanced processing metrics
    ProcessingSuccessRate: Math.round(processingSuccess * 100) / 100,
    ProcessingStatus: determineProcessingStatus(
      successfulChunks,
      totalChunks,
      embeddingSuccessRate
    ),
    ProcessingEfficiency: processingEfficiency,

    // Drilling-specific fields
    IsDrillingReport: isDrillingReport,
    DocumentType: detectedDocType,
    DocumentCategory: categorizeDrillingDocument(detectedDocType),
    ExtractionMethod: extractionMethod,
    EmbeddingSuccessRate: embeddingSuccessPercent,
    AverageChunkLength: averageChunkLength,
    ProcessingVersion: processingVersion,

    // Enhanced drilling metadata
    DrillingMetadata: drillingMetadata,

    // Quality assessment
    ContentQuality: assessContentQuality({
      textLength,
      totalChunks,
      successfulChunks,
      embeddingSuccessRate,
      qualityMetrics,
    }),

    // Search optimization fields
    SearchableFields: createSearchableFields({
      fileName,
      documentType: detectedDocType,
      extractionMethod,
      isDrillingReport,
    }),

    // Timestamps and TTL
    CreatedAt: timestamp,
    UpdatedAt: timestamp,
    TTL:
      Math.floor(Date.now() / 1000) +
      (isDrillingReport ? 3 * 365 * 24 * 60 * 60 : 2 * 365 * 24 * 60 * 60), // 3 years for drilling reports, 2 for others

    // Additional metadata
    ...metadata,
  };

  // Add drilling-specific GSI fields for better querying
  if (isDrillingReport) {
    item.GSI1PK = `DRILLING#${detectedDocType || "UNKNOWN"}`;
    item.GSI1SK = `USER#${userId}#${timestamp}`;
    item.GSI2PK = `USER#${userId}#DRILLING`;
    item.GSI2SK = `TYPE#${detectedDocType || "UNKNOWN"}#${timestamp}`;
  }

  const operation = () =>
    docClient.send(
      new PutCommand({
        TableName: tableName,
        Item: item,
        ConditionExpression: "attribute_not_exists(PK)", // Prevent duplicates
      })
    );

  const result = await executeWithRetry(
    operation,
    "Save Enhanced Drilling File Details"
  );

  if (result.success) {
    const logData = {
      fileId,
      documentType: detectedDocType,
      chunks: `${successfulChunks}/${totalChunks}`,
      processingTime: `${processingTimeMs}ms`,
      efficiency: processingEfficiency.overallScore.toFixed(2),
    };

    console.log(
      `✅ Enhanced drilling file details saved successfully:`,
      logData
    );

    return {
      success: true,
      fileId,
      data: {
        fileId,
        userId,
        fileName,
        documentType: detectedDocType,
        totalChunks,
        successfulChunks,
        processingSuccessRate: Math.round(processingSuccess * 100) / 100,
        embeddingSuccessRate: embeddingSuccessPercent,
        processingTimeMs,
        processingEfficiency: processingEfficiency.overallScore,
        publicUrl,
        isDrillingReport,
        processingVersion,
        contentQuality: item.ContentQuality,
        drillingOptimized: true,
      },
    };
  }

  console.error(
    "❌ Failed to save enhanced drilling file details:",
    result.message
  );
  return result;
};

/**
 * Detect drilling document type from filename and provided type
 */
function detectDrillingDocumentType(fileName, providedType) {
  if (providedType && ["BHA", "MMR", "RVEN"].includes(providedType)) {
    return providedType;
  }

  const name = fileName.toLowerCase();
  if (name.includes("bha")) return "BHA";
  if (name.includes("mmr")) return "MMR";
  if (name.includes("rven")) return "RVEN";
  if (name.includes("drill") || name.includes("motor") || name.includes("bit"))
    return "DRILLING";

  return null;
}

/**
 * Categorize drilling document for organization
 */
function categorizeDrillingDocument(documentType) {
  const categories = {
    BHA: "Assembly_Reports",
    MMR: "Motor_Reports",
    RVEN: "Evaluation_Reports",
    DRILLING: "General_Drilling",
  };

  return categories[documentType] || "Unknown";
}

/**
 * Calculate processing efficiency metrics
 */
function calculateProcessingEfficiency({
  processingTimeMs,
  totalChunks,
  textLength,
  successfulChunks,
}) {
  const efficiency = {
    timePerChunk: totalChunks > 0 ? processingTimeMs / totalChunks : 0,
    timePerCharacter: textLength > 0 ? processingTimeMs / textLength : 0,
    successRate: totalChunks > 0 ? successfulChunks / totalChunks : 0,
    throughput:
      processingTimeMs > 0 ? (successfulChunks / processingTimeMs) * 1000 : 0, // chunks per second
    overallScore: 0,
  };

  // Calculate overall efficiency score (0-100)
  let score = 0;
  if (efficiency.successRate >= 0.9) score += 40;
  else if (efficiency.successRate >= 0.7) score += 30;
  else if (efficiency.successRate >= 0.5) score += 20;

  if (efficiency.timePerChunk < 1000) score += 30; // Under 1 second per chunk
  else if (efficiency.timePerChunk < 2000) score += 20;
  else if (efficiency.timePerChunk < 5000) score += 10;

  if (efficiency.throughput > 1) score += 20; // More than 1 chunk per second
  else if (efficiency.throughput > 0.5) score += 15;
  else if (efficiency.throughput > 0.1) score += 10;

  efficiency.overallScore = Math.min(score, 100);
  return efficiency;
}

/**
 * Create drilling-specific metadata object
 */
function createDrillingMetadata({
  documentType,
  extractionMethod,
  embeddingSuccessRate,
  averageChunkLength,
  qualityMetrics,
  processingEfficiency,
}) {
  const metadata = {
    hasStructuredData: qualityMetrics.hasStructuredData || false,
    hasTechnicalMetrics: qualityMetrics.hasTechnicalMetrics || false,
    ocrUsed:
      extractionMethod &&
      (extractionMethod.includes("ocr") ||
        extractionMethod.includes("tesseract")),
    highQualityExtraction: (embeddingSuccessRate || 0) > 0.8,
    processingComplexity: determineProcessingComplexity(
      averageChunkLength,
      extractionMethod
    ),
    dataRichness: assessDataRichness({ averageChunkLength, qualityMetrics }),
    optimizationApplied: true,
  };

  // Add document-specific metadata
  if (documentType) {
    metadata.documentSpecificFields = getDocumentSpecificFields(documentType);
  }

  return metadata;
}

/**
 * Determine processing complexity level
 */
function determineProcessingComplexity(averageChunkLength, extractionMethod) {
  let complexity = "SIMPLE";

  if (
    extractionMethod &&
    (extractionMethod.includes("ocr") || extractionMethod.includes("tesseract"))
  ) {
    complexity = "COMPLEX";
  } else if (
    extractionMethod &&
    extractionMethod.includes("structure-preserving")
  ) {
    complexity = "MODERATE";
  }

  if (averageChunkLength && averageChunkLength > 1500) {
    complexity = complexity === "SIMPLE" ? "MODERATE" : "COMPLEX";
  }

  return complexity;
}

/**
 * Assess data richness of the processed content
 */
function assessDataRichness({ averageChunkLength, qualityMetrics }) {
  let richness = "LOW";
  let score = 0;

  if (averageChunkLength > 1000) score += 2;
  else if (averageChunkLength > 500) score += 1;

  if (qualityMetrics.hasStructuredData) score += 2;
  if (qualityMetrics.hasTechnicalMetrics) score += 2;
  if (qualityMetrics.numericalDataCount > 10) score += 1;

  if (score >= 5) richness = "HIGH";
  else if (score >= 3) richness = "MODERATE";

  return richness;
}

/**
 * Get document type specific fields for metadata
 */
function getDocumentSpecificFields(documentType) {
  const fields = {
    BHA: ["motorSpecs", "bitData", "assemblyComponents", "performanceMetrics"],
    MMR: [
      "motorMeasurements",
      "statorData",
      "pressureReadings",
      "conditionAssessment",
    ],
    RVEN: [
      "runEvaluation",
      "performanceNotes",
      "equipmentCondition",
      "recommendations",
    ],
    DRILLING: ["generalSpecs", "operationalData", "technicalMetrics"],
  };

  return fields[documentType] || [];
}

/**
 * Determine processing status with enhanced logic
 */
function determineProcessingStatus(
  successfulChunks,
  totalChunks,
  embeddingSuccessRate
) {
  if (totalChunks === 0) return "no_content";

  const chunkSuccessRate = successfulChunks / totalChunks;
  const overallSuccessRate =
    embeddingSuccessRate !== null
      ? Math.min(chunkSuccessRate, embeddingSuccessRate)
      : chunkSuccessRate;

  if (overallSuccessRate >= 0.95) return "completed";
  if (overallSuccessRate >= 0.8) return "mostly_successful";
  if (overallSuccessRate >= 0.6) return "partial";
  if (overallSuccessRate >= 0.3) return "limited";
  return "failed";
}

/**
 * Assess overall content quality
 */
function assessContentQuality({
  textLength,
  totalChunks,
  successfulChunks,
  embeddingSuccessRate,
  qualityMetrics,
}) {
  let quality = {
    score: 0,
    level: "LOW",
    factors: [],
  };

  // Text length assessment
  if (textLength > 5000) {
    quality.score += 20;
    quality.factors.push("sufficient_text_length");
  } else if (textLength > 1000) {
    quality.score += 10;
  }

  // Chunk processing success
  const chunkSuccess = totalChunks > 0 ? successfulChunks / totalChunks : 0;
  if (chunkSuccess >= 0.9) {
    quality.score += 25;
    quality.factors.push("high_chunk_success");
  } else if (chunkSuccess >= 0.7) {
    quality.score += 15;
  }

  // Embedding success
  if (embeddingSuccessRate !== null) {
    if (embeddingSuccessRate >= 0.9) {
      quality.score += 25;
      quality.factors.push("high_embedding_success");
    } else if (embeddingSuccessRate >= 0.7) {
      quality.score += 15;
    }
  }

  // Quality metrics assessment
  if (qualityMetrics.hasStructuredData) {
    quality.score += 15;
    quality.factors.push("structured_data_present");
  }

  if (qualityMetrics.hasTechnicalMetrics) {
    quality.score += 15;
    quality.factors.push("technical_metrics_present");
  }

  // Determine quality level
  if (quality.score >= 80) quality.level = "HIGH";
  else if (quality.score >= 60) quality.level = "MODERATE";
  else if (quality.score >= 40) quality.level = "FAIR";
  else quality.level = "LOW";

  return quality;
}

/**
 * Create searchable fields for better discoverability
 */
function createSearchableFields({
  fileName,
  documentType,
  extractionMethod,
  isDrillingReport,
}) {
  const fields = [];

  // File name components
  const nameWords = fileName
    .toLowerCase()
    .replace(/[^a-z0-9]/g, " ")
    .split(/\s+/)
    .filter(Boolean);
  fields.push(...nameWords);

  // Document type
  if (documentType) {
    fields.push(documentType.toLowerCase());
  }

  // Extraction method
  if (extractionMethod) {
    fields.push(extractionMethod.toLowerCase().replace(/[^a-z0-9]/g, "_"));
  }

  // Drilling-specific terms
  if (isDrillingReport) {
    fields.push("drilling", "report", "technical", "performance");
  }

  return [...new Set(fields)]; // Remove duplicates
}

/**
 * Save multiple items in batch (useful for bulk operations)
 * @param {Array} items - Array of items to save
 * @param {string} tableName - DynamoDB table name
 * @returns {Promise<Object>} - Batch save result
 */
const saveBatch = async (items, tableName) => {
  if (!Array.isArray(items) || items.length === 0) {
    return {
      success: false,
      error: new Error("Items array is required and cannot be empty"),
    };
  }

  if (!tableName?.trim()) {
    return {
      success: false,
      error: new Error("Table name is required"),
    };
  }

  const results = [];
  const errors = [];

  // Process items in batches of 25 (DynamoDB limit)
  for (let i = 0; i < items.length; i += BATCH_SIZE) {
    const batch = items.slice(i, i + BATCH_SIZE);

    const requestItems = {
      [tableName]: batch.map(item => ({
        PutRequest: { Item: item },
      })),
    };

    const operation = () =>
      docClient.send(
        new BatchWriteCommand({
          RequestItems: requestItems,
        })
      );

    const result = await executeWithRetry(
      operation,
      `Batch Save ${i + 1}-${i + batch.length}`
    );

    if (result.success) {
      results.push(...batch);
    } else {
      errors.push({
        batch: i / BATCH_SIZE + 1,
        items: batch,
        error: result.error,
      });
    }
  }

  return {
    success: errors.length === 0,
    totalItems: items.length,
    successfulItems: results.length,
    failedItems: errors.length,
    errors: errors.length > 0 ? errors : undefined,
  };
};

module.exports = {
  saveQuery,
  saveFileDetails,
  saveBatch,
  // Export utilities for testing
  validateCommonParams,
  executeWithRetry,
};
