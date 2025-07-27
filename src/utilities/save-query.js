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
 * Save a user query with metadata (model, temperature, total tokens, sources, metrics, etc.)
 * @param {Object} payload - Data to save
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
 * @param {Object} [payload.metadata] - Additional metadata
 * @returns {Promise<Object>} - Save result
 */
/**
 * Save a user query with metadata (model, temperature, total tokens, sources, metrics, etc.)
 * @param {Object} payload - Data to save
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
 * @param {Object} [payload.metadata] - Additional metadata
 * @returns {Promise<Object>} - Save result
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
  // Validate required parameters
  const commonValidation = validateCommonParams({
    userId,
    timestamp,
    tableName,
  });
  if (!commonValidation.isValid) {
    return {
      success: false,
      error: new Error(
        `Validation failed: ${commonValidation.errors.join(", ")}`
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
      error: new Error(`Validation failed: ${additionalErrors.join(", ")}`),
    };
  }

  const queryId = uuidv4();
  const PK = `QUERY#${queryId}`;
  const SK = `USER#${userId}#QUERY#${queryId}`;

  // Calculate text statistics
  const queryWordCount = queryText.trim().split(/\s+/).length;
  const answerWordCount = answer.trim().split(/\s+/).length;

  // Validate and sanitize sources array
  const sanitizedSources = Array.isArray(sources)
    ? sources.map((source, index) => ({
        FileName: source.FileName || source.fileName || `Document ${index + 1}`,
        ChunkIndex: source.ChunkIndex || source.chunkIndex || index,
        Score: source.Score || source.score || "0.00",
        ...source, // Include any additional source properties
      }))
    : [];

  // Validate and sanitize metrics object
  const sanitizedMetrics = {
    totalRequestTimeMs: metrics.totalRequestTimeMs || 0,
    cached: metrics.cached || false,
    embeddingFromCache: metrics.embeddingFromCache || false,
    resultsCount: metrics.resultsCount || 0,
    tokensUsed: metrics.tokensUsed || totalTokens || 0,
    ...metrics, // Include any additional metrics
  };

  const item = {
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
    QueryWordCount: queryWordCount,
    AnswerWordCount: answerWordCount,
    // Use lowercase field names to match what Service class expects
    sources: sanitizedSources,
    metrics: sanitizedMetrics,
    SourcesCount: sanitizedSources.length,
    CreatedAt: timestamp,
    UpdatedAt: timestamp,
    TTL: Math.floor(Date.now() / 1000) + 365 * 24 * 60 * 60, // 1 year TTL
    ...metadata, // Spread additional metadata
  };

  console.log("💾 Saving query with sources and metrics:", {
    queryId,
    sourcesCount: sanitizedSources.length,
    metricsKeys: Object.keys(sanitizedMetrics),
    sources: sanitizedSources,
    metrics: sanitizedMetrics,
  });

  const operation = () =>
    docClient.send(
      new PutCommand({
        TableName: tableName,
        Item: item,
        ConditionExpression: "attribute_not_exists(PK)", // Prevent duplicates
      })
    );

  const result = await executeWithRetry(operation, "Save Query");

  if (result.success) {
    console.log(
      `✅ Query saved successfully. QueryId: ${queryId}, Tokens: ${totalTokens}, Sources: ${sanitizedSources.length}`
    );
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
        metrics: sanitizedMetrics,
      },
    };
  }

  console.error("❌ Failed to save query:", result.message);
  return result;
};

/**
 * Save file details with enhanced metadata
 * @param {Object} payload - File data to save
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
 * @param {number} [payload.fileSize] - File size in bytes
 * @param {Object} [payload.metadata] - Additional metadata
 * @returns {Promise<Object>} - Save result
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
  fileSize = 0,
  metadata = {},
}) => {
  console.log("Saving file details to DynamoDB...", {
    userId,
    fileName,
    key,
    collectionName,
    totalChunks,
    successfulChunks,
  });

  // Validate required parameters
  const commonValidation = validateCommonParams({
    userId,
    timestamp,
    tableName,
  });
  if (!commonValidation.isValid) {
    return {
      success: false,
      error: new Error(
        `Validation failed: ${commonValidation.errors.join(", ")}`
      ),
    };
  }

  const additionalErrors = [];
  if (!fileName?.trim()) additionalErrors.push("File name is required");
  if (!key?.trim()) additionalErrors.push("S3 key is required");
  if (!collectionName?.trim())
    additionalErrors.push("Collection name is required");
  if (!bucketName?.trim()) additionalErrors.push("Bucket name is required");

  if (additionalErrors.length > 0) {
    return {
      success: false,
      error: new Error(`Validation failed: ${additionalErrors.join(", ")}`),
    };
  }

  const fileId = uuidv4();
  const PK = `FILE#${fileId}`;
  const SK = `USER#${userId}#FILE#${fileId}`;

  // Generate public URL (consider making this configurable)
  const encodedKey = encodeURIComponent(key);
  const publicUrl = `https://${bucketName}.s3.amazonaws.com/${encodedKey}`;

  // Extract file extension and MIME type
  const fileExtension = fileName.split(".").pop()?.toLowerCase() || "";
  const mimeType =
    fileExtension === "pdf" ? "application/pdf" : "application/octet-stream";

  // Calculate processing statistics
  const processingSuccess =
    totalChunks > 0 ? (successfulChunks / totalChunks) * 100 : 0;

  const item = {
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
    FileSize: fileSize,
    TextLength: textLength,
    TotalChunks: totalChunks,
    SuccessfulChunks: successfulChunks,
    ProcessingSuccessRate: Math.round(processingSuccess * 100) / 100, // Round to 2 decimals
    ProcessingStatus:
      successfulChunks === totalChunks ? "completed" : "partial",
    CreatedAt: timestamp,
    UpdatedAt: timestamp,
    TTL: Math.floor(Date.now() / 1000) + 2 * 365 * 24 * 60 * 60, // 2 years TTL
    ...metadata, // Spread additional metadata
  };

  const operation = () =>
    docClient.send(
      new PutCommand({
        TableName: tableName,
        Item: item,
        ConditionExpression: "attribute_not_exists(PK)", // Prevent duplicates
      })
    );

  const result = await executeWithRetry(operation, "Save File Details");

  if (result.success) {
    console.log(
      `File details saved successfully. FileId: ${fileId}, Chunks: ${successfulChunks}/${totalChunks}`
    );
    return {
      success: true,
      fileId,
      data: {
        fileId,
        userId,
        fileName,
        totalChunks,
        successfulChunks,
        processingSuccessRate: Math.round(processingSuccess * 100) / 100,
        publicUrl,
      },
    };
  }

  console.error("Failed to save file details:", result.message);
  return result;
};

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
