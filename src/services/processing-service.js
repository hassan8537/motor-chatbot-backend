const { handlers } = require("../utilities/handlers");
const extractFromPDF = require("../utilities/pdf-parser");
const chunkText = require("../utilities/chunk-text");
const getEmbedding = require("../utilities/get-embedding");
const { v4: uuidv4 } = require("uuid");
const { upsertEmbeddings } = require("../utilities/qdrant-functions");
const { s3Client } = require("../config/aws");
const { GetObjectCommand } = require("@aws-sdk/client-s3");
const path = require("path");
const { saveFileDetails } = require("../utilities/save-query");
const { Readable } = require("stream");

// 🚀 Optimized Constants
const MAX_CONCURRENT_EMBEDDINGS = 8; // Increased for better throughput
const MAX_CONCURRENT_UPLOADS = 10; // Separate limit for Qdrant uploads
const MAX_RETRY_ATTEMPTS = 3;
const RETRY_DELAY_MS = 500; // Reduced initial delay
const SUPPORTED_FILE_TYPES = [".pdf"];
const MAX_FILE_SIZE = 50 * 1024 * 1024; // 50MB
const STREAM_TIMEOUT_MS = 45000; // 45 seconds
const BATCH_PROCESSING_DELAY = 50; // Reduced delay between batches

// 📊 Rate limiting for embedding API
const EMBEDDING_RATE_LIMIT = {
  maxRequests: 100,
  windowMs: 60000, // 1 minute
  requests: [],
};

class OptimizedDocumentAIService {
  constructor() {
    this.bucket = process.env.BUCKET_NAME;
    this.tableName = process.env.DYNAMODB_TABLE_NAME;

    // 📊 Performance tracking
    this.metrics = {
      totalProcessed: 0,
      totalErrors: 0,
      averageProcessingTime: 0,
      totalChunksProcessed: 0,
      totalTextExtracted: 0,
    };

    // 🧠 Simple caching for repeated operations
    this.textCache = new Map(); // Cache extracted text by S3 key
    this.chunkCache = new Map(); // Cache chunked text
    this.CACHE_TTL_MS = 30 * 60 * 1000; // 30 minutes

    // Validate required environment variables
    if (!this.bucket || !this.tableName) {
      throw new Error(
        "Missing required environment variables: BUCKET_NAME, DYNAMODB_TABLE_NAME"
      );
    }

    console.log("🚀 Optimized Document AI Service initialized");
  }

  /**
   * 🎯 Enhanced rate limiting for embedding API
   */
  async checkEmbeddingRateLimit() {
    const now = Date.now();

    // Clean old requests
    EMBEDDING_RATE_LIMIT.requests = EMBEDDING_RATE_LIMIT.requests.filter(
      timestamp => now - timestamp < EMBEDDING_RATE_LIMIT.windowMs
    );

    if (
      EMBEDDING_RATE_LIMIT.requests.length >= EMBEDDING_RATE_LIMIT.maxRequests
    ) {
      const oldestRequest = Math.min(...EMBEDDING_RATE_LIMIT.requests);
      const waitTime = EMBEDDING_RATE_LIMIT.windowMs - (now - oldestRequest);

      if (waitTime > 0) {
        console.log(`⏳ Rate limit reached, waiting ${waitTime}ms`);
        await this.delay(waitTime);
      }
    }

    EMBEDDING_RATE_LIMIT.requests.push(now);
  }

  /**
   * 💾 Caching utilities
   */
  getCachedText(key) {
    const cached = this.textCache.get(key);
    if (cached && Date.now() - cached.timestamp < this.CACHE_TTL_MS) {
      console.log("⚡ Using cached text extraction");
      return cached.text;
    }
    this.textCache.delete(key);
    return null;
  }

  setCachedText(key, text) {
    this.textCache.set(key, {
      text,
      timestamp: Date.now(),
    });

    // Cleanup if cache gets too large
    if (this.textCache.size > 50) {
      const oldestKey = this.textCache.keys().next().value;
      this.textCache.delete(oldestKey);
    }
  }

  getCachedChunks(textHash) {
    const cached = this.chunkCache.get(textHash);
    if (cached && Date.now() - cached.timestamp < this.CACHE_TTL_MS) {
      console.log("⚡ Using cached text chunks");
      return cached.chunks;
    }
    this.chunkCache.delete(textHash);
    return null;
  }

  setCachedChunks(textHash, chunks) {
    this.chunkCache.set(textHash, {
      chunks,
      timestamp: Date.now(),
    });

    // Cleanup if cache gets too large
    if (this.chunkCache.size > 20) {
      const oldestKey = this.chunkCache.keys().next().value;
      this.chunkCache.delete(oldestKey);
    }
  }

  /**
   * 🔧 Enhanced stream to buffer with progress tracking
   */
  async streamToBuffer(stream, expectedSize = null) {
    return new Promise((resolve, reject) => {
      const chunks = [];
      let receivedSize = 0;

      stream.on("data", chunk => {
        chunks.push(chunk);
        receivedSize += chunk.length;

        // Progress logging for large files
        if (expectedSize && receivedSize > 0) {
          const progress = Math.round((receivedSize / expectedSize) * 100);
          if (progress % 25 === 0) {
            // Log every 25%
            console.log(`📥 Download progress: ${progress}%`);
          }
        }
      });

      stream.on("end", () => {
        try {
          const buffer = Buffer.concat(chunks);
          console.log(`✅ Stream completed: ${buffer.length} bytes`);
          resolve(buffer);
        } catch (error) {
          reject(new Error(`Buffer concatenation failed: ${error.message}`));
        }
      });

      stream.on("error", error => {
        reject(new Error(`Stream error: ${error.message}`));
      });

      // Enhanced timeout with better error message
      const timeout = setTimeout(() => {
        reject(
          new Error(`Stream timeout after ${STREAM_TIMEOUT_MS / 1000} seconds`)
        );
      }, STREAM_TIMEOUT_MS);

      stream.on("end", () => clearTimeout(timeout));
      stream.on("error", () => clearTimeout(timeout));
    });
  }

  /**
   * 📥 Enhanced S3 download with progress tracking
   */
  async downloadPdfFromS3(key) {
    let lastError;

    for (let attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
      try {
        console.log(
          `📥 Downloading PDF (attempt ${attempt}/${MAX_RETRY_ATTEMPTS}): ${key}`
        );

        const command = new GetObjectCommand({
          Bucket: this.bucket,
          Key: key,
        });

        const startTime = Date.now();
        const data = await s3Client.send(command);

        if (!data.Body) {
          throw new Error("No data received from S3");
        }

        // Enhanced file size validation
        const contentLength = data.ContentLength;
        if (contentLength && contentLength > MAX_FILE_SIZE) {
          throw new Error(
            `File too large: ${(contentLength / 1024 / 1024).toFixed(
              2
            )}MB (max: ${MAX_FILE_SIZE / 1024 / 1024}MB)`
          );
        }

        const buffer = await this.streamToBuffer(data.Body, contentLength);
        const downloadTime = Date.now() - startTime;

        console.log(
          `✅ PDF downloaded: ${(buffer.length / 1024 / 1024).toFixed(
            2
          )}MB in ${downloadTime}ms`
        );
        return buffer;
      } catch (error) {
        lastError = error;
        console.error(
          `❌ S3 download attempt ${attempt} failed:`,
          error.message
        );

        // Don't retry on certain errors
        if (
          error.message.includes("too large") ||
          error.message.includes("not found")
        ) {
          throw error;
        }

        if (attempt < MAX_RETRY_ATTEMPTS) {
          const delay = RETRY_DELAY_MS * Math.pow(2, attempt - 1); // Exponential backoff
          await this.delay(delay);
        }
      }
    }

    throw new Error(
      `Failed to download PDF after ${MAX_RETRY_ATTEMPTS} attempts: ${lastError.message}`
    );
  }

  /**
   * 🧠 Enhanced text extraction with caching
   */
  async extractTextFromPdf(pdfBuffer, key) {
    // Check cache first
    const cachedText = this.getCachedText(key);
    if (cachedText) {
      return cachedText;
    }

    console.log("🔍 Extracting text from PDF...");
    const startTime = Date.now();

    try {
      const { text } = await extractFromPDF(pdfBuffer);

      if (!text || text.trim().length === 0) {
        throw new Error("No text content extracted from PDF");
      }

      const extractionTime = Date.now() - startTime;
      console.log(
        `✅ Text extracted: ${text.length} characters in ${extractionTime}ms`
      );

      // Cache the result
      this.setCachedText(key, text);

      return text;
    } catch (error) {
      console.error("❌ Text extraction failed:", error.message);
      throw new Error(`Text extraction failed: ${error.message}`);
    }
  }

  /**
   * ✂️ Enhanced text chunking with caching
   */
  async chunkTextWithCache(text) {
    // Create a simple hash for caching
    const textHash = this.simpleHash(text.substring(0, 1000)); // Hash first 1000 chars

    // Check cache first
    const cachedChunks = this.getCachedChunks(textHash);
    if (cachedChunks) {
      return cachedChunks;
    }

    console.log("✂️ Chunking text...");
    const startTime = Date.now();

    try {
      const chunks = await chunkText(text);

      if (!chunks || chunks.length === 0) {
        throw new Error("No text chunks generated");
      }

      const chunkingTime = Date.now() - startTime;
      console.log(`✅ Generated ${chunks.length} chunks in ${chunkingTime}ms`);

      // Cache the result
      this.setCachedChunks(textHash, chunks);

      return chunks;
    } catch (error) {
      console.error("❌ Text chunking failed:", error.message);
      throw new Error(`Text chunking failed: ${error.message}`);
    }
  }

  /**
   * 🚀 Optimized batch embedding processing with enhanced concurrency
   */
  async processEmbeddingsInBatches(
    chunks,
    collectionName,
    key,
    fileName,
    userId
  ) {
    console.log(
      `🧠 Processing ${chunks.length} chunks with optimized batching`
    );

    const results = [];
    const errors = [];
    const startTime = Date.now();

    // Process chunks in optimized batches
    for (let i = 0; i < chunks.length; i += MAX_CONCURRENT_EMBEDDINGS) {
      const batch = chunks.slice(i, i + MAX_CONCURRENT_EMBEDDINGS);
      const batchNumber = Math.floor(i / MAX_CONCURRENT_EMBEDDINGS) + 1;
      const totalBatches = Math.ceil(chunks.length / MAX_CONCURRENT_EMBEDDINGS);

      console.log(
        `📦 Processing batch ${batchNumber}/${totalBatches} (${batch.length} chunks)`
      );

      try {
        // Process embeddings with rate limiting
        const embeddingPromises = batch.map(async (chunk, index) => {
          const globalIndex = i + index;

          try {
            // Apply rate limiting
            await this.checkEmbeddingRateLimit();

            // Generate embedding with retry
            const embedding = await this.retryOperation(
              () => getEmbedding(chunk),
              2,
              `Generate embedding for chunk ${globalIndex}`
            );

            return {
              chunk,
              embedding,
              globalIndex,
            };
          } catch (error) {
            throw new Error(
              `Chunk ${globalIndex} embedding failed: ${error.message}`
            );
          }
        });

        const embeddingResults = await Promise.allSettled(embeddingPromises);

        // Process successful embeddings and upload to Qdrant in parallel
        const uploadPromises = [];

        embeddingResults.forEach((result, index) => {
          const globalIndex = i + index;

          if (result.status === "fulfilled") {
            const { chunk, embedding } = result.value;

            // Create upload promise
            const uploadPromise = this.retryOperation(
              async () => {
                const id = uuidv4();

                await upsertEmbeddings({
                  collectionName,
                  id,
                  vector: embedding,
                  payload: {
                    key,
                    name: fileName,
                    content: chunk,
                    sectionType: "full_text",
                    chunkIndex: globalIndex,
                    totalChunks: chunks.length,
                    userId, // Add userId to payload
                    createdAt: new Date().toISOString(),
                  },
                  dimension: 1536,
                });

                return {
                  id,
                  content: chunk,
                  embedding,
                  chunkIndex: globalIndex,
                };
              },
              2,
              `Upload chunk ${globalIndex} to Qdrant`
            );

            uploadPromises.push(uploadPromise);
          } else {
            errors.push({
              chunkIndex: globalIndex,
              error: result.reason.message,
              stage: "embedding",
            });
          }
        });

        // Execute uploads with controlled concurrency
        const uploadBatches = [];
        for (
          let j = 0;
          j < uploadPromises.length;
          j += MAX_CONCURRENT_UPLOADS
        ) {
          uploadBatches.push(
            uploadPromises.slice(j, j + MAX_CONCURRENT_UPLOADS)
          );
        }

        for (const uploadBatch of uploadBatches) {
          const uploadResults = await Promise.allSettled(uploadBatch);

          uploadResults.forEach((result, index) => {
            if (result.status === "fulfilled") {
              results.push(result.value);
            } else {
              errors.push({
                chunkIndex: i + index,
                error: result.reason.message,
                stage: "upload",
              });
            }
          });
        }
      } catch (error) {
        console.error(`❌ Batch ${batchNumber} processing error:`, error);
        errors.push({
          batch: batchNumber,
          error: error.message,
          stage: "batch",
        });
      }

      // Progress update
      const progressPercent = Math.round(
        (results.length / chunks.length) * 100
      );
      console.log(
        `📊 Progress: ${results.length}/${chunks.length} chunks processed (${progressPercent}%)`
      );

      // Optimized delay between batches
      if (i + MAX_CONCURRENT_EMBEDDINGS < chunks.length) {
        await this.delay(BATCH_PROCESSING_DELAY);
      }
    }

    const totalTime = Date.now() - startTime;
    console.log(
      `✅ Embedding processing complete: ${results.length} successful, ${errors.length} failed in ${totalTime}ms`
    );

    if (errors.length > 0) {
      console.warn("⚠️ Some chunks failed to process:", errors.slice(0, 5)); // Log first 5 errors
    }

    return {
      results,
      errors,
      successCount: results.length,
      errorCount: errors.length,
      processingTimeMs: totalTime,
    };
  }

  /**
   * 🔄 Enhanced retry logic with exponential backoff
   */
  async retryOperation(operation, maxRetries = 3, operationName = "operation") {
    let lastError;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await operation();
      } catch (error) {
        lastError = error;

        if (attempt === 1) {
          console.warn(`⚠️ ${operationName} failed, retrying...`);
        }

        // Don't retry on certain errors
        if (
          error.message?.includes("validation") ||
          error.message?.includes("authorization") ||
          error.message?.includes("not found")
        ) {
          throw error;
        }

        if (attempt < maxRetries) {
          const delay = RETRY_DELAY_MS * Math.pow(2, attempt - 1);
          await this.delay(delay);
        }
      }
    }

    throw new Error(
      `${operationName} failed after ${maxRetries} attempts: ${lastError.message}`
    );
  }

  /**
   * 🔢 Simple hash function for caching
   */
  simpleHash(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = (hash << 5) - hash + char;
      hash = hash & hash; // Convert to 32-bit integer
    }
    return hash.toString();
  }

  /**
   * ✅ Enhanced input validation
   */
  validateInput({ key, collectionName, userId }) {
    const errors = [];

    if (!key?.trim()) {
      errors.push("Missing or invalid 'key' parameter");
    } else {
      // Enhanced file validation
      const fileExtension = path.extname(key).toLowerCase();
      if (!SUPPORTED_FILE_TYPES.includes(fileExtension)) {
        errors.push(
          `Unsupported file type: ${fileExtension}. Supported types: ${SUPPORTED_FILE_TYPES.join(
            ", "
          )}`
        );
      }

      // Check for suspicious file names
      if (key.includes("..") || key.includes("//")) {
        errors.push("Invalid file path detected");
      }
    }

    if (!collectionName?.trim()) {
      errors.push("Missing or invalid 'collectionName' parameter");
    } else if (!/^[a-zA-Z0-9_-]+$/.test(collectionName)) {
      errors.push("Collection name contains invalid characters");
    }

    if (!userId?.trim()) {
      errors.push("Missing or invalid user ID");
    }

    return {
      isValid: errors.length === 0,
      errors,
    };
  }

  /**
   * 📊 Update performance metrics
   */
  updateMetrics(processingTime, chunksProcessed, textLength, success = true) {
    if (success) {
      this.metrics.totalProcessed++;
      this.metrics.totalChunksProcessed += chunksProcessed;
      this.metrics.totalTextExtracted += textLength;

      // Moving average for processing time
      const alpha = 0.1;
      this.metrics.averageProcessingTime =
        this.metrics.averageProcessingTime * (1 - alpha) +
        processingTime * alpha;
    } else {
      this.metrics.totalErrors++;
    }
  }

  /**
   * ⏱️ Utility function for delays
   */
  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * 🎯 Main optimized PDF processing method
   */
  async processUploadedPdf(req, res) {
    const startTime = Date.now();

    try {
      console.log("🚀 === Starting optimized PDF processing ===");

      const { key, collectionName } = req.body;
      const userId = req.user?.UserId;

      // Enhanced validation
      const validation = this.validateInput({ key, collectionName, userId });
      if (!validation.isValid) {
        return handlers.response.failed({
          res,
          message: `Validation failed: ${validation.errors.join(", ")}`,
          statusCode: 400,
        });
      }

      const fileName = path.basename(key);
      console.log(`📄 Processing file: ${fileName} for user: ${userId}`);

      // Step 1: Download PDF from S3
      const pdfBuffer = await this.downloadPdfFromS3(key);

      // Step 2: Extract text with caching
      const text = await this.extractTextFromPdf(pdfBuffer, key);

      // Step 3: Chunk text with caching
      const chunks = await this.chunkTextWithCache(text);

      // Step 4: Process embeddings with optimized batching
      console.log(
        "🧠 Step 4: Processing embeddings with optimized batching..."
      );
      const embeddingResults = await this.processEmbeddingsInBatches(
        chunks,
        collectionName,
        key,
        fileName,
        userId
      );

      // Step 5: Save file metadata
      console.log("💾 Step 5: Saving file metadata...");
      await this.retryOperation(
        () =>
          saveFileDetails({
            userId,
            fileName,
            key,
            collectionName,
            timestamp: new Date().toISOString(),
            tableName: this.tableName,
            bucketName: this.bucket,
            totalChunks: chunks.length,
            successfulChunks: embeddingResults.successCount,
            textLength: text.length,
            processingTimeMs: embeddingResults.processingTimeMs,
          }),
        2,
        "Save file metadata"
      );

      const totalProcessingTime = Date.now() - startTime;

      // Update metrics
      this.updateMetrics(
        totalProcessingTime,
        embeddingResults.successCount,
        text.length,
        true
      );

      console.log(
        `🎉 === PDF processing completed in ${totalProcessingTime}ms ===`
      );

      return handlers.response.success({
        res,
        message: "PDF processed and embeddings stored successfully",
        data: {
          fileName,
          totalChunks: chunks.length,
          successfulChunks: embeddingResults.successCount,
          failedChunks: embeddingResults.errorCount,
          processingTimeMs: totalProcessingTime,
          embeddingProcessingTimeMs: embeddingResults.processingTimeMs,
          textLength: text.length,
          collectionName,
          performance: {
            avgChunkProcessingTime:
              embeddingResults.processingTimeMs / chunks.length,
            throughputChunksPerSecond: Math.round(
              (embeddingResults.successCount /
                embeddingResults.processingTimeMs) *
                1000
            ),
          },
          errors:
            embeddingResults.errors.length > 0
              ? embeddingResults.errors.slice(0, 10)
              : undefined, // Limit errors in response
        },
      });
    } catch (error) {
      const totalProcessingTime = Date.now() - startTime;

      // Update error metrics
      this.updateMetrics(totalProcessingTime, 0, 0, false);

      console.error(
        `❌ PDF processing failed after ${totalProcessingTime}ms:`,
        error
      );

      return handlers.response.error({
        res,
        message: `PDF processing failed: ${error.message}`,
        statusCode: error.statusCode || 500,
        data: {
          processingTimeMs: totalProcessingTime,
          stage: error.stage || "unknown",
          error:
            process.env.NODE_ENV === "development" ? error.stack : undefined,
        },
      });
    }
  }

  /**
   * 📊 Get service metrics
   */
  async getMetrics(req, res) {
    try {
      return handlers.response.success({
        res,
        message: "Document AI service metrics",
        data: {
          performance: this.metrics,
          cache: {
            textCacheSize: this.textCache.size,
            chunkCacheSize: this.chunkCache.size,
          },
          system: {
            uptime: process.uptime(),
            memoryUsage: process.memoryUsage(),
          },
        },
      });
    } catch (error) {
      return handlers.response.error({
        res,
        message: "Failed to get metrics",
        statusCode: 500,
      });
    }
  }

  /**
   * 🧹 Clear caches
   */
  async clearCaches(req, res) {
    try {
      this.textCache.clear();
      this.chunkCache.clear();

      console.log("🧹 Document AI caches cleared");

      return handlers.response.success({
        res,
        message: "Caches cleared successfully",
        data: { timestamp: new Date().toISOString() },
      });
    } catch (error) {
      return handlers.response.error({
        res,
        message: "Failed to clear caches",
        statusCode: 500,
      });
    }
  }
}

module.exports = new OptimizedDocumentAIService();
