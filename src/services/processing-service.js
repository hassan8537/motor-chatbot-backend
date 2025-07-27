const { handlers } = require("../utilities/handlers");
const extractFromPDF = require("../utilities/pdf-parser");
const chunkText = require("../utilities/chunk-text");
const getEmbedding = require("../utilities/get-embedding");
const { v4: uuidv4 } = require("uuid");
const { upsertEmbeddings } = require("../utilities/qdrant-functions");
const { s3Client } = require("../config/aws");
const { GetObjectCommand, DeleteObjectCommand } = require("@aws-sdk/client-s3");
const path = require("path");
const { saveFileDetails } = require("../utilities/save-query");
const { Readable } = require("stream");

// 🚀 Optimized Constants
const MAX_CONCURRENT_EMBEDDINGS = 8;
const MAX_CONCURRENT_UPLOADS = 10;
const MAX_RETRY_ATTEMPTS = 3;
const RETRY_DELAY_MS = 500;
const SUPPORTED_FILE_TYPES = [".pdf"];
const MAX_FILE_SIZE = 50 * 1024 * 1024; // 50MB
const STREAM_TIMEOUT_MS = 45000;
const BATCH_PROCESSING_DELAY = 50;
const MIN_TEXT_LENGTH = 50; // Minimum viable text length

// 📊 Rate limiting for embedding API
const EMBEDDING_RATE_LIMIT = {
  maxRequests: 100,
  windowMs: 60000,
  requests: [],
};

class OptimizedDocumentAIService {
  constructor() {
    this.bucket = process.env.BUCKET_NAME;
    this.tableName = process.env.DYNAMODB_TABLE_NAME;

    // 📊 Performance tracking with enhanced metrics
    this.metrics = {
      totalProcessed: 0,
      totalErrors: 0,
      totalDeleted: 0, // Track deleted files
      averageProcessingTime: 0,
      totalChunksProcessed: 0,
      totalTextExtracted: 0,
      errorTypes: {
        textExtraction: 0,
        chunking: 0,
        embedding: 0,
        upload: 0,
        s3Access: 0,
        validation: 0,
      },
    };

    // 🧠 Simple caching for repeated operations
    this.textCache = new Map();
    this.chunkCache = new Map();
    this.CACHE_TTL_MS = 30 * 60 * 1000; // 30 minutes

    // Validate required environment variables
    if (!this.bucket || !this.tableName) {
      throw new Error(
        "Missing required environment variables: BUCKET_NAME, DYNAMODB_TABLE_NAME"
      );
    }

    console.log(
      "🚀 Enhanced Document AI Service initialized with auto-cleanup"
    );
  }

  /**
   * 🗑️ Delete file from S3 with retry logic
   */
  async deleteFileFromS3(key, reason = "processing_failed") {
    try {
      console.log(`🗑️ Deleting file from S3 due to: ${reason} - ${key}`);

      const deleteCommand = new DeleteObjectCommand({
        Bucket: this.bucket,
        Key: key,
      });

      await s3Client.send(deleteCommand);
      this.metrics.totalDeleted++;
      console.log(`✅ File deleted from S3: ${key}`);

      return true;
    } catch (error) {
      console.error(`❌ Failed to delete file from S3: ${key}`, error.message);
      return false;
    }
  }

  /**
   * 🎯 Enhanced rate limiting for embedding API
   */
  async checkEmbeddingRateLimit() {
    const now = Date.now();

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

        if (expectedSize && receivedSize > 0) {
          const progress = Math.round((receivedSize / expectedSize) * 100);
          if (progress % 25 === 0) {
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

        // Track S3 access errors
        this.metrics.errorTypes.s3Access++;

        if (
          error.message.includes("too large") ||
          error.message.includes("not found") ||
          error.message.includes("NoSuchKey")
        ) {
          throw error;
        }

        if (attempt < MAX_RETRY_ATTEMPTS) {
          const delay = RETRY_DELAY_MS * Math.pow(2, attempt - 1);
          await this.delay(delay);
        }
      }
    }

    throw new Error(
      `Failed to download PDF after ${MAX_RETRY_ATTEMPTS} attempts: ${lastError.message}`
    );
  }

  /**
   * 🧠 Enhanced text extraction with better error handling
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
      const result = await extractFromPDF(pdfBuffer);

      if (
        !result ||
        !result.text ||
        result.text.trim().length < MIN_TEXT_LENGTH
      ) {
        throw new Error(
          `Insufficient text extracted: only ${
            result?.text?.length || 0
          } characters found`
        );
      }

      const extractionTime = Date.now() - startTime;
      console.log(
        `✅ Text extracted: ${result.text.length} characters in ${extractionTime}ms (method: ${result.method})`
      );

      // Cache the result
      this.setCachedText(key, result.text);

      return result.text;
    } catch (error) {
      console.error("❌ Text extraction failed:", error.message);
      this.metrics.errorTypes.textExtraction++;

      // Enhanced error classification
      if (
        error.message.includes("no extractable text") ||
        error.message.includes("image-based")
      ) {
        throw new Error(
          "PDF_IMAGE_BASED: This PDF appears to be image-based. Please convert it to a text-based PDF and try again."
        );
      }

      if (
        error.message.includes("corrupted") ||
        error.message.includes("invalid")
      ) {
        throw new Error(
          "PDF_CORRUPTED: PDF file appears to be corrupted. Please try uploading a different file."
        );
      }

      if (error.message.includes("password")) {
        throw new Error(
          "PDF_PROTECTED: PDF is password protected. Please remove the password and try again."
        );
      }

      throw new Error(`Text extraction failed: ${error.message}`);
    }
  }

  /**
   * ✂️ Enhanced text chunking with caching and validation
   */
  async chunkTextWithCache(text) {
    const textHash = this.simpleHash(text.substring(0, 1000));

    const cachedChunks = this.getCachedChunks(textHash);
    if (cachedChunks) {
      return cachedChunks;
    }

    console.log("✂️ Chunking text...");
    const startTime = Date.now();

    try {
      const chunks = await chunkText(text);

      if (!chunks || chunks.length === 0) {
        throw new Error(
          "No text chunks generated - text may be too short or invalid"
        );
      }

      // Validate chunk quality
      const validChunks = chunks.filter(
        chunk => chunk && typeof chunk === "string" && chunk.trim().length > 10
      );

      if (validChunks.length === 0) {
        throw new Error("All generated chunks are invalid or too short");
      }

      const chunkingTime = Date.now() - startTime;
      console.log(
        `✅ Generated ${validChunks.length} valid chunks in ${chunkingTime}ms`
      );

      this.setCachedChunks(textHash, validChunks);
      return validChunks;
    } catch (error) {
      console.error("❌ Text chunking failed:", error.message);
      this.metrics.errorTypes.chunking++;
      throw new Error(`Text chunking failed: ${error.message}`);
    }
  }

  /**
   * 🚀 Optimized batch embedding processing with enhanced error handling
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

    for (let i = 0; i < chunks.length; i += MAX_CONCURRENT_EMBEDDINGS) {
      const batch = chunks.slice(i, i + MAX_CONCURRENT_EMBEDDINGS);
      const batchNumber = Math.floor(i / MAX_CONCURRENT_EMBEDDINGS) + 1;
      const totalBatches = Math.ceil(chunks.length / MAX_CONCURRENT_EMBEDDINGS);

      console.log(
        `📦 Processing batch ${batchNumber}/${totalBatches} (${batch.length} chunks)`
      );

      try {
        const embeddingPromises = batch.map(async (chunk, index) => {
          const globalIndex = i + index;

          try {
            await this.checkEmbeddingRateLimit();

            const embedding = await this.retryOperation(
              () => getEmbedding(chunk),
              2,
              `Generate embedding for chunk ${globalIndex}`
            );

            return { chunk, embedding, globalIndex };
          } catch (error) {
            this.metrics.errorTypes.embedding++;
            throw new Error(
              `Chunk ${globalIndex} embedding failed: ${error.message}`
            );
          }
        });

        const embeddingResults = await Promise.allSettled(embeddingPromises);
        const uploadPromises = [];

        embeddingResults.forEach((result, index) => {
          const globalIndex = i + index;

          if (result.status === "fulfilled") {
            const { chunk, embedding } = result.value;

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
                    userId,
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
              this.metrics.errorTypes.upload++;
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

      const progressPercent = Math.round(
        (results.length / chunks.length) * 100
      );
      console.log(
        `📊 Progress: ${results.length}/${chunks.length} chunks processed (${progressPercent}%)`
      );

      if (i + MAX_CONCURRENT_EMBEDDINGS < chunks.length) {
        await this.delay(BATCH_PROCESSING_DELAY);
      }
    }

    const totalTime = Date.now() - startTime;
    console.log(
      `✅ Embedding processing complete: ${results.length} successful, ${errors.length} failed in ${totalTime}ms`
    );

    if (errors.length > 0) {
      console.warn("⚠️ Some chunks failed to process:", errors.slice(0, 5));
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
      hash = hash & hash;
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
      const fileExtension = path.extname(key).toLowerCase();
      if (!SUPPORTED_FILE_TYPES.includes(fileExtension)) {
        errors.push(
          `Unsupported file type: ${fileExtension}. Supported types: ${SUPPORTED_FILE_TYPES.join(
            ", "
          )}`
        );
      }

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

    if (errors.length > 0) {
      this.metrics.errorTypes.validation++;
    }

    return { isValid: errors.length === 0, errors };
  }

  /**
   * 📊 Update performance metrics
   */
  updateMetrics(processingTime, chunksProcessed, textLength, success = true) {
    if (success) {
      this.metrics.totalProcessed++;
      this.metrics.totalChunksProcessed += chunksProcessed;
      this.metrics.totalTextExtracted += textLength;

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
   * 🎯 Main optimized PDF processing method with auto-cleanup
   */
  async processUploadedPdf(req, res) {
    const startTime = Date.now();
    let shouldDeleteFile = false;
    let deleteReason = "";

    try {
      console.log("🚀 === Starting optimized PDF processing ===");

      const { key, collectionName } = req.body;
      const userId = req.user?.UserId;

      // Enhanced validation
      const validation = this.validateInput({ key, collectionName, userId });
      if (!validation.isValid) {
        shouldDeleteFile = true;
        deleteReason = "validation_failed";

        return handlers.response.failed({
          res,
          message: `Validation failed: ${validation.errors.join(", ")}`,
          statusCode: 400,
        });
      }

      const fileName = path.basename(key);
      console.log(`📄 Processing file: ${fileName} for user: ${userId}`);

      // Step 1: Download PDF from S3
      let pdfBuffer;
      try {
        pdfBuffer = await this.downloadPdfFromS3(key);
      } catch (error) {
        if (
          error.message.includes("too large") ||
          error.message.includes("not found")
        ) {
          shouldDeleteFile = true;
          deleteReason = "s3_download_failed";
        }
        throw error;
      }

      // Step 2: Extract text with enhanced error handling
      let text;
      try {
        text = await this.extractTextFromPdf(pdfBuffer, key);
      } catch (error) {
        shouldDeleteFile = true;
        deleteReason = "text_extraction_failed";
        throw error;
      }

      // Step 3: Chunk text with caching
      let chunks;
      try {
        chunks = await this.chunkTextWithCache(text);
      } catch (error) {
        shouldDeleteFile = true;
        deleteReason = "text_chunking_failed";
        throw error;
      }

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

      // Check if embedding processing was successful enough
      const successRate = embeddingResults.successCount / chunks.length;
      if (successRate < 0.5) {
        // Less than 50% success rate
        shouldDeleteFile = true;
        deleteReason = "embedding_processing_failed";
        throw new Error(
          `Embedding processing failed: only ${Math.round(
            successRate * 100
          )}% of chunks processed successfully`
        );
      }

      // Step 5: Save file metadata
      console.log("💾 Step 5: Saving file metadata...");
      try {
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
      } catch (error) {
        shouldDeleteFile = true;
        deleteReason = "metadata_save_failed";
        throw error;
      }

      const totalProcessingTime = Date.now() - startTime;

      // Update success metrics
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
          successRate: Math.round(
            (embeddingResults.successCount / chunks.length) * 100
          ),
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
              : undefined,
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

      // Delete file from S3 if processing failed
      if (shouldDeleteFile && req.body?.key) {
        console.log(`🗑️ Deleting failed file from S3: ${deleteReason}`);
        await this.deleteFileFromS3(req.body.key, deleteReason);
      }

      // Enhanced error response with user-friendly messages
      let userMessage = error.message;
      let suggestions = [];

      if (error.message.includes("PDF_IMAGE_BASED")) {
        userMessage =
          "This PDF appears to be image-based and cannot be processed.";
        suggestions.push("Convert the PDF to a text-based format");
        suggestions.push("Ensure the PDF contains selectable text");
      } else if (error.message.includes("PDF_CORRUPTED")) {
        userMessage = "The PDF file appears to be corrupted or invalid.";
        suggestions.push("Try re-saving or re-creating the PDF");
        suggestions.push("Upload a different PDF file");
      } else if (error.message.includes("PDF_PROTECTED")) {
        userMessage = "The PDF is password protected.";
        suggestions.push("Remove the password protection");
        suggestions.push("Upload an unprotected version");
      }

      return handlers.response.error({
        res,
        message: `PDF processing failed: ${userMessage}`,
        statusCode: error.statusCode || 500,
        data: {
          processingTimeMs: totalProcessingTime,
          stage: error.stage || deleteReason || "unknown",
          fileDeleted: shouldDeleteFile,
          deleteReason,
          suggestions,
          errorType: this.classifyError(error),
          error:
            process.env.NODE_ENV === "development" ? error.stack : undefined,
        },
      });
    }
  }

  /**
   * 🏷️ Classify error types for better tracking
   */
  classifyError(error) {
    const message = error.message.toLowerCase();

    if (
      message.includes("image-based") ||
      message.includes("no extractable text")
    ) {
      return "image_based_pdf";
    }
    if (message.includes("corrupted") || message.includes("invalid")) {
      return "corrupted_pdf";
    }
    if (message.includes("password") || message.includes("protected")) {
      return "protected_pdf";
    }
    if (message.includes("too large") || message.includes("size")) {
      return "file_too_large";
    }
    if (message.includes("embedding") || message.includes("vector")) {
      return "embedding_error";
    }
    if (message.includes("s3") || message.includes("download")) {
      return "s3_error";
    }
    if (message.includes("chunk")) {
      return "chunking_error";
    }

    return "unknown_error";
  }

  /**
   * 📊 Get enhanced service metrics
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
            cacheHitRate: this.calculateCacheHitRate(),
          },
          system: {
            uptime: process.uptime(),
            memoryUsage: process.memoryUsage(),
            nodeVersion: process.version,
          },
          processing: {
            successRate: this.calculateSuccessRate(),
            averageProcessingTime: Math.round(
              this.metrics.averageProcessingTime
            ),
            totalFilesProcessed: this.metrics.totalProcessed,
            totalErrors: this.metrics.totalErrors,
            totalFilesDeleted: this.metrics.totalDeleted,
          },
          errorBreakdown: this.metrics.errorTypes,
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
   * 📈 Calculate cache hit rate
   */
  calculateCacheHitRate() {
    const totalCacheAttempts = this.metrics.totalProcessed;
    if (totalCacheAttempts === 0) return 0;

    // This is a simplified calculation - you might want to track actual cache hits
    const estimatedHits = Math.min(
      this.textCache.size + this.chunkCache.size,
      totalCacheAttempts
    );
    return Math.round((estimatedHits / totalCacheAttempts) * 100);
  }

  /**
   * 📊 Calculate success rate
   */
  calculateSuccessRate() {
    const total = this.metrics.totalProcessed + this.metrics.totalErrors;
    if (total === 0) return 100;

    return Math.round((this.metrics.totalProcessed / total) * 100);
  }

  /**
   * 🧹 Clear caches with enhanced logging
   */
  async clearCaches(req, res) {
    try {
      const textCacheSize = this.textCache.size;
      const chunkCacheSize = this.chunkCache.size;

      this.textCache.clear();
      this.chunkCache.clear();

      console.log(
        `🧹 Document AI caches cleared: ${textCacheSize} text entries, ${chunkCacheSize} chunk entries`
      );

      return handlers.response.success({
        res,
        message: "Caches cleared successfully",
        data: {
          timestamp: new Date().toISOString(),
          clearedEntries: {
            textCache: textCacheSize,
            chunkCache: chunkCacheSize,
            total: textCacheSize + chunkCacheSize,
          },
        },
      });
    } catch (error) {
      return handlers.response.error({
        res,
        message: "Failed to clear caches",
        statusCode: 500,
      });
    }
  }

  /**
   * 🔍 Health check with comprehensive status
   */
  async healthCheck(req, res) {
    try {
      const health = {
        status: "healthy",
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        version: process.version,
        environment: process.env.NODE_ENV || "development",
        services: {
          s3: "unknown",
          dynamodb: "unknown",
          qdrant: "unknown",
        },
        metrics: {
          processedFiles: this.metrics.totalProcessed,
          errors: this.metrics.totalErrors,
          successRate: this.calculateSuccessRate(),
          cacheSize: this.textCache.size + this.chunkCache.size,
        },
      };

      // Quick S3 connectivity check
      try {
        await s3Client.send(
          new GetObjectCommand({
            Bucket: this.bucket,
            Key: "health-check-dummy-key",
          })
        );
        health.services.s3 = "healthy";
      } catch (error) {
        if (error.name === "NoSuchKey") {
          health.services.s3 = "healthy"; // Bucket accessible, key doesn't exist (expected)
        } else {
          health.services.s3 = "error";
          health.status = "degraded";
        }
      }

      return handlers.response.success({
        res,
        message: "Health check completed",
        data: health,
      });
    } catch (error) {
      return handlers.response.error({
        res,
        message: "Health check failed",
        statusCode: 503,
        data: {
          status: "unhealthy",
          error: error.message,
          timestamp: new Date().toISOString(),
        },
      });
    }
  }

  /**
   * 📋 Get processing statistics
   */
  async getProcessingStats(req, res) {
    try {
      const stats = {
        overview: {
          totalProcessed: this.metrics.totalProcessed,
          totalErrors: this.metrics.totalErrors,
          totalDeleted: this.metrics.totalDeleted,
          successRate: this.calculateSuccessRate(),
          averageProcessingTime: Math.round(this.metrics.averageProcessingTime),
        },
        performance: {
          totalChunksProcessed: this.metrics.totalChunksProcessed,
          totalTextExtracted: this.metrics.totalTextExtracted,
          averageChunksPerFile:
            this.metrics.totalProcessed > 0
              ? Math.round(
                  this.metrics.totalChunksProcessed /
                    this.metrics.totalProcessed
                )
              : 0,
          averageTextPerFile:
            this.metrics.totalProcessed > 0
              ? Math.round(
                  this.metrics.totalTextExtracted / this.metrics.totalProcessed
                )
              : 0,
        },
        errorBreakdown: this.metrics.errorTypes,
        cache: {
          textCacheEntries: this.textCache.size,
          chunkCacheEntries: this.chunkCache.size,
          estimatedHitRate: this.calculateCacheHitRate(),
        },
        system: {
          uptime: Math.round(process.uptime()),
          memoryUsageMB: Math.round(process.memoryUsage().rss / 1024 / 1024),
          nodeVersion: process.version,
        },
      };

      return handlers.response.success({
        res,
        message: "Processing statistics retrieved",
        data: stats,
      });
    } catch (error) {
      return handlers.response.error({
        res,
        message: "Failed to get processing statistics",
        statusCode: 500,
      });
    }
  }
}

module.exports = new OptimizedDocumentAIService();
