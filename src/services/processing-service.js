const { handlers } = require("../utilities/handlers");
const { extractFromPDF, extractTextAuto } = require("../utilities/pdf-parser"); // Updated import
const chunkText = require("../utilities/chunk-text"); // Updated import
const getEmbedding = require("../utilities/get-embedding");
const { v4: uuidv4 } = require("uuid");
const { s3Client } = require("../config/aws");
const { GetObjectCommand, DeleteObjectCommand } = require("@aws-sdk/client-s3");
const path = require("path");
const { saveFileDetails } = require("../utilities/save-query");
const { Readable } = require("stream");
const upsertEmbedding = require("../utilities/upsert-embedding");
const { pdfBufferToText } = require("../utilities/pdf-extractor");

// Universal Constants - Works for any document type
const MAX_CONCURRENT_EMBEDDINGS = 10;
const MAX_CONCURRENT_UPLOADS = 12;
const MAX_RETRY_ATTEMPTS = 3;
const RETRY_DELAY_MS = 500;
const SUPPORTED_FILE_TYPES = [".pdf"];
const MAX_FILE_SIZE = 100 * 1024 * 1024; // 100MB for any large documents
const STREAM_TIMEOUT_MS = 90000; // 90 seconds for large files
const BATCH_PROCESSING_DELAY = 50;
const MIN_TEXT_LENGTH = 100; // Minimum text for any content
const QUALITY_THRESHOLD = 0.3; // Lowered threshold for more flexibility

// Universal Rate limiting for document processing
const EMBEDDING_RATE_LIMIT = {
  maxRequests: 120,
  windowMs: 60000,
  requests: [],
};

class ProcessingService {
  constructor() {
    this.bucket = process.env.BUCKET_NAME;
    this.tableName = process.env.DYNAMODB_TABLE_NAME;

    // Universal metrics tracking
    this.metrics = {
      totalProcessed: 0,
      totalErrors: 0,
      totalDeleted: 0,
      averageProcessingTime: 0,
      totalChunksProcessed: 0,
      totalTextExtracted: 0,
      averageQualityScore: 0,
      digitalTextSuccess: 0,
      ocrFallbacks: 0,
      hybridExtractions: 0,
      errorTypes: {
        textExtraction: 0,
        chunking: 0,
        embedding: 0,
        upload: 0,
        s3Access: 0,
        validation: 0,
        qualityCheck: 0,
      },
      extractionMethods: {
        digitalOnly: 0,
        ocrOnly: 0,
        hybrid: 0,
        autoSelected: 0,
      },
    };

    // Universal caching
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
      "🚀 Universal Document Service initialized - Smart PDF extraction enabled"
    );
  }

  /**
   * Universal file deletion
   */
  async deleteFileFromS3(
    key,
    reason = "processing_failed",
    additionalInfo = {}
  ) {
    try {
      console.log(`🗑️ Deleting document from S3: ${reason} - ${key}`);

      const deleteCommand = new DeleteObjectCommand({
        Bucket: this.bucket,
        Key: key,
      });

      await s3Client.send(deleteCommand);
      this.metrics.totalDeleted++;

      console.log(`✅ Document deleted from S3: ${key}`);
      return true;
    } catch (error) {
      console.error(
        `❌ Failed to delete document from S3: ${key}`,
        error.message
      );
      return false;
    }
  }

  /**
   * Universal rate limiting
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
   * Universal caching
   */
  getCachedText(key) {
    const cached = this.textCache.get(key);
    if (cached && Date.now() - cached.timestamp < this.CACHE_TTL_MS) {
      console.log("⚡ Using cached document text");
      return cached.data;
    }
    this.textCache.delete(key);
    return null;
  }

  setCachedText(key, extractionResult) {
    this.textCache.set(key, {
      data: extractionResult,
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
      console.log("⚡ Using cached document chunks");
      return cached.chunks;
    }
    this.chunkCache.delete(textHash);
    return null;
  }

  setCachedChunks(textHash, chunks, metadata = {}) {
    this.chunkCache.set(textHash, {
      chunks,
      metadata,
      timestamp: Date.now(),
    });

    if (this.chunkCache.size > 30) {
      const oldestKey = this.chunkCache.keys().next().value;
      this.chunkCache.delete(oldestKey);
    }
  }

  /**
   * Universal stream to buffer
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
            console.log(`📥 Document download progress: ${progress}%`);
          }
        }
      });

      stream.on("end", () => {
        try {
          const buffer = Buffer.concat(chunks);
          console.log(
            `✅ Document stream completed: ${(
              buffer.length /
              1024 /
              1024
            ).toFixed(2)}MB`
          );
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
          new Error(
            `Document stream timeout after ${STREAM_TIMEOUT_MS / 1000} seconds`
          )
        );
      }, STREAM_TIMEOUT_MS);

      stream.on("end", () => clearTimeout(timeout));
      stream.on("error", () => clearTimeout(timeout));
    });
  }

  /**
   * Universal S3 download with better error handling
   */
  async downloadPdfFromS3(key) {
    let lastError;

    for (let attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
      try {
        console.log(
          `📥 Downloading document (attempt ${attempt}/${MAX_RETRY_ATTEMPTS}): ${key}`
        );

        const command = new GetObjectCommand({
          Bucket: this.bucket,
          Key: key,
        });

        const startTime = Date.now();
        const data = await s3Client.send(command);

        if (!data.Body) {
          throw new Error("No document data received from S3");
        }

        const contentLength = data.ContentLength;
        if (contentLength && contentLength > MAX_FILE_SIZE) {
          throw new Error(
            `Document too large: ${(contentLength / 1024 / 1024).toFixed(
              2
            )}MB (max: ${MAX_FILE_SIZE / 1024 / 1024}MB)`
          );
        }

        const buffer = await this.streamToBuffer(data.Body, contentLength);
        const downloadTime = Date.now() - startTime;

        console.log(
          `✅ Document downloaded: ${(buffer.length / 1024 / 1024).toFixed(
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
        this.metrics.errorTypes.s3Access++;

        if (
          error.message.includes("too large") ||
          error.message.includes("not found") ||
          error.message.includes("NoSuchKey") ||
          error.name === "NoSuchKey"
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
      `Failed to download document after ${MAX_RETRY_ATTEMPTS} attempts: ${lastError.message}`
    );
  }

  /**
   * IMPROVED: Smart text extraction with automatic strategy selection
   */
  async extractTextFromPdf(pdfBuffer, key, fileName) {
    const cachedResult = this.getCachedText(key);
    if (cachedResult) {
      return cachedResult;
    }

    console.log("🔍 Smart text extraction starting...");
    const startTime = Date.now();

    try {
      // Use the smart auto-extraction that chooses the best strategy
      const result = await pdfBufferToText(pdfBuffer);

      if (
        !result ||
        !result.text ||
        result.text.trim().length < MIN_TEXT_LENGTH
      ) {
        throw new Error(
          `Insufficient content extracted: only ${
            result?.text?.length || 0
          } characters found (min: ${MIN_TEXT_LENGTH})`
        );
      }

      // Quality assessment with more flexible thresholds
      if (result.qualityScore && result.qualityScore < QUALITY_THRESHOLD) {
        console.warn(
          `⚠️ Low quality content detected (score: ${result.qualityScore.toFixed(
            2
          )})`
        );

        // Only fail if quality is extremely low
        if (result.qualityScore < 0.1) {
          throw new Error(
            `CONTENT_QUALITY_TOO_LOW: Quality score ${result.qualityScore.toFixed(
              2
            )} below minimum threshold`
          );
        }
      }

      const extractionTime = Date.now() - startTime;
      console.log(
        `✅ Document text extracted: ${result.text.length} characters in ${extractionTime}ms`
      );
      console.log(
        `📊 Method: ${result.method}, Quality: ${
          result.qualityScore?.toFixed(2) || "N/A"
        }`
      );

      // Update extraction method metrics
      this.updateExtractionMethodMetrics(result.method);
      this.updateExtractionMetrics(result);

      // Cache the complete result
      this.setCachedText(key, result);

      return result;
    } catch (error) {
      console.error("❌ Document text extraction failed:", error.message);
      this.metrics.errorTypes.textExtraction++;

      // Better error classification
      if (error.message.includes("CONTENT_QUALITY_TOO_LOW")) {
        throw new Error(
          "QUALITY_INSUFFICIENT: This document has insufficient readable content for processing."
        );
      }

      if (
        error.message.includes("NoSuchKey") ||
        error.message.includes("not found")
      ) {
        throw new Error(
          "FILE_NOT_FOUND: The specified document could not be found."
        );
      }

      if (
        error.message.includes("timeout") ||
        error.message.includes("TIMEOUT")
      ) {
        throw new Error(
          "EXTRACTION_TIMEOUT: Document processing timed out. Try a smaller file."
        );
      }

      if (error.message.includes("OCR") && error.message.includes("failed")) {
        throw new Error(
          "OCR_FAILED: Unable to extract text from this image-based PDF. The document may be corrupted or have poor image quality."
        );
      }

      throw new Error(`Document text extraction failed: ${error.message}`);
    }
  }

  /**
   * IMPROVED: Universal text chunking with better content type detection
   */
  async chunkTextWithCache(text, fileName = "", extractionMethod = "unknown") {
    const textHash = this.simpleHash(text.substring(0, 1000));

    const cachedChunks = this.getCachedChunks(textHash);
    if (cachedChunks) {
      return cachedChunks;
    }

    console.log("✂️ Chunking document text with universal strategy...");
    const startTime = Date.now();

    try {
      // Detect content type from filename and text
      const contentType = this.detectContentType(text, fileName);
      console.log(`📋 Detected content type: ${contentType}`);

      const chunks = await chunkText(text, {
        chunkSize: Number(process.env.CHUNK_SIZE || 1350),
        chunkOverlap: Number(process.env.CHUNK_OVERLAP || 250),
        contentType: contentType,
        enhanceChunks: true,
      });

      if (!chunks || chunks.length === 0) {
        throw new Error(
          "No chunks generated - content may be too short or invalid"
        );
      }

      // Universal chunk validation with content-type awareness
      const validChunks = chunks.filter(chunk =>
        this.validateChunk(chunk, contentType)
      );

      if (validChunks.length === 0) {
        throw new Error("All generated chunks are invalid after validation");
      }

      const chunkingTime = Date.now() - startTime;
      console.log(
        `✅ Generated ${validChunks.length} valid chunks in ${chunkingTime}ms (${contentType})`
      );

      this.setCachedChunks(textHash, validChunks, {
        contentType,
        extractionMethod,
        chunkingTime,
      });
      return validChunks;
    } catch (error) {
      console.error("❌ Document text chunking failed:", error.message);
      this.metrics.errorTypes.chunking++;
      throw new Error(`Document text chunking failed: ${error.message}`);
    }
  }

  /**
   * Detect content type from text and filename
   */
  detectContentType(text, fileName = "") {
    const fileExt = path.extname(fileName).toLowerCase();
    const fileNameLower = fileName.toLowerCase();

    // Check filename patterns first
    if (
      fileNameLower.includes("drill") ||
      fileNameLower.includes("motor") ||
      fileNameLower.includes("bha")
    ) {
      return "TECHNICAL_DRILLING";
    }
    if (
      fileNameLower.includes("medical") ||
      fileNameLower.includes("patient") ||
      fileNameLower.includes("diagnosis")
    ) {
      return "MEDICAL";
    }
    if (
      fileNameLower.includes("contract") ||
      fileNameLower.includes("legal") ||
      fileNameLower.includes("agreement")
    ) {
      return "LEGAL";
    }
    if (
      fileNameLower.includes("financial") ||
      fileNameLower.includes("report") ||
      fileNameLower.includes("budget")
    ) {
      return "FINANCIAL";
    }

    // Analyze text content patterns
    const textSample = text.substring(0, 2000).toLowerCase();

    // Technical/Engineering patterns
    if (
      /\b(motor|stator|bit|drilling|wob|rop|rpm|pressure|torque)\b/i.test(
        textSample
      )
    ) {
      return "TECHNICAL_DRILLING";
    }

    // Medical patterns
    if (
      /\b(patient|diagnosis|treatment|medical|clinical|symptoms|prescription)\b/i.test(
        textSample
      )
    ) {
      return "MEDICAL";
    }

    // Legal patterns
    if (
      /\b(whereas|hereby|contract|agreement|clause|section|article)\b/i.test(
        textSample
      )
    ) {
      return "LEGAL";
    }

    // Financial patterns
    if (
      /\b(revenue|profit|financial|budget|investment|quarterly)\b/i.test(
        textSample
      )
    ) {
      return "FINANCIAL";
    }

    // Academic patterns
    if (
      /\b(abstract|methodology|research|study|analysis|conclusion)\b/i.test(
        textSample
      )
    ) {
      return "ACADEMIC";
    }

    // Code patterns
    if (
      /\b(function|class|import|const|var|def|public|private)\b/i.test(
        textSample
      )
    ) {
      return "CODE";
    }

    return "GENERAL";
  }

  /**
   * IMPROVED: Universal batch embedding processing with better error handling
   */
  async processEmbeddingsInBatches(
    chunks,
    collectionName,
    key,
    fileName,
    userId,
    extractionMetadata = {}
  ) {
    console.log(
      `🧠 Processing ${chunks.length} document chunks for embeddings`
    );

    const results = [];
    const errors = [];
    const startTime = Date.now();

    // Process chunks with progress tracking
    for (let i = 0; i < chunks.length; i++) {
      const chunk = `File/Document name: ${fileName}\nChunk ${i + 1}/${
        chunks.length
      }:\n${chunks[i]}`;

      console.log(`🔍 Processing chunk ${i + 1}/${chunks.length}`);

      try {
        // Rate limiting
        await this.checkEmbeddingRateLimit();

        // Generate embedding with retry logic
        const embedding = await this.retryOperation(
          async () => {
            const result = await getEmbedding(chunk, {
              model: "text-embedding-3-small",
            });
            return result;
          },
          3,
          `Generate embedding for chunk ${i + 1}`
        );

        // Upload to Qdrant with enhanced metadata
        const uploadResult = await this.retryOperation(
          async () => {
            const id = uuidv4();

            const payload = {
              key,
              name: fileName,
              content: chunk,
              chunkIndex: i,
              totalChunks: chunks.length,
              userId,
              createdAt: new Date().toISOString(),
              processingVersion: "universal-v2",
              extractionMethod: extractionMetadata.method || "unknown",
              extractionQuality: extractionMetadata.qualityScore || null,
              contentType: extractionMetadata.contentType || "general",
            };

            await upsertEmbedding({
              collectionName,
              points: [
                {
                  id,
                  vector: embedding,
                  payload: payload,
                },
              ],
            });

            return {
              id,
              content: chunk,
              embedding,
              chunkIndex: i,
              embeddingDimensions: embedding.length,
            };
          },
          3,
          `Upload chunk ${i + 1} to Qdrant`
        );

        results.push(uploadResult);
        console.log(`✅ Chunk ${i + 1} processed successfully`);
      } catch (error) {
        console.error(`❌ Chunk ${i + 1} failed:`, error.message);
        this.metrics.errorTypes.embedding++;
        errors.push({
          chunkIndex: i,
          error: error.message,
          chunkPreview: chunk.substring(0, 100),
          timestamp: new Date().toISOString(),
        });
      }

      // Progress update every 20%
      const progressPercent = Math.round(((i + 1) / chunks.length) * 100);
      if (progressPercent % 20 === 0) {
        console.log(
          `📊 Progress: ${progressPercent}% (${results.length} success, ${errors.length} failed)`
        );
      }

      // Small delay to prevent overwhelming the system
      if (i < chunks.length - 1) {
        await this.delay(BATCH_PROCESSING_DELAY);
      }
    }

    const totalTime = Date.now() - startTime;
    console.log(`🏁 Document embedding processing complete`);
    console.log(
      `✅ Successful: ${results.length}, ❌ Failed: ${errors.length}`
    );
    console.log(
      `⏱️ Total time: ${totalTime}ms, Avg per chunk: ${Math.round(
        totalTime / chunks.length
      )}ms`
    );

    return {
      results,
      errors,
      successCount: results.length,
      errorCount: errors.length,
      processingTimeMs: totalTime,
      averageTimePerChunk: Math.round(totalTime / chunks.length),
      successRate: (results.length / chunks.length) * 100,
    };
  }

  /**
   * IMPROVED: Universal chunk validation with content-type awareness
   */
  validateChunk(chunk, contentType = "GENERAL") {
    if (!chunk || typeof chunk !== "string" || chunk.trim().length < 20) {
      return false;
    }

    // Remove any enhancement prefixes to check core content
    const cleanChunk = chunk.replace(/^[A-Z_\s]+:\s*/g, "");

    // Basic content validation
    const wordCount = cleanChunk
      .split(/\s+/)
      .filter(word => word.length > 2).length;
    if (wordCount < 5) {
      return false;
    }

    // Content-type specific validation
    const contentIndicators = this.getContentIndicators(contentType);
    const hasRelevantContent = contentIndicators.some(pattern =>
      pattern.test(cleanChunk)
    );

    // General fallback validation
    if (!hasRelevantContent) {
      const generalIndicators = [
        /\w{3,}/g, // Words with 3+ letters
        /\d+/g, // Numbers
        /[.!?:;]/g, // Punctuation
      ];
      return generalIndicators.some(pattern => pattern.test(cleanChunk));
    }

    return hasRelevantContent;
  }

  /**
   * Get content indicators for different content types
   */
  getContentIndicators(contentType) {
    const indicators = {
      TECHNICAL_DRILLING: [
        /\b(motor|stator|bit|drilling|wob|rop|rpm|pressure|torque|bha)\b/i,
        /\d+\.?\d*\s*(klbs|rpm|gpm|psi|usft)/i,
      ],
      MEDICAL: [
        /\b(patient|diagnosis|treatment|medical|clinical|symptoms)\b/i,
        /\d+\s*(mg|ml|dose)/i,
      ],
      LEGAL: [
        /\b(contract|agreement|clause|section|article|whereas)\b/i,
        /(section|article)\s+\d+/i,
      ],
      FINANCIAL: [
        /\b(revenue|profit|financial|budget|investment)\b/i,
        /\$[\d,]+|\d+%/i,
      ],
      ACADEMIC: [
        /\b(research|study|analysis|methodology|conclusion)\b/i,
        /(figure|table)\s+\d+/i,
      ],
      CODE: [/\b(function|class|import|const|var|def)\b/i, /[{}();]/],
      GENERAL: [/\w{3,}/g, /\d+/g, /[.!?:;]/g],
    };

    return indicators[contentType] || indicators.GENERAL;
  }

  /**
   * Universal retry operation with exponential backoff
   */
  async retryOperation(operation, maxRetries = 3, operationName = "operation") {
    let lastError;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await operation();
      } catch (error) {
        lastError = error;
        console.warn(
          `⚠️ ${operationName} attempt ${attempt} failed: ${error.message}`
        );

        // Don't retry certain errors
        if (
          error.message?.includes("validation") ||
          error.message?.includes("authorization") ||
          error.message?.includes("not found") ||
          error.message?.includes("too large") ||
          error.name === "ValidationError"
        ) {
          throw error;
        }

        if (attempt < maxRetries) {
          const delay = RETRY_DELAY_MS * Math.pow(2, attempt - 1);
          console.log(`⏳ Retrying ${operationName} in ${delay}ms...`);
          await this.delay(delay);
        }
      }
    }

    throw new Error(
      `${operationName} failed after ${maxRetries} attempts: ${lastError.message}`
    );
  }

  /**
   * Universal input validation with better error messages
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

      if (key.length > 1024) {
        errors.push("File path too long");
      }
    }

    if (!collectionName?.trim()) {
      errors.push("Missing or invalid 'collectionName' parameter");
    } else if (!/^[a-zA-Z0-9_-]+$/.test(collectionName)) {
      errors.push(
        "Collection name contains invalid characters (only alphanumeric, underscore, and hyphen allowed)"
      );
    } else if (collectionName.length > 63) {
      errors.push("Collection name too long (max 63 characters)");
    }

    if (!userId?.trim()) {
      errors.push("Missing or invalid user ID");
    } else if (userId.length > 256) {
      errors.push("User ID too long");
    }

    if (errors.length > 0) {
      this.metrics.errorTypes.validation++;
    }

    return { isValid: errors.length === 0, errors };
  }

  /**
   * Update extraction method metrics
   */
  updateExtractionMethodMetrics(method) {
    if (method.includes("digital") && !method.includes("ocr")) {
      this.metrics.extractionMethods.digitalOnly++;
      this.metrics.digitalTextSuccess++;
    } else if (method.includes("ocr") && !method.includes("digital")) {
      this.metrics.extractionMethods.ocrOnly++;
      this.metrics.ocrFallbacks++;
    } else if (method.includes("hybrid")) {
      this.metrics.extractionMethods.hybrid++;
      this.metrics.hybridExtractions++;
    }

    if (method === "auto" || method.includes("auto")) {
      this.metrics.extractionMethods.autoSelected++;
    }
  }

  /**
   * Update universal metrics
   */
  updateMetrics(processingTime, chunksProcessed, textLength, success = true) {
    if (success) {
      this.metrics.totalProcessed++;
      this.metrics.totalChunksProcessed += chunksProcessed;
      this.metrics.totalTextExtracted += textLength;

      // Update rolling average for processing time
      const alpha = 0.1; // Smoothing factor
      this.metrics.averageProcessingTime =
        this.metrics.averageProcessingTime * (1 - alpha) +
        processingTime * alpha;
    } else {
      this.metrics.totalErrors++;
    }
  }

  updateExtractionMetrics(extractionResult) {
    if (extractionResult.qualityScore) {
      const currentAvg = this.metrics.averageQualityScore;
      const currentCount = this.metrics.totalProcessed;
      this.metrics.averageQualityScore =
        (currentAvg * currentCount + extractionResult.qualityScore) /
        (currentCount + 1);
    }
  }

  /**
   * Simple hash function for caching
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
   * Utility delay function
   */
  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * MAIN IMPROVED PDF PROCESSING METHOD
   */
  /**
   * Simplified `processUploadedPdf` with unconditional deletion on failure
   */

  async processUploadedPdf(req, res) {
    const startTime = Date.now();
    let processingStage = "init";
    const { key, collectionName } = req.body;
    const userId = req.user?.UserId;
    const fileName = path.basename(key);

    try {
      const validation = this.validateInput({ key, collectionName, userId });
      if (!validation.isValid) {
        await this.deleteFileFromS3(key, "validation_failed");
        return handlers.response.failed({
          res,
          message: `Validation failed: ${validation.errors.join(", ")}`,
          statusCode: 400,
        });
      }

      console.log(`📄 Processing: ${fileName}`);

      processingStage = "download";
      const pdfBuffer = await this.downloadPdfFromS3(key);

      processingStage = "extract";
      const text = await pdfBufferToText(pdfBuffer);

      processingStage = "chunk";
      const chunks = await chunkText(text, {
        chunkSize: Number(process.env.CHUNK_SIZE || 1350),
        chunkOverlap: Number(process.env.CHUNK_OVERLAP || 250),
      });

      processingStage = "embedding";
      const embeddingResults = await this.processEmbeddingsInBatches(
        chunks,
        collectionName,
        key,
        fileName,
        userId
      );

      processingStage = "save";
      await saveFileDetails({
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
      });

      const totalTime = Date.now() - startTime;
      this.updateMetrics(totalTime, embeddingResults.successCount, text.length);

      return handlers.response.success({
        res,
        message: "Processing complete",
        data: {
          fileName,
          totalChunks: chunks.length,
          successCount: embeddingResults.successCount,
          failedCount: embeddingResults.errorCount,
          processingTimeMs: totalTime,
        },
      });
    } catch (error) {
      console.error(`❌ Failed at ${processingStage}:`, error.message);
      await this.deleteFileFromS3(key, `failure_at_${processingStage}`);

      return handlers.response.error({
        res,
        message: `Processing failed at '${processingStage}': ${error.message}`,
        statusCode: 500,
      });
    }
  }

  /**
   * Enhanced service metrics with extraction method breakdown
   */
  async getMetrics(req, res) {
    try {
      return handlers.response.success({
        res,
        message: "Universal document service metrics",
        data: {
          performance: {
            totalProcessed: this.metrics.totalProcessed,
            totalErrors: this.metrics.totalErrors,
            successRate: this.calculateSuccessRate(),
            averageProcessingTime: Math.round(
              this.metrics.averageProcessingTime
            ),
            totalChunksProcessed: this.metrics.totalChunksProcessed,
            totalTextExtracted: this.metrics.totalTextExtracted,
            averageQualityScore: this.metrics.averageQualityScore.toFixed(3),
          },
          extractionMethods: {
            digitalTextSuccess: this.metrics.digitalTextSuccess,
            ocrFallbacks: this.metrics.ocrFallbacks,
            hybridExtractions: this.metrics.hybridExtractions,
            methodBreakdown: this.metrics.extractionMethods,
            digitalSuccessRate:
              this.metrics.totalProcessed > 0
                ? `${(
                    (this.metrics.digitalTextSuccess /
                      this.metrics.totalProcessed) *
                    100
                  ).toFixed(1)}%`
                : "0%",
            ocrFallbackRate:
              this.metrics.totalProcessed > 0
                ? `${(
                    (this.metrics.ocrFallbacks / this.metrics.totalProcessed) *
                    100
                  ).toFixed(1)}%`
                : "0%",
          },
          cache: {
            textCacheSize: this.textCache.size,
            chunkCacheSize: this.chunkCache.size,
            cacheHitRate: "N/A", // Could implement cache hit tracking
          },
          system: {
            uptime: Math.round(process.uptime()),
            memoryUsage: {
              rss: Math.round(process.memoryUsage().rss / 1024 / 1024),
              heapUsed: Math.round(
                process.memoryUsage().heapUsed / 1024 / 1024
              ),
              heapTotal: Math.round(
                process.memoryUsage().heapTotal / 1024 / 1024
              ),
            },
            nodeVersion: process.version,
            serviceType: "universal-document-processor",
            version: "universal-v2",
          },
          errorBreakdown: this.metrics.errorTypes,
        },
      });
    } catch (error) {
      return handlers.response.error({
        res,
        message: "Failed to get service metrics",
        statusCode: 500,
      });
    }
  }

  calculateSuccessRate() {
    const total = this.metrics.totalProcessed + this.metrics.totalErrors;
    if (total === 0) return 100;
    return Math.round((this.metrics.totalProcessed / total) * 100);
  }

  /**
   * Clear caches with detailed reporting
   */
  async clearCaches(req, res) {
    try {
      const textCacheSize = this.textCache.size;
      const chunkCacheSize = this.chunkCache.size;

      this.textCache.clear();
      this.chunkCache.clear();

      return handlers.response.success({
        res,
        message: "Caches cleared successfully",
        data: {
          clearedEntries: {
            textCache: textCacheSize,
            chunkCache: chunkCacheSize,
            total: textCacheSize + chunkCacheSize,
          },
          timestamp: new Date().toISOString(),
          memoryFreed: "Cache memory has been released",
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
   * Enhanced health check
   */
  async healthCheck(req, res) {
    try {
      const memUsage = process.memoryUsage();
      const health = {
        status: "healthy",
        timestamp: new Date().toISOString(),
        uptime: Math.round(process.uptime()),
        version: process.version,
        serviceType: "universal-document-processor",
        serviceVersion: "universal-v2",
        metrics: {
          processedFiles: this.metrics.totalProcessed,
          errors: this.metrics.totalErrors,
          successRate: this.calculateSuccessRate(),
          cacheSize: this.textCache.size + this.chunkCache.size,
        },
        system: {
          memoryUsageMB: Math.round(memUsage.rss / 1024 / 1024),
          heapUsedMB: Math.round(memUsage.heapUsed / 1024 / 1024),
          cpuUsage: process.cpuUsage(),
        },
        extractionCapabilities: {
          digitalText: "enabled",
          ocrFallback: "enabled",
          hybridExtraction: "enabled",
          autoStrategy: "enabled",
        },
      };

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
   * Enhanced processing statistics
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
          serviceType: "universal-v2",
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
          averageQualityScore: this.metrics.averageQualityScore.toFixed(3),
        },
        extractionAnalysis: {
          digitalOnlySuccess: this.metrics.extractionMethods.digitalOnly,
          ocrOnlyUsed: this.metrics.extractionMethods.ocrOnly,
          hybridExtractions: this.metrics.extractionMethods.hybrid,
          autoStrategyUsed: this.metrics.extractionMethods.autoSelected,
          digitalSuccessRate:
            this.metrics.totalProcessed > 0
              ? `${(
                  (this.metrics.digitalTextSuccess /
                    this.metrics.totalProcessed) *
                  100
                ).toFixed(1)}%`
              : "0%",
          ocrFallbackRate:
            this.metrics.totalProcessed > 0
              ? `${(
                  (this.metrics.ocrFallbacks / this.metrics.totalProcessed) *
                  100
                ).toFixed(1)}%`
              : "0%",
        },
        errorBreakdown: this.metrics.errorTypes,
        cache: {
          textCacheEntries: this.textCache.size,
          chunkCacheEntries: this.chunkCache.size,
          totalCacheEntries: this.textCache.size + this.chunkCache.size,
        },
        system: {
          uptime: Math.round(process.uptime()),
          memoryUsageMB: Math.round(process.memoryUsage().rss / 1024 / 1024),
          nodeVersion: process.version,
          version: "universal-v2",
          lastUpdated: new Date().toISOString(),
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

module.exports = new ProcessingService();
