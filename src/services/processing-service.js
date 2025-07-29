const { handlers } = require("../utilities/handlers");
const extractFromPDF = require("../utilities/pdf-parser");
const chunkText = require("../utilities/chunk-text");
const getEmbedding = require("../utilities/get-embedding");
const { v4: uuidv4 } = require("uuid");
const { s3Client } = require("../config/aws");
const { GetObjectCommand, DeleteObjectCommand } = require("@aws-sdk/client-s3");
const path = require("path");
const { saveFileDetails } = require("../utilities/save-query");
const { Readable } = require("stream");
const upsertEmbedding = require("../utilities/upsert-embedding");

// 🚀 Enhanced Constants for Drilling Reports
const MAX_CONCURRENT_EMBEDDINGS = 12; // Increased for better throughput
const MAX_CONCURRENT_UPLOADS = 15;
const MAX_RETRY_ATTEMPTS = 3;
const RETRY_DELAY_MS = 500;
const SUPPORTED_FILE_TYPES = [".pdf"];
const MAX_FILE_SIZE = 75 * 1024 * 1024; // Increased to 75MB for larger drilling reports
const STREAM_TIMEOUT_MS = 60000; // Increased timeout for large files
const BATCH_PROCESSING_DELAY = 30; // Reduced delay for faster processing
const MIN_TEXT_LENGTH = 100; // Higher minimum for technical content
const DRILLING_QUALITY_THRESHOLD = 0.6; // Quality threshold for drilling content

// 📊 Enhanced Rate limiting for drilling document processing
const EMBEDDING_RATE_LIMIT = {
  maxRequests: 150, // Increased for better throughput
  windowMs: 60000,
  requests: [],
};

class EnhancedDrillingDocumentService {
  constructor() {
    this.bucket = process.env.BUCKET_NAME;
    this.tableName = process.env.DYNAMODB_TABLE_NAME;

    // 📊 Enhanced metrics tracking for drilling reports
    this.metrics = {
      totalProcessed: 0,
      totalErrors: 0,
      totalDeleted: 0,
      averageProcessingTime: 0,
      totalChunksProcessed: 0,
      totalTextExtracted: 0,
      drillingSpecificMetrics: {
        bhaReports: 0,
        mmrReports: 0,
        rvenReports: 0,
        averageQualityScore: 0,
        structuredDataExtractions: 0,
        ocrFallbacks: 0,
      },
      errorTypes: {
        textExtraction: 0,
        chunking: 0,
        embedding: 0,
        upload: 0,
        s3Access: 0,
        validation: 0,
        qualityCheck: 0,
      },
    };

    // 🧠 Enhanced caching for drilling content
    this.textCache = new Map();
    this.chunkCache = new Map();
    this.qualityCache = new Map(); // Cache quality assessments
    this.CACHE_TTL_MS = 45 * 60 * 1000; // Extended to 45 minutes

    // Validate required environment variables
    if (!this.bucket || !this.tableName) {
      throw new Error(
        "Missing required environment variables: BUCKET_NAME, DYNAMODB_TABLE_NAME"
      );
    }

    console.log(
      "🚀 Enhanced Drilling Document Service initialized with advanced optimizations"
    );
  }

  /**
   * 🗑️ Enhanced file deletion with drilling-specific cleanup
   */
  async deleteFileFromS3(
    key,
    reason = "processing_failed",
    additionalInfo = {}
  ) {
    try {
      console.log(`🗑️ Deleting drilling report from S3: ${reason} - ${key}`);
      console.log(`📊 Additional info:`, additionalInfo);

      const deleteCommand = new DeleteObjectCommand({
        Bucket: this.bucket,
        Key: key,
      });

      await s3Client.send(deleteCommand);
      this.metrics.totalDeleted++;

      // Log drilling-specific deletion reasons
      if (reason.includes("quality")) {
        this.metrics.errorTypes.qualityCheck++;
      }

      console.log(`✅ Drilling report deleted from S3: ${key}`);
      return true;
    } catch (error) {
      console.error(
        `❌ Failed to delete drilling report from S3: ${key}`,
        error.message
      );
      return false;
    }
  }

  /**
   * 🎯 Enhanced rate limiting with drilling-specific optimizations
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
        console.log(
          `⏳ Rate limit reached for drilling embeddings, waiting ${waitTime}ms`
        );
        await this.delay(waitTime);
      }
    }

    EMBEDDING_RATE_LIMIT.requests.push(now);
  }

  /**
   * 💾 Enhanced caching with drilling-specific optimizations
   */
  getCachedText(key) {
    const cached = this.textCache.get(key);
    if (cached && Date.now() - cached.timestamp < this.CACHE_TTL_MS) {
      console.log("⚡ Using cached drilling report text");
      return cached.text;
    }
    this.textCache.delete(key);
    return null;
  }

  setCachedText(key, text, metadata = {}) {
    this.textCache.set(key, {
      text,
      metadata,
      timestamp: Date.now(),
    });

    // Enhanced cache management
    if (this.textCache.size > 75) {
      // Increased cache size
      const oldestKey = this.textCache.keys().next().value;
      this.textCache.delete(oldestKey);
    }
  }

  getCachedQuality(textHash) {
    const cached = this.qualityCache.get(textHash);
    if (cached && Date.now() - cached.timestamp < this.CACHE_TTL_MS) {
      console.log("⚡ Using cached quality assessment");
      return cached.quality;
    }
    this.qualityCache.delete(textHash);
    return null;
  }

  setCachedQuality(textHash, quality) {
    this.qualityCache.set(textHash, {
      quality,
      timestamp: Date.now(),
    });

    if (this.qualityCache.size > 50) {
      const oldestKey = this.qualityCache.keys().next().value;
      this.qualityCache.delete(oldestKey);
    }
  }

  /**
   * 🔧 Enhanced stream to buffer with drilling-specific progress tracking
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
          if (progress % 20 === 0) {
            // More frequent updates for large drilling reports
            console.log(`📥 Drilling report download progress: ${progress}%`);
          }
        }
      });

      stream.on("end", () => {
        try {
          const buffer = Buffer.concat(chunks);
          console.log(
            `✅ Drilling report stream completed: ${(
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
            `Drilling report stream timeout after ${
              STREAM_TIMEOUT_MS / 1000
            } seconds`
          )
        );
      }, STREAM_TIMEOUT_MS);

      stream.on("end", () => clearTimeout(timeout));
      stream.on("error", () => clearTimeout(timeout));
    });
  }

  /**
   * 📥 Enhanced S3 download with drilling report optimizations
   */
  async downloadPdfFromS3(key) {
    let lastError;

    for (let attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
      try {
        console.log(
          `📥 Downloading drilling report (attempt ${attempt}/${MAX_RETRY_ATTEMPTS}): ${key}`
        );

        const command = new GetObjectCommand({
          Bucket: this.bucket,
          Key: key,
        });

        const startTime = Date.now();
        const data = await s3Client.send(command);

        if (!data.Body) {
          throw new Error("No drilling report data received from S3");
        }

        const contentLength = data.ContentLength;
        if (contentLength && contentLength > MAX_FILE_SIZE) {
          throw new Error(
            `Drilling report too large: ${(contentLength / 1024 / 1024).toFixed(
              2
            )}MB (max: ${MAX_FILE_SIZE / 1024 / 1024}MB)`
          );
        }

        const buffer = await this.streamToBuffer(data.Body, contentLength);
        const downloadTime = Date.now() - startTime;

        console.log(
          `✅ Drilling report downloaded: ${(
            buffer.length /
            1024 /
            1024
          ).toFixed(2)}MB in ${downloadTime}ms`
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
      `Failed to download drilling report after ${MAX_RETRY_ATTEMPTS} attempts: ${lastError.message}`
    );
  }

  /**
   * 🧠 Enhanced text extraction with drilling report optimization
   */
  async extractTextFromPdf(pdfBuffer, key) {
    // Check cache first
    const cachedText = this.getCachedText(key);
    if (cachedText) {
      return cachedText;
    }

    console.log(
      "🔍 Extracting text from drilling report with enhanced methods..."
    );
    const startTime = Date.now();

    try {
      // Detect document type from filename
      const documentType = this.detectDocumentTypeFromFilename(key);
      console.log(
        `📋 Detected document type: ${documentType || "auto-detect"}`
      );

      // Enhanced extraction with drilling optimizations
      const result = await extractFromPDF(pdfBuffer, {
        preserveStructure: true,
        enhanceDrillingTerms: true,
        documentType: documentType,
      });

      if (
        !result ||
        !result.text ||
        result.text.trim().length < MIN_TEXT_LENGTH
      ) {
        throw new Error(
          `Insufficient drilling content extracted: only ${
            result?.text?.length || 0
          } characters found (min: ${MIN_TEXT_LENGTH})`
        );
      }

      // Quality assessment for drilling content
      if (result.qualityScore < DRILLING_QUALITY_THRESHOLD) {
        console.warn(
          `⚠️ Low quality drilling content detected (score: ${result.qualityScore.toFixed(
            2
          )})`
        );

        // Still process but flag as low quality
        if (result.qualityScore < 0.3) {
          throw new Error(
            `DRILLING_CONTENT_QUALITY_TOO_LOW: Quality score ${result.qualityScore.toFixed(
              2
            )} below minimum threshold`
          );
        }
      }

      const extractionTime = Date.now() - startTime;
      console.log(
        `✅ Drilling report text extracted: ${
          result.text.length
        } characters in ${extractionTime}ms (method: ${
          result.method
        }, quality: ${result.qualityScore.toFixed(2)})`
      );

      // Update drilling-specific metrics
      this.updateDrillingMetrics(result);

      // Cache the result with metadata
      this.setCachedText(key, result.text, {
        method: result.method,
        qualityScore: result.qualityScore,
        documentType: result.documentType,
      });

      return result.text;
    } catch (error) {
      console.error(
        "❌ Drilling report text extraction failed:",
        error.message
      );
      this.metrics.errorTypes.textExtraction++;

      // Enhanced error classification for drilling reports
      if (error.message.includes("DRILLING_CONTENT_QUALITY_TOO_LOW")) {
        throw new Error(
          "DRILLING_QUALITY_INSUFFICIENT: This drilling report has insufficient readable content. Please ensure the PDF contains clear, structured drilling data."
        );
      }

      if (
        error.message.includes("no extractable text") ||
        error.message.includes("image-based")
      ) {
        throw new Error(
          "DRILLING_PDF_IMAGE_BASED: This drilling report appears to be image-based. Please convert it to a text-based PDF with selectable drilling data."
        );
      }

      if (
        error.message.includes("corrupted") ||
        error.message.includes("invalid")
      ) {
        throw new Error(
          "DRILLING_PDF_CORRUPTED: Drilling report PDF appears to be corrupted. Please upload a valid drilling report."
        );
      }

      throw new Error(
        `Drilling report text extraction failed: ${error.message}`
      );
    }
  }

  // Add these missing methods to your EnhancedDrillingDocumentService class

  /**
   * 💾 Get cached chunks
   */
  getCachedChunks(textHash) {
    const cached = this.chunkCache.get(textHash);
    if (cached && Date.now() - cached.timestamp < this.CACHE_TTL_MS) {
      console.log("⚡ Using cached drilling report chunks");
      return cached.chunks;
    }
    this.chunkCache.delete(textHash);
    return null;
  }

  /**
   * 💾 Set cached chunks
   */
  setCachedChunks(textHash, chunks, metadata = {}) {
    this.chunkCache.set(textHash, {
      chunks,
      metadata,
      timestamp: Date.now(),
    });

    // Enhanced cache management
    if (this.chunkCache.size > 50) {
      const oldestKey = this.chunkCache.keys().next().value;
      this.chunkCache.delete(oldestKey);
    }
  }

  /**
   * ✂️ Enhanced text chunking with drilling-specific optimizations
   */
  async chunkTextWithCache(text) {
    const textHash = this.simpleHash(text.substring(0, 1500)); // Larger sample for drilling content

    const cachedChunks = this.getCachedChunks(textHash);
    if (cachedChunks) {
      return cachedChunks;
    }

    console.log("✂️ Chunking drilling report text with enhanced algorithm...");
    const startTime = Date.now();

    try {
      // Enhanced chunking optimized for drilling reports
      const chunks = await chunkText(text);

      if (!chunks || chunks.length === 0) {
        throw new Error(
          "No drilling report chunks generated - content may be too short or invalid"
        );
      }

      // Enhanced chunk validation for drilling content
      const validChunks = chunks.filter(chunk =>
        this.validateDrillingChunk(chunk)
      );

      if (validChunks.length === 0) {
        throw new Error(
          "All generated chunks are invalid for drilling analysis"
        );
      }

      // Quality assessment for chunks
      const averageChunkQuality = this.assessChunksQuality(validChunks);
      console.log(
        `📊 Average chunk quality for drilling content: ${averageChunkQuality.toFixed(
          2
        )}`
      );

      const chunkingTime = Date.now() - startTime;
      console.log(
        `✅ Generated ${validChunks.length} valid drilling chunks in ${chunkingTime}ms`
      );

      this.setCachedChunks(textHash, validChunks);
      return validChunks;
    } catch (error) {
      console.error("❌ Drilling report text chunking failed:", error.message);
      this.metrics.errorTypes.chunking++;
      throw new Error(`Drilling report text chunking failed: ${error.message}`);
    }
  }

  /**
   * 🚀 Enhanced batch embedding processing for drilling reports - with comprehensive debugging
   */
  async processEmbeddingsInBatches(
    chunks,
    collectionName,
    key,
    fileName,
    userId
  ) {
    console.log(
      `🧠 Processing ${chunks.length} drilling report chunks with enhanced embeddings`
    );

    // Pre-flight checks
    console.log("🔍 Pre-flight checks:");
    console.log("  - Collection name:", collectionName);
    console.log("  - User ID:", userId);
    console.log("  - File name:", fileName);
    console.log("  - Key:", key);

    // Check chunks
    console.log("📋 Chunk analysis:");
    chunks.forEach((chunk, i) => {
      console.log(
        `  Chunk ${i}: ${chunk.length} chars - "${chunk.substring(0, 50)}..."`
      );
    });

    const results = [];
    const errors = [];
    const startTime = Date.now();

    // Process one chunk at a time for debugging
    for (let i = 0; i < chunks.length; i++) {
      const chunk = `File/Document name: ${fileName}\nChunk ${i + 1}/${
        chunks.length
      }:\n${chunks[i]}`;
      console.log(`\n🔍 === Processing chunk ${i + 1}/${chunks.length} ===`);
      console.log(`Chunk content: "${chunk.substring(0, 200)}..."`);
      console.log(`Chunk length: ${chunk.length} characters`);

      try {
        // Test 1: Rate limiting
        console.log("⏳ Checking rate limit...");
        await this.checkEmbeddingRateLimit();
        console.log("✅ Rate limit check passed");

        // Test 2: Embedding generation
        console.log("🧠 Generating embedding...");
        console.log("  - Using enhanced drilling context: true");
        console.log("  - Model: text-embedding-3-small");

        const embeddingStartTime = Date.now();
        const embedding = await this.retryOperation(
          async () => {
            console.log("  🔄 Calling getEmbedding...");
            const result = await getEmbedding(chunk, {
              enhanceDrillingContext: true,
              model: "text-embedding-3-small",
            });
            console.log(
              `  ✅ getEmbedding returned result with length: ${result?.length}`
            );
            return result;
          },
          3,
          `Generate drilling embedding for chunk ${i}`
        );

        const embeddingTime = Date.now() - embeddingStartTime;
        console.log(`✅ Embedding generated in ${embeddingTime}ms`);
        console.log(`   - Embedding length: ${embedding?.length}`);
        console.log(
          `   - First few values: [${embedding?.slice(0, 3).join(", ")}...]`
        );

        // Test 3: Upload to Qdrant
        console.log("📤 Uploading to Qdrant...");
        const uploadStartTime = Date.now();

        const uploadResult = await this.retryOperation(
          async () => {
            const id = uuidv4();
            console.log(`  🆔 Generated ID: ${id}`);

            // Simplified payload for debugging
            const payload = {
              key,
              name: fileName,
              content: chunk,
              chunkIndex: i,
              totalChunks: chunks.length,
              userId,
              createdAt: new Date().toISOString(),
              processingVersion: "2.0-drilling-optimized",
            };

            console.log("  📦 Payload prepared, calling upsertEmbedding...");
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

            console.log("  ✅ upsertEmbedding completed successfully");
            return {
              id,
              content: chunk,
              embedding,
              chunkIndex: i,
            };
          },
          3,
          `Upload drilling chunk ${i} to Qdrant`
        );

        const uploadTime = Date.now() - uploadStartTime;
        console.log(`✅ Upload completed in ${uploadTime}ms`);

        results.push(uploadResult);
        console.log(
          `🎉 Chunk ${i + 1} processed successfully! Total success: ${
            results.length
          }`
        );
      } catch (error) {
        console.error(`❌ Chunk ${i + 1} failed:`, error.message);
        console.error(`❌ Error stack:`, error.stack);
        console.error(`❌ Chunk that failed: "${chunk.substring(0, 100)}..."`);

        this.metrics.errorTypes.embedding++;
        errors.push({
          chunkIndex: i,
          error: error.message,
          stage: error.message.includes("embedding")
            ? "embedding"
            : error.message.includes("upsert")
            ? "upload"
            : "unknown",
          chunkPreview: chunk.substring(0, 100),
        });
      }

      // Progress update
      const progressPercent = Math.round(((i + 1) / chunks.length) * 100);
      console.log(
        `📊 Progress: ${i + 1}/${
          chunks.length
        } (${progressPercent}%) - Success: ${results.length}, Failed: ${
          errors.length
        }`
      );
    }

    const totalTime = Date.now() - startTime;
    console.log(`\n🏁 === Drilling embedding processing complete ===`);
    console.log(`✅ Successful: ${results.length}`);
    console.log(`❌ Failed: ${errors.length}`);
    console.log(`⏱️ Total time: ${totalTime}ms`);
    console.log(
      `📈 Success rate: ${Math.round((results.length / chunks.length) * 100)}%`
    );

    if (errors.length > 0) {
      console.warn("\n⚠️ Error summary:");
      errors.forEach((error, index) => {
        console.warn(
          `  ${index + 1}. Chunk ${error.chunkIndex}: ${error.error} (${
            error.stage
          })`
        );
      });
    }

    return {
      results,
      errors,
      successCount: results.length,
      errorCount: errors.length,
      processingTimeMs: totalTime,
      averageTimePerChunk: totalTime / chunks.length,
      drillingOptimized: true,
    };
  }

  /**
   * 📊 Update drilling-specific metrics
   */
  updateDrillingMetrics(extractionResult) {
    if (extractionResult.documentType) {
      const docType = extractionResult.documentType.toLowerCase();
      if (docType.includes("bha"))
        this.metrics.drillingSpecificMetrics.bhaReports++;
      else if (docType.includes("mmr"))
        this.metrics.drillingSpecificMetrics.mmrReports++;
      else if (docType.includes("rven"))
        this.metrics.drillingSpecificMetrics.rvenReports++;
    }

    if (extractionResult.qualityScore) {
      const currentAvg =
        this.metrics.drillingSpecificMetrics.averageQualityScore;
      const currentCount = this.metrics.totalProcessed;
      this.metrics.drillingSpecificMetrics.averageQualityScore =
        (currentAvg * currentCount + extractionResult.qualityScore) /
        (currentCount + 1);
    }

    if (
      extractionResult.text &&
      extractionResult.text.includes("STRUCTURED DATA")
    ) {
      this.metrics.drillingSpecificMetrics.structuredDataExtractions++;
    }

    if (
      extractionResult.method &&
      (extractionResult.method.includes("ocr") ||
        extractionResult.method.includes("tesseract"))
    ) {
      this.metrics.drillingSpecificMetrics.ocrFallbacks++;
    }
  }

  /**
   * 🔍 Detect document type from filename
   */
  detectDocumentTypeFromFilename(filename) {
    const name = filename.toLowerCase();
    if (name.includes("bha")) return "BHA";
    if (name.includes("mmr")) return "MMR";
    if (name.includes("rven")) return "RVEN";
    return null;
  }

  /**
   * ✅ Validate drilling chunk content
   */
  validateDrillingChunk(chunk) {
    if (!chunk || typeof chunk !== "string" || chunk.trim().length < 30) {
      return false;
    }

    // Check for drilling-relevant content
    const drillingIndicators = [
      /\d+\.?\d*\s*(?:klbs|rpm|gpm|psi|usft|degf)/i, // Technical units
      /motor|bit|bha|stator|drilling|performance|rop|wob/i, // Drilling terms
      /make|model|grade|vendor|specs|tfa/i, // Equipment terms
    ];

    return drillingIndicators.some(pattern => pattern.test(chunk));
  }

  /**
   * 📊 Assess quality of chunks for drilling content
   */
  assessChunksQuality(chunks) {
    let totalQuality = 0;

    chunks.forEach(chunk => {
      let quality = 0;

      // Technical content indicators
      if (/\d+\.?\d*\s*(?:klbs|rpm|gpm|psi)/i.test(chunk)) quality += 0.3;
      if (/motor|stator|bit|bha/i.test(chunk)) quality += 0.3;
      if (/drilling|performance|rop|wob/i.test(chunk)) quality += 0.2;
      if (/STRUCTURED DATA|METRICS:/i.test(chunk)) quality += 0.2;

      totalQuality += Math.min(quality, 1.0);
    });

    return totalQuality / chunks.length;
  }

  /**
   * 🏷️ Identify chunk section type for drilling reports
   */
  identifyChunkSectionType(chunk) {
    if (/MOTOR.*STATOR.*SPECIFICATIONS/i.test(chunk))
      return "motor_stator_specs";
    if (/BIT.*CONFIGURATION.*SPECS/i.test(chunk)) return "bit_specifications";
    if (/BHA.*ASSEMBLY.*DETAILS/i.test(chunk)) return "bha_assembly";
    if (/DRILLING.*PERFORMANCE.*METRICS/i.test(chunk))
      return "performance_metrics";
    if (/OPERATIONAL.*DATA/i.test(chunk)) return "operational_data";
    if (/STRUCTURED DATA/i.test(chunk)) return "structured_summary";
    return "general_drilling";
  }

  /**
   * 🔄 Enhanced retry operation with drilling-specific error handling
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

        // Don't retry certain drilling-specific errors
        if (
          error.message?.includes("DRILLING_CONTENT_QUALITY_TOO_LOW") ||
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
   * 🔢 Enhanced hash function
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
   * ✅ Enhanced input validation for drilling reports
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

      // Enhanced validation for drilling report filenames
      const fileName = path.basename(key).toLowerCase();
      if (
        !fileName.includes("bha") &&
        !fileName.includes("mmr") &&
        !fileName.includes("rven") &&
        !fileName.includes("drill")
      ) {
        console.warn(
          `⚠️ Filename '${fileName}' doesn't appear to be a standard drilling report`
        );
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
   * 📊 Enhanced performance metrics update
   */
  updateMetrics(
    processingTime,
    chunksProcessed,
    textLength,
    success = true,
    additionalData = {}
  ) {
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

    // Update drilling-specific metrics if provided
    if (additionalData.documentType) {
      this.updateDrillingMetrics(additionalData);
    }
  }

  /**
   * ⏱️ Utility function for delays
   */
  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * 🎯 Main enhanced PDF processing method for drilling reports
   */
  async processUploadedPdf(req, res) {
    const startTime = Date.now();
    let shouldDeleteFile = false;
    let deleteReason = "";
    let drillingMetadata = {};

    try {
      console.log("🚀 === Starting enhanced drilling report processing ===");

      const { key, collectionName } = req.body;
      const userId = req.user?.UserId;

      // Enhanced validation
      const validation = this.validateInput({ key, collectionName, userId });
      if (!validation.isValid) {
        shouldDeleteFile = true;
        deleteReason = "validation_failed";

        return handlers.response.failed({
          res,
          message: `Drilling report validation failed: ${validation.errors.join(
            ", "
          )}`,
          statusCode: 400,
        });
      }

      const fileName = path.basename(key);
      const documentType = this.detectDocumentTypeFromFilename(key);
      console.log(
        `📄 Processing drilling report: ${fileName} (type: ${
          documentType || "auto-detect"
        }) for user: ${userId}`
      );

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

      // Step 2: Extract text with enhanced drilling report processing
      let text;
      try {
        text = await this.extractTextFromPdf(pdfBuffer, key);
        drillingMetadata.extractionMethod = "enhanced";
        drillingMetadata.documentType =
          this.detectDocumentTypeFromFilename(key);
      } catch (error) {
        shouldDeleteFile = true;
        deleteReason = "text_extraction_failed";
        drillingMetadata.extractionError = error.message;
        throw error;
      }

      // Step 3: Chunk text with drilling-specific optimizations
      let chunks;
      try {
        chunks = await this.chunkTextWithCache(text);
        drillingMetadata.chunkCount = chunks.length;
        drillingMetadata.averageChunkLength = Math.round(
          text.length / chunks.length
        );
      } catch (error) {
        shouldDeleteFile = true;
        deleteReason = "text_chunking_failed";
        throw error;
      }

      // Step 4: Process embeddings with drilling optimizations
      console.log(
        "🧠 Step 4: Processing embeddings with drilling-specific enhancements..."
      );
      const embeddingResults = await this.processEmbeddingsInBatches(
        chunks,
        collectionName,
        key,
        fileName,
        userId
      );

      // Enhanced success rate assessment for drilling content
      const successRate = embeddingResults.successCount / chunks.length;
      drillingMetadata.embeddingSuccessRate = successRate;

      if (successRate < 0.6) {
        // Higher threshold for drilling reports
        shouldDeleteFile = true;
        deleteReason = "embedding_processing_failed";
        throw new Error(
          `Drilling report embedding processing failed: only ${Math.round(
            successRate * 100
          )}% of chunks processed successfully (minimum 60% required for drilling analysis)`
        );
      }

      // Step 5: Save enhanced file metadata
      console.log("💾 Step 5: Saving enhanced drilling report metadata...");
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
              // Enhanced drilling-specific metadata
              documentType: drillingMetadata.documentType,
              extractionMethod: drillingMetadata.extractionMethod,
              embeddingSuccessRate: drillingMetadata.embeddingSuccessRate,
              averageChunkLength: drillingMetadata.averageChunkLength,
              isDrillingReport: true,
              processingVersion: "2.0-drilling-optimized",
            }),
          2,
          "Save drilling report metadata"
        );
      } catch (error) {
        shouldDeleteFile = true;
        deleteReason = "metadata_save_failed";
        throw error;
      }

      const totalProcessingTime = Date.now() - startTime;

      // Update enhanced success metrics
      this.updateMetrics(
        totalProcessingTime,
        embeddingResults.successCount,
        text.length,
        true,
        drillingMetadata
      );

      console.log(
        `🎉 === Drilling report processing completed in ${totalProcessingTime}ms ===`
      );

      return handlers.response.success({
        res,
        message: "Drilling report processed and embeddings stored successfully",
        data: {
          fileName,
          documentType: drillingMetadata.documentType,
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
          // Enhanced drilling-specific response data
          drillingMetadata: {
            extractionMethod: drillingMetadata.extractionMethod,
            averageChunkLength: drillingMetadata.averageChunkLength,
            embeddingSuccessRate: Math.round(
              drillingMetadata.embeddingSuccessRate * 100
            ),
            isDrillingOptimized: true,
            processingVersion: "2.0-drilling-optimized",
          },
          performance: {
            avgChunkProcessingTime:
              embeddingResults.processingTimeMs / chunks.length,
            throughputChunksPerSecond: Math.round(
              (embeddingResults.successCount /
                embeddingResults.processingTimeMs) *
                1000
            ),
            drillingOptimized: true,
          },
          errors:
            embeddingResults.errors.length > 0
              ? embeddingResults.errors.slice(0, 10)
              : undefined,
        },
      });
    } catch (error) {
      const totalProcessingTime = Date.now() - startTime;

      // Update error metrics with drilling context
      this.updateMetrics(totalProcessingTime, 0, 0, false, drillingMetadata);

      console.error(
        `❌ Drilling report processing failed after ${totalProcessingTime}ms:`,
        error
      );

      // Delete file from S3 if processing failed
      if (shouldDeleteFile && req.body?.key) {
        console.log(
          `🗑️ Deleting failed drilling report from S3: ${deleteReason}`
        );
        await this.deleteFileFromS3(
          req.body.key,
          deleteReason,
          drillingMetadata
        );
      }

      // Enhanced error response with drilling-specific messages
      let userMessage = error.message;
      let suggestions = [];

      if (error.message.includes("DRILLING_QUALITY_INSUFFICIENT")) {
        userMessage =
          "This drilling report has insufficient readable content for analysis.";
        suggestions.push(
          "Ensure the PDF contains clear, structured drilling data"
        );
        suggestions.push(
          "Verify all technical tables and performance metrics are visible"
        );
      } else if (error.message.includes("DRILLING_PDF_IMAGE_BASED")) {
        userMessage =
          "This drilling report appears to be image-based and cannot be processed.";
        suggestions.push(
          "Convert the PDF to text-based format with selectable drilling data"
        );
        suggestions.push(
          "Ensure BHA tables, motor specs, and performance data are text-based"
        );
      } else if (error.message.includes("PDF_CORRUPTED")) {
        userMessage =
          "The drilling report PDF appears to be corrupted or invalid.";
        suggestions.push("Try re-saving the drilling report PDF");
        suggestions.push("Upload a different version of the report");
      } else if (error.message.includes("embedding_processing_failed")) {
        userMessage = "Failed to process drilling report data for analysis.";
        suggestions.push(
          "Verify the report contains standard drilling terminology"
        );
        suggestions.push(
          "Check that technical data (ROP, WOB, motor specs) is clearly formatted"
        );
      }

      return handlers.response.error({
        res,
        message: `Drilling report processing failed: ${userMessage}`,
        statusCode: error.statusCode || 500,
        data: {
          processingTimeMs: totalProcessingTime,
          stage: error.stage || deleteReason || "unknown",
          fileDeleted: shouldDeleteFile,
          deleteReason,
          suggestions,
          errorType: this.classifyDrillingError(error),
          drillingMetadata,
          processingVersion: "2.0-drilling-optimized",
          error:
            process.env.NODE_ENV === "development" ? error.stack : undefined,
        },
      });
    }
  }

  /**
   * 🏷️ Enhanced error classification for drilling reports
   */
  classifyDrillingError(error) {
    const message = error.message.toLowerCase();

    if (
      message.includes("drilling_quality_insufficient") ||
      message.includes("quality_too_low")
    ) {
      return "drilling_content_quality_low";
    }
    if (
      message.includes("drilling_pdf_image_based") ||
      message.includes("no extractable text")
    ) {
      return "drilling_image_based_pdf";
    }
    if (message.includes("corrupted") || message.includes("invalid")) {
      return "drilling_corrupted_pdf";
    }
    if (message.includes("too large") || message.includes("size")) {
      return "drilling_file_too_large";
    }
    if (message.includes("embedding") || message.includes("vector")) {
      return "drilling_embedding_error";
    }
    if (message.includes("s3") || message.includes("download")) {
      return "drilling_s3_error";
    }
    if (message.includes("chunk")) {
      return "drilling_chunking_error";
    }
    if (message.includes("validation")) {
      return "drilling_validation_error";
    }

    return "drilling_unknown_error";
  }

  /**
   * 📊 Enhanced service metrics for drilling reports
   */
  async getMetrics(req, res) {
    try {
      return handlers.response.success({
        res,
        message: "Enhanced drilling document service metrics",
        data: {
          performance: this.metrics,
          drillingSpecific: {
            ...this.metrics.drillingSpecificMetrics,
            documentTypeDistribution: {
              bhaReports: this.metrics.drillingSpecificMetrics.bhaReports,
              mmrReports: this.metrics.drillingSpecificMetrics.mmrReports,
              rvenReports: this.metrics.drillingSpecificMetrics.rvenReports,
              total:
                this.metrics.drillingSpecificMetrics.bhaReports +
                this.metrics.drillingSpecificMetrics.mmrReports +
                this.metrics.drillingSpecificMetrics.rvenReports,
            },
            qualityMetrics: {
              averageQualityScore:
                this.metrics.drillingSpecificMetrics.averageQualityScore.toFixed(
                  3
                ),
              structuredDataExtractions:
                this.metrics.drillingSpecificMetrics.structuredDataExtractions,
              ocrFallbackRate:
                this.metrics.totalProcessed > 0
                  ? (
                      (this.metrics.drillingSpecificMetrics.ocrFallbacks /
                        this.metrics.totalProcessed) *
                      100
                    ).toFixed(1) + "%"
                  : "0%",
            },
          },
          cache: {
            textCacheSize: this.textCache.size,
            chunkCacheSize: this.chunkCache.size,
            qualityCacheSize: this.qualityCache.size,
            estimatedHitRate: this.calculateCacheHitRate(),
          },
          system: {
            uptime: process.uptime(),
            memoryUsage: process.memoryUsage(),
            nodeVersion: process.version,
            optimizedFor: "drilling-reports",
            version: "2.0-drilling-optimized",
          },
          processing: {
            successRate: this.calculateSuccessRate(),
            averageProcessingTime: Math.round(
              this.metrics.averageProcessingTime
            ),
            totalFilesProcessed: this.metrics.totalProcessed,
            totalErrors: this.metrics.totalErrors,
            totalFilesDeleted: this.metrics.totalDeleted,
            averageChunksPerFile:
              this.metrics.totalProcessed > 0
                ? Math.round(
                    this.metrics.totalChunksProcessed /
                      this.metrics.totalProcessed
                  )
                : 0,
          },
          errorBreakdown: this.metrics.errorTypes,
        },
      });
    } catch (error) {
      return handlers.response.error({
        res,
        message: "Failed to get drilling service metrics",
        statusCode: 500,
      });
    }
  }

  /**
   * 📈 Calculate enhanced cache hit rate
   */
  calculateCacheHitRate() {
    const totalCacheAttempts = this.metrics.totalProcessed;
    if (totalCacheAttempts === 0) return 0;

    const estimatedHits = Math.min(
      this.textCache.size + this.chunkCache.size + this.qualityCache.size,
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
   * 🧹 Enhanced cache clearing
   */
  async clearCaches(req, res) {
    try {
      const textCacheSize = this.textCache.size;
      const chunkCacheSize = this.chunkCache.size;
      const qualityCacheSize = this.qualityCache.size;

      this.textCache.clear();
      this.chunkCache.clear();
      this.qualityCache.clear();

      console.log(
        `🧹 Drilling service caches cleared: ${textCacheSize} text, ${chunkCacheSize} chunk, ${qualityCacheSize} quality entries`
      );

      return handlers.response.success({
        res,
        message: "Drilling service caches cleared successfully",
        data: {
          timestamp: new Date().toISOString(),
          clearedEntries: {
            textCache: textCacheSize,
            chunkCache: chunkCacheSize,
            qualityCache: qualityCacheSize,
            total: textCacheSize + chunkCacheSize + qualityCacheSize,
          },
          serviceType: "drilling-optimized",
        },
      });
    } catch (error) {
      return handlers.response.error({
        res,
        message: "Failed to clear drilling service caches",
        statusCode: 500,
      });
    }
  }

  /**
   * 🔍 Enhanced health check with drilling-specific status
   */
  async healthCheck(req, res) {
    try {
      const health = {
        status: "healthy",
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        version: process.version,
        environment: process.env.NODE_ENV || "development",
        serviceType: "drilling-document-processor",
        optimizationVersion: "2.0-drilling-optimized",
        services: {
          s3: "unknown",
          dynamodb: "unknown",
          qdrant: "unknown",
        },
        metrics: {
          processedFiles: this.metrics.totalProcessed,
          errors: this.metrics.totalErrors,
          successRate: this.calculateSuccessRate(),
          cacheSize:
            this.textCache.size + this.chunkCache.size + this.qualityCache.size,
        },
        drillingMetrics: {
          bhaReports: this.metrics.drillingSpecificMetrics.bhaReports,
          mmrReports: this.metrics.drillingSpecificMetrics.mmrReports,
          rvenReports: this.metrics.drillingSpecificMetrics.rvenReports,
          averageQuality:
            this.metrics.drillingSpecificMetrics.averageQualityScore.toFixed(3),
          structuredExtractions:
            this.metrics.drillingSpecificMetrics.structuredDataExtractions,
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
          health.services.s3 = "healthy";
        } else {
          health.services.s3 = "error";
          health.status = "degraded";
        }
      }

      return handlers.response.success({
        res,
        message: "Drilling service health check completed",
        data: health,
      });
    } catch (error) {
      return handlers.response.error({
        res,
        message: "Drilling service health check failed",
        statusCode: 503,
        data: {
          status: "unhealthy",
          error: error.message,
          timestamp: new Date().toISOString(),
          serviceType: "drilling-document-processor",
        },
      });
    }
  }

  /**
   * 📋 Enhanced processing statistics for drilling reports
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
          serviceType: "drilling-optimized",
        },
        drillingSpecific: {
          documentTypes: {
            bhaReports: this.metrics.drillingSpecificMetrics.bhaReports,
            mmrReports: this.metrics.drillingSpecificMetrics.mmrReports,
            rvenReports: this.metrics.drillingSpecificMetrics.rvenReports,
            totalDrillingReports:
              this.metrics.drillingSpecificMetrics.bhaReports +
              this.metrics.drillingSpecificMetrics.mmrReports +
              this.metrics.drillingSpecificMetrics.rvenReports,
          },
          qualityMetrics: {
            averageQualityScore:
              this.metrics.drillingSpecificMetrics.averageQualityScore.toFixed(
                3
              ),
            structuredDataExtractions:
              this.metrics.drillingSpecificMetrics.structuredDataExtractions,
            ocrFallbacks: this.metrics.drillingSpecificMetrics.ocrFallbacks,
            ocrFallbackRate:
              this.metrics.totalProcessed > 0
                ? `${(
                    (this.metrics.drillingSpecificMetrics.ocrFallbacks /
                      this.metrics.totalProcessed) *
                    100
                  ).toFixed(1)}%`
                : "0%",
          },
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
          qualityCacheEntries: this.qualityCache.size,
          estimatedHitRate: this.calculateCacheHitRate(),
        },
        system: {
          uptime: Math.round(process.uptime()),
          memoryUsageMB: Math.round(process.memoryUsage().rss / 1024 / 1024),
          nodeVersion: process.version,
          optimizationVersion: "2.0-drilling-optimized",
        },
      };

      return handlers.response.success({
        res,
        message: "Drilling service processing statistics retrieved",
        data: stats,
      });
    } catch (error) {
      return handlers.response.error({
        res,
        message: "Failed to get drilling service processing statistics",
        statusCode: 500,
      });
    }
  }
}

module.exports = new EnhancedDrillingDocumentService();
