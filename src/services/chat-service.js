const { ScanCommand } = require("@aws-sdk/lib-dynamodb");
const { docClient } = require("../config/aws");
const openaiClient = require("../config/openai");
const getEmbedding = require("../utilities/get-embedding");
const { handlers } = require("../utilities/handlers");
const { saveQuery } = require("../utilities/save-query");
const searchResults = require("../utilities/search-results");

class Service {
  constructor() {
    this.bucket = process.env.BUCKET_NAME;
    this.tableName = process.env.DYNAMODB_TABLE_NAME;

    // 🚀 Performance optimizations
    this.embeddingCache = new Map();
    this.responseCache = new Map();
    this.CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes
    this.MAX_CACHE_SIZE = 100;

    // 📊 Performance metrics
    this.metrics = {
      totalRequests: 0,
      cacheHits: 0,
      averageResponseTime: 0,
    };

    // 🧹 Auto cleanup cache every 10 minutes
    setInterval(() => this.cleanupCache(), 10 * 60 * 1000);
  }

  // 🎯 Embedding cache for performance
  getCachedEmbedding(query) {
    const key = query.toLowerCase().trim();
    console.log(`🔍 Looking for cached embedding with key: ${key}`);
    const cached = this.embeddingCache.get(key);

    if (cached && Date.now() - cached.timestamp < this.CACHE_TTL_MS) {
      console.log(`✅ Found valid cached embedding`);
      // Don't increment cacheHits here - only for full response cache
      return cached.embedding;
    }

    if (cached) {
      console.log(`❌ Cached embedding expired`);
      this.embeddingCache.delete(key);
    } else {
      console.log(`❌ No cached embedding found`);
    }
    return null;
  }

  setCachedEmbedding(query, embedding) {
    const key = query.toLowerCase().trim();
    console.log(`💾 Caching embedding for key: ${key}`);

    // Cleanup if cache is getting too large
    if (this.embeddingCache.size >= this.MAX_CACHE_SIZE) {
      const oldestKey = this.embeddingCache.keys().next().value;
      this.embeddingCache.delete(oldestKey);
      console.log(`🧹 Removed oldest embedding cache entry: ${oldestKey}`);
    }

    this.embeddingCache.set(key, {
      embedding,
      timestamp: Date.now(),
    });

    console.log(
      `📊 Embedding cache size: ${this.embeddingCache.size}/${this.MAX_CACHE_SIZE}`
    );
  }

  // 💾 Response cache for identical queries
  getCachedResponse(query, userId) {
    const key = `${userId}_${query.toLowerCase().trim()}`;
    console.log(`🔍 Looking for cached response with key: ${key}`);
    const cached = this.responseCache.get(key);

    if (cached && Date.now() - cached.timestamp < this.CACHE_TTL_MS) {
      console.log(`✅ Found valid cached response`);
      this.metrics.cacheHits++; // Only increment for full response cache hits
      return {
        ...cached.response,
        metrics: {
          ...cached.response.metrics,
          cached: true,
        },
      };
    }

    if (cached) {
      console.log(`❌ Cached response expired`);
      this.responseCache.delete(key);
    } else {
      console.log(`❌ No cached response found`);
    }
    return null;
  }

  setCachedResponse(query, userId, response) {
    const key = `${userId}_${query.toLowerCase().trim()}`;
    console.log(`💾 Caching response for key: ${key}`);

    // Cleanup if cache is getting too large
    if (this.responseCache.size >= this.MAX_CACHE_SIZE) {
      const oldestKey = this.responseCache.keys().next().value;
      this.responseCache.delete(oldestKey);
      console.log(`🧹 Removed oldest response cache entry: ${oldestKey}`);
    }

    this.responseCache.set(key, {
      response,
      timestamp: Date.now(),
    });

    console.log(
      `📊 Response cache size: ${this.responseCache.size}/${this.MAX_CACHE_SIZE}`
    );
  }

  // 🧹 Cache cleanup
  cleanupCache() {
    const now = Date.now();
    let cleaned = 0;

    console.log(`🧹 Starting cache cleanup...`);

    // Clean embedding cache
    for (const [key, value] of this.embeddingCache.entries()) {
      if (now - value.timestamp > this.CACHE_TTL_MS) {
        this.embeddingCache.delete(key);
        cleaned++;
      }
    }

    // Clean response cache
    for (const [key, value] of this.responseCache.entries()) {
      if (now - value.timestamp > this.CACHE_TTL_MS) {
        this.responseCache.delete(key);
        cleaned++;
      }
    }

    if (cleaned > 0) {
      console.log(`🧹 Cache cleanup: removed ${cleaned} expired entries`);
    } else {
      console.log(`🧹 Cache cleanup: no expired entries found`);
    }
  }

  // 📊 Update metrics
  updateMetrics(responseTime, cached = false) {
    this.metrics.totalRequests++;

    console.log(
      `📊 Updating metrics - Request #${this.metrics.totalRequests}, Cached: ${cached}, Response Time: ${responseTime}ms`
    );

    if (cached) {
      // Note: cacheHits is already incremented in getCachedResponse
      console.log(`📊 Cache hit! Total hits: ${this.metrics.cacheHits}`);
    }

    // Moving average for response time
    const alpha = 0.1;
    this.metrics.averageResponseTime =
      this.metrics.averageResponseTime * (1 - alpha) + responseTime * alpha;

    console.log(`📊 Updated metrics:`, {
      totalRequests: this.metrics.totalRequests,
      cacheHits: this.metrics.cacheHits,
      averageResponseTime: Math.round(this.metrics.averageResponseTime),
      cacheHitRate:
        this.metrics.totalRequests > 0
          ? this.metrics.cacheHits / this.metrics.totalRequests
          : 0,
    });
  }

  // 🔍 Enhanced error handling with retry logic
  async retryOperation(operation, maxRetries = 2, operationName = "operation") {
    let lastError;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await operation();
      } catch (error) {
        lastError = error;
        console.warn(
          `⚠️ ${operationName} attempt ${attempt} failed:`,
          error.message
        );

        // Don't retry on certain errors
        if (
          error.message?.includes("validation") ||
          error.message?.includes("authorization") ||
          error.message?.includes("rate limit")
        ) {
          throw error;
        }

        if (attempt < maxRetries) {
          const delay = 1000 * attempt; // Progressive delay
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    }

    throw new Error(
      `${operationName} failed after ${maxRetries} attempts: ${lastError.message}`
    );
  }

  async search(req, res) {
    const startTime = Date.now();
    let cached = false;

    try {
      const { query } = req.body;
      const userId = req.user?.UserId;

      if (!query) {
        return handlers.response.failed({ res, message: "Query is required" });
      }

      const sanitizedQuery = query.trim();
      console.log(`🔍 Search query: "${sanitizedQuery}" for user: ${userId}`);
      console.log(
        `📊 Before search - Cache sizes: Embedding=${this.embeddingCache.size}, Response=${this.responseCache.size}`
      );

      // 🚀 Check response cache first
      const cachedResponse = this.getCachedResponse(sanitizedQuery, userId);
      if (cachedResponse) {
        console.log("⚡ Using cached response");
        cached = true;
        const responseTime = Date.now() - startTime;
        this.updateMetrics(responseTime, true);

        return handlers.response.success({
          res,
          message: "Answer from GPT",
          data: cachedResponse,
        });
      }

      // 🎯 Get or generate embedding
      let embedding = this.getCachedEmbedding(sanitizedQuery);
      let embeddingFromCache = true;

      if (!embedding) {
        console.log("🧠 Generating new embedding");
        embedding = await this.retryOperation(
          () => getEmbedding(sanitizedQuery),
          2,
          "Generate Embedding"
        );
        this.setCachedEmbedding(sanitizedQuery, embedding);
        embeddingFromCache = false;
      } else {
        console.log("⚡ Using cached embedding");
      }

      // 🔍 Search with retry logic
      const results = await this.retryOperation(
        () =>
          searchResults({
            collectionName: "document_embeddings",
            embedding,
            limit: 4,
          }),
        2,
        "Vector Search"
      );

      console.log(`📊 Found ${results.length} results`);

      // 📝 Build content
      const relevantContents = results.map(r => r.payload.content).join("\n\n");

      // 📋 Build sources
      const sources = results.map((r, idx) => ({
        FileName: r.payload?.name || r.payload?.key || `Document ${idx + 1}`,
        ChunkIndex: idx,
        Score: (r.score || 0).toFixed(2),
      }));

      const modelUsed = "gpt-4-turbo";
      const temperature = 0.2;

      // 🤖 AI completion with retry
      const completion = await this.retryOperation(
        () =>
          openaiClient.chat.completions.create({
            model: modelUsed,
            temperature,
            messages: [
              {
                role: "system",
                content: `You are an intelligent and precise AI assistant trained to answer questions based on structured data extracted from documents such as technical reports, motor/BHA logs, summaries, tables, and other factual sources.

Your goal is to provide accurate, clear, and well-formatted answers using only the given context.

Instructions:
- Use only the context provided to form your answer.
- If a question asks for terms like "most", "least", "average", or "common", and the context only includes one relevant item, return that item and clearly note it is the only available record.
- Do not say "not available" unless no relevant information exists in the context.
- Use the following formatting rules:
  - Use bullet points or line breaks when presenting multiple facts.
  - Include appropriate units (e.g., psi, ft, %, hours, gpm, klbs, deg) where relevant.
  - Preserve domain-specific abbreviations (e.g., ROP, DiffP, PU WT, FTG, RPM) as they appear in the context.
  - Do not include or reference file names, document titles, or metadata.

Clarity, brevity, and accuracy are essential.`,
              },
              {
                role: "user",
                content: `Context:\n${relevantContents}\n\nQuestion: ${sanitizedQuery}`,
              },
            ],
          }),
        2,
        "OpenAI API Call"
      );

      const content = completion.choices[0].message.content;
      const totalRequestTimeMs = Date.now() - startTime;

      // 📊 Enhanced metrics
      const responseData = {
        answer: content,
        sources,
        metrics: {
          totalRequestTimeMs,
          cached: false,
          embeddingFromCache,
          resultsCount: results.length,
          tokensUsed: completion.usage?.total_tokens || 0,
        },
      };

      console.log("💾 About to save query with data:", {
        sources: sources.length,
        metrics: Object.keys(responseData.metrics),
        sourcesData: sources,
        metricsData: responseData.metrics,
      });

      // 💾 Save chat with sources and metrics
      const saveResult = await this.retryOperation(
        () =>
          saveQuery({
            userId,
            queryText: sanitizedQuery,
            answer: content,
            model: modelUsed,
            temperature,
            totalTokens: completion.usage?.total_tokens || null,
            timestamp: new Date().toISOString(),
            sources, // Pass the sources array
            metrics: responseData.metrics, // Pass the metrics object
            tableName: this.tableName,
          }),
        2,
        "Save Query"
      );

      if (saveResult.success) {
        console.log("✅ Query saved successfully with sources and metrics");
      } else {
        console.error("❌ Failed to save query:", saveResult.error);
      }

      // 🚀 Cache the response for future use
      this.setCachedResponse(sanitizedQuery, userId, responseData);

      // 📊 Update performance metrics - NOT cached since it's a new request
      this.updateMetrics(totalRequestTimeMs, false);

      console.log(
        `📊 After search - Cache sizes: Embedding=${this.embeddingCache.size}, Response=${this.responseCache.size}`
      );
      console.log(
        `✅ Search completed in ${totalRequestTimeMs}ms (cached: ${cached})`
      );

      // ✅ Respond
      handlers.response.success({
        res,
        message: "Answer from GPT",
        data: responseData,
      });
    } catch (error) {
      const errorTime = Date.now() - startTime;
      this.updateMetrics(errorTime, cached);

      console.error("❌ Search error:", error);
      handlers.response.error({
        res,
        message: `Search failed: ${error.message}`,
        statusCode: error.statusCode || 500,
      });
    }
  }

  // 📜 Enhanced chats with better error handling and debugging
  async chats(req, res) {
    try {
      const userId = req.user?.UserId;
      const { limit = 10, lastKey } = req.query;

      if (!userId) {
        return handlers.response.failed({ res, message: "Missing userId" });
      }

      const params = {
        TableName: this.tableName,
        FilterExpression: "UserId = :userId AND EntityType = :entityType",
        ExpressionAttributeValues: {
          ":userId": userId,
          ":entityType": "Chat",
        },
        Limit: Number(limit),
      };

      if (lastKey) {
        try {
          params.ExclusiveStartKey = JSON.parse(lastKey);
        } catch (err) {
          return handlers.response.failed({
            res,
            message: "Invalid pagination token",
          });
        }
      }

      // 🔄 Use retry logic for database operations
      const result = await this.retryOperation(
        () => docClient.send(new ScanCommand(params)),
        2,
        "Fetch Chats"
      );

      console.log(
        `📊 Found ${result.Items?.length || 0} chat items from database`
      );

      // 📋 Enhanced chat mapping with error handling and debugging
      const chats = (result.Items || []).map((item, index) => {
        console.log(`🔍 Processing chat item ${index + 1}:`, {
          hasQuery: !!item.Query,
          hasAnswer: !!item.Answer,
          hasSources: !!item.sources,
          hasMetrics: !!item.metrics,
          sourcesLength: Array.isArray(item.sources) ? item.sources.length : 0,
          metricsKeys: item.metrics ? Object.keys(item.metrics) : [],
          itemKeys: Object.keys(item),
        });

        return {
          query: item.Query || "",
          answer: item.Answer || "",
          sources: item.sources || [],
          metrics: item.metrics || {},
          timestamp: item.CreatedAt || item.timestamp,
        };
      });

      console.log(
        `📊 Processed ${chats.length} chats with sources/metrics data`
      );

      return handlers.response.success({
        res,
        message: "User chats fetched successfully",
        data: {
          chats: chats.reverse(),
          nextPageToken: result.LastEvaluatedKey
            ? JSON.stringify(result.LastEvaluatedKey)
            : null,
          metadata: {
            count: chats.length,
            hasMore: !!result.LastEvaluatedKey,
          },
        },
      });
    } catch (error) {
      console.error("❌ Fetch Chats Error:", error);
      return handlers.response.error({
        res,
        message: error.message || "Failed to fetch chats",
        statusCode: error.statusCode || 500,
      });
    }
  }

  // 📊 Performance metrics endpoint - FIXED VERSION
  async getMetrics(req, res) {
    try {
      // Calculate cache hit rate as decimal (0-1) for proper percentage display
      const cacheHitRate =
        this.metrics.totalRequests > 0
          ? this.metrics.cacheHits / this.metrics.totalRequests
          : 0;

      const metricsData = {
        performance: {
          totalRequests: this.metrics.totalRequests,
          cacheHitRate, // Return as decimal (0-1)
          averageResponseTime: Math.round(this.metrics.averageResponseTime),
        },
        cache: {
          embeddingCacheSize: this.embeddingCache.size,
          responseCacheSize: this.responseCache.size,
          maxCacheSize: this.MAX_CACHE_SIZE,
        },
        system: {
          uptime: process.uptime(),
          memoryUsage: process.memoryUsage(),
          nodeVersion: process.version,
        },
      };

      return handlers.response.success({
        res,
        message: "Service metrics retrieved",
        data: metricsData,
      });
    } catch (error) {
      console.error("❌ Metrics Error:", error);
      return handlers.response.error({
        res,
        message: "Failed to fetch metrics",
        statusCode: 500,
      });
    }
  }

  // 🧹 Clear cache endpoint
  async clearCache(req, res) {
    try {
      const { type = "all" } = req.body;
      let cleared = [];

      console.log(`🧹 Clearing cache type: ${type}`);

      switch (type) {
        case "embedding":
          this.embeddingCache.clear();
          cleared.push("embeddings");
          break;
        case "response":
          this.responseCache.clear();
          cleared.push("responses");
          break;
        case "all":
        default:
          this.embeddingCache.clear();
          this.responseCache.clear();
          cleared = ["embeddings", "responses"];
          break;
      }

      console.log(`🧹 Cache cleared: ${cleared.join(", ")}`);
      console.log(
        `📊 Cache sizes after clear - Embedding: ${this.embeddingCache.size}, Response: ${this.responseCache.size}`
      );

      return handlers.response.success({
        res,
        message: `Successfully cleared ${cleared.join(", ")} cache`,
        data: { clearedCaches: cleared },
      });
    } catch (error) {
      console.error("❌ Clear Cache Error:", error);
      return handlers.response.error({
        res,
        message: "Failed to clear cache",
        statusCode: 500,
      });
    }
  }
}

module.exports = new Service();
