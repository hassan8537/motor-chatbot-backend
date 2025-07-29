const { docClient } = require("../config/aws");
const openaiClient = require("../config/openai");
const getEmbedding = require("../utilities/get-embedding");
const { handlers } = require("../utilities/handlers");
const { saveQuery } = require("../utilities/save-query");
const searchResults = require("../utilities/search-results");
const { QueryCommand, BatchWriteCommand } = require("@aws-sdk/lib-dynamodb");

class EnhancedDrillingSearchService {
  constructor() {
    this.bucket = process.env.BUCKET_NAME;
    this.tableName = process.env.DYNAMODB_TABLE_NAME;

    // Enhanced caching with drilling-specific optimizations
    this.embeddingCache = new Map();
    this.responseCache = new Map();
    this.drillingQueryCache = new Map(); // Specialized cache for drilling queries

    // Enhanced cache settings
    this.CACHE_TTL_MS = 10 * 60 * 1000; // Extended to 10 minutes for technical queries
    this.DRILLING_CACHE_TTL_MS = 30 * 60 * 1000; // 30 minutes for drilling analysis
    this.MAX_CACHE_SIZE = 200; // Increased cache size
    this.MAX_DRILLING_CACHE_SIZE = 100;

    // Enhanced metrics tracking
    this.metrics = {
      totalRequests: 0,
      cacheHits: 0,
      averageResponseTime: 0,
      drillingQueries: 0,
      aggregationQueries: 0,
      technicalTermsProcessed: 0,
      embeddingEnhancements: 0,
    };

    // Cleanup intervals
    setInterval(() => this.cleanupCache(), 15 * 60 * 1000); // Every 15 minutes

    console.log("🚀 Enhanced Drilling Search Service initialized");
  }

  /**
   * Enhanced embedding caching with drilling context
   */
  getCachedEmbedding(query) {
    const key = this.normalizeQueryKey(query);
    const cached = this.embeddingCache.get(key);
    if (cached && Date.now() - cached.timestamp < this.CACHE_TTL_MS) {
      return cached.embedding;
    }
    this.embeddingCache.delete(key);
    return null;
  }

  setCachedEmbedding(query, embedding) {
    const key = this.normalizeQueryKey(query);
    if (this.embeddingCache.size >= this.MAX_CACHE_SIZE) {
      const oldestKey = this.embeddingCache.keys().next().value;
      this.embeddingCache.delete(oldestKey);
    }
    this.embeddingCache.set(key, {
      embedding,
      timestamp: Date.now(),
      queryType: this.classifyQuery(query),
    });
  }

  /**
   * Enhanced response caching with drilling-specific TTL
   */
  getCachedResponse(query, userId) {
    const key = `${userId}_${this.normalizeQueryKey(query)}`;
    const cached = this.responseCache.get(key);

    // Use longer TTL for drilling analysis queries
    const ttl = this.isDrillingQuery(query)
      ? this.DRILLING_CACHE_TTL_MS
      : this.CACHE_TTL_MS;

    if (cached && Date.now() - cached.timestamp < ttl) {
      this.metrics.cacheHits++;
      return {
        ...cached.response,
        metrics: { ...cached.response.metrics, cached: true },
      };
    }
    this.responseCache.delete(key);
    return null;
  }

  setCachedResponse(query, userId, response) {
    const key = `${userId}_${this.normalizeQueryKey(query)}`;
    if (this.responseCache.size >= this.MAX_CACHE_SIZE) {
      const oldestKey = this.responseCache.keys().next().value;
      this.responseCache.delete(oldestKey);
    }
    this.responseCache.set(key, {
      response,
      timestamp: Date.now(),
      queryType: this.classifyQuery(query),
    });
  }

  /**
   * Specialized drilling query caching for aggregation results
   */
  getCachedDrillingAnalysis(query, userId) {
    const key = `${userId}_${this.normalizeQueryKey(query)}`;
    const cached = this.drillingQueryCache.get(key);
    if (cached && Date.now() - cached.timestamp < this.DRILLING_CACHE_TTL_MS) {
      return cached.analysis;
    }
    this.drillingQueryCache.delete(key);
    return null;
  }

  setCachedDrillingAnalysis(query, userId, analysis) {
    const key = `${userId}_${this.normalizeQueryKey(query)}`;
    if (this.drillingQueryCache.size >= this.MAX_DRILLING_CACHE_SIZE) {
      const oldestKey = this.drillingQueryCache.keys().next().value;
      this.drillingQueryCache.delete(oldestKey);
    }
    this.drillingQueryCache.set(key, {
      analysis,
      timestamp: Date.now(),
      queryComplexity: this.assessQueryComplexity(query),
    });
  }

  /**
   * Enhanced cache cleanup with drilling optimizations
   */
  cleanupCache() {
    const now = Date.now();

    // Clean embedding cache
    for (const [key, value] of this.embeddingCache.entries()) {
      if (now - value.timestamp > this.CACHE_TTL_MS) {
        this.embeddingCache.delete(key);
      }
    }

    // Clean response cache with different TTLs
    for (const [key, value] of this.responseCache.entries()) {
      const ttl =
        value.queryType === "drilling"
          ? this.DRILLING_CACHE_TTL_MS
          : this.CACHE_TTL_MS;
      if (now - value.timestamp > ttl) {
        this.responseCache.delete(key);
      }
    }

    // Clean drilling analysis cache
    for (const [key, value] of this.drillingQueryCache.entries()) {
      if (now - value.timestamp > this.DRILLING_CACHE_TTL_MS) {
        this.drillingQueryCache.delete(key);
      }
    }

    console.log(
      `🧹 Cache cleanup completed: ${this.embeddingCache.size} embeddings, ${this.responseCache.size} responses, ${this.drillingQueryCache.size} drilling analyses`
    );
  }

  /**
   * Enhanced metrics tracking
   */
  updateMetrics(responseTime, cached = false, queryType = "general") {
    this.metrics.totalRequests++;

    if (queryType === "drilling") this.metrics.drillingQueries++;
    if (queryType === "aggregation") this.metrics.aggregationQueries++;

    const alpha = 0.1;
    this.metrics.averageResponseTime =
      this.metrics.averageResponseTime * (1 - alpha) + responseTime * alpha;
  }

  /**
   * Enhanced retry operation with drilling-specific error handling
   */
  async retryOperation(operation, maxRetries = 2, name = "operation") {
    let lastError;
    for (let i = 1; i <= maxRetries; i++) {
      try {
        return await operation();
      } catch (error) {
        lastError = error;

        // Don't retry certain drilling-specific errors
        if (
          [
            "validation",
            "authorization",
            "rate limit",
            "drilling_content_insufficient",
          ].some(msg => error.message.toLowerCase().includes(msg))
        ) {
          throw error;
        }

        if (i < maxRetries) {
          const delay = 1000 * i * (name.includes("drilling") ? 1.5 : 1); // Longer delay for drilling operations
          await new Promise(r => setTimeout(r, delay));
        }
      }
    }
    throw new Error(
      `${name} failed after ${maxRetries} attempts: ${lastError.message}`
    );
  }

  /**
   * Enhanced search with drilling optimizations
   */
  async search(req, res) {
    const startTime = Date.now();
    const { query, collectionName = "document_embeddings" } = req.body;
    const userId = req.user?.UserId;

    if (!query) {
      return handlers.response.failed({ res, message: "Query is required" });
    }

    const sanitizedQuery = query.trim();
    const queryType = this.classifyQuery(sanitizedQuery);
    const isDrilling = this.isDrillingQuery(sanitizedQuery);

    console.log(
      `🔍 Processing ${queryType} query: "${sanitizedQuery.substring(
        0,
        100
      )}..."`
    );

    // Check for cached response
    const cachedResponse = this.getCachedResponse(sanitizedQuery, userId);
    if (cachedResponse) {
      this.updateMetrics(Date.now() - startTime, true, queryType);
      return handlers.response.success({
        res,
        message: "Answer from cache",
        data: cachedResponse,
      });
    }

    // Check for cached drilling analysis for complex queries
    if (isDrilling && queryType === "aggregation") {
      const cachedAnalysis = this.getCachedDrillingAnalysis(
        sanitizedQuery,
        userId
      );
      if (cachedAnalysis) {
        this.updateMetrics(Date.now() - startTime, true, "drilling");
        return handlers.response.success({
          res,
          message: "Drilling analysis from cache",
          data: cachedAnalysis,
        });
      }
    }

    // Generate or retrieve embedding
    let embedding = this.getCachedEmbedding(sanitizedQuery);
    let embeddingFromCache = true;

    if (!embedding) {
      console.log(`🧠 Generating enhanced embedding for ${queryType} query...`);

      embedding = await this.retryOperation(
        () =>
          getEmbedding(sanitizedQuery, {
            enhanceDrillingContext: isDrilling,
            model: "text-embedding-3-small",
          }),
        2,
        `Generate ${isDrilling ? "Drilling" : "Standard"} Embedding`
      );

      this.setCachedEmbedding(sanitizedQuery, embedding);
      embeddingFromCache = false;

      if (isDrilling) this.metrics.embeddingEnhancements++;
    }

    // Enhanced search with drilling optimizations
    const searchOptions = this.getSearchOptions(queryType, isDrilling);

    const results = await this.retryOperation(
      () =>
        searchResults({
          collectionName,
          embedding,
          ...searchOptions,
        }),
      2,
      `${isDrilling ? "Drilling" : "Standard"} Vector Search`
    );

    console.log(
      `📊 Found ${results.length} relevant results for ${queryType} query`
    );

    // Process results with drilling-specific context
    const context = this.buildEnhancedContext(results, queryType, isDrilling);
    const sources = this.formatSources(results);

    // Enhanced OpenAI completion with drilling-specific prompts
    const model = "gpt-4-turbo";
    const temperature = isDrilling ? 0.1 : 0.3; // Lower temperature for technical drilling queries

    const systemPrompt = this.getSystemPrompt(queryType, isDrilling);
    const userPrompt = this.buildUserPrompt(context, sanitizedQuery, queryType);

    console.log(
      `🤖 Generating ${
        isDrilling ? "drilling-optimized" : "standard"
      } response...`
    );

    const completion = await this.retryOperation(
      () =>
        openaiClient.chat.completions.create({
          model,
          temperature,
          messages: [
            { role: "system", content: systemPrompt },
            { role: "user", content: userPrompt },
          ],
        }),
      2,
      "OpenAI API Call"
    );

    const content = completion.choices[0].message.content;
    const totalRequestTimeMs = Date.now() - startTime;

    // Build enhanced response data
    const responseData = {
      answer: content,
      sources,
      queryType,
      isDrillingOptimized: isDrilling,
      metrics: {
        totalRequestTimeMs,
        cached: false,
        embeddingFromCache,
        resultsCount: results.length,
        tokensUsed: completion.usage?.total_tokens || 0,
        processingVersion: "2.0-drilling-optimized",
        queryComplexity: this.assessQueryComplexity(sanitizedQuery),
        technicalTermsFound: this.countTechnicalTerms(sanitizedQuery),
      },
    };

    // Track technical terms
    this.metrics.technicalTermsProcessed +=
      responseData.metrics.technicalTermsFound;

    // Save query with enhanced metadata
    await this.retryOperation(
      () =>
        saveQuery({
          userId,
          queryText: sanitizedQuery,
          answer: content,
          model,
          temperature,
          totalTokens: completion.usage?.total_tokens || null,
          timestamp: new Date().toISOString(),
          sources,
          metrics: responseData.metrics,
          tableName: this.tableName,
          metadata: {
            queryType,
            isDrillingQuery: isDrilling,
            processingVersion: "2.0-drilling-optimized",
            technicalTermsCount: responseData.metrics.technicalTermsFound,
          },
        }),
      2,
      "Save Enhanced Query"
    );

    // Cache the response
    this.setCachedResponse(sanitizedQuery, userId, responseData);

    // Cache drilling analysis for aggregation queries
    if (isDrilling && queryType === "aggregation") {
      this.setCachedDrillingAnalysis(sanitizedQuery, userId, responseData);
    }

    this.updateMetrics(totalRequestTimeMs, false, queryType);

    return handlers.response.success({
      res,
      message: `Answer from ${
        isDrilling ? "drilling-optimized" : "standard"
      } GPT`,
      data: responseData,
    });
  }

  async chats(req, res) {
    try {
      const userId = req.user?.UserId;
      const { limit = 10, lastKey, filterByType } = req.query;

      if (!userId) {
        return handlers.response.failed({ res, message: "Missing userId" });
      }

      console.log(
        `Fetching chats for user: ${userId}, filterByType: ${filterByType}`
      );

      // Strategy: Use higher limit to overcome filter limitations
      const params = {
        TableName: this.tableName,
        IndexName: "UserIdIndex",
        KeyConditionExpression: "UserId = :userId",
        ExpressionAttributeValues: {
          ":userId": userId,
        },
        Limit: Math.max(Number(limit) * 10, 50), // Much higher limit to find filtered items
        ScanIndexForward: false, // Newest first
      };

      // Only add filter if we're specifically looking for drilling queries
      // Skip EntityType filter since it might be causing issues
      if (filterByType === "drilling") {
        params.FilterExpression = "isDrillingQuery = :isDrilling";
        params.ExpressionAttributeValues[":isDrilling"] = true;
      }

      if (lastKey) {
        try {
          params.ExclusiveStartKey = JSON.parse(lastKey);
        } catch {
          return handlers.response.failed({
            res,
            message: "Invalid pagination token",
          });
        }
      }

      console.log("Query params:", JSON.stringify(params, null, 2));

      const result = await docClient.send(new QueryCommand(params));
      console.log(`Found ${result.Items?.length || 0} total items`);

      // Filter in application code instead of database
      const chats = (result.Items || [])
        .filter(item => {
          // Filter for Chat EntityType and content
          const isChat = item.EntityType === "Chat";
          const hasContent = !!(item.Query || item.Answer);

          console.log(
            `Item check - EntityType: "${item.EntityType}", isChat: ${isChat}, hasContent: ${hasContent}`
          );

          return isChat && hasContent;
        })
        .map(item => ({
          id: item.QueryId,
          query: item.Query || "",
          answer: item.Answer || "",
          sources: item.sources || [],
          metrics: item.metrics || {},
          timestamp: item.CreatedAt || item.UpdatedAt,
          queryType: item.queryType || item.QueryType || "general",
          isDrillingQuery:
            item.isDrillingQuery || item.IsDrillingQuery || false,
          technicalTermsCount:
            item.technicalTermsCount || item.TechnicalTermsCount || 0,
          model: item.Model,
          temperature: item.Temperature,
          totalTokens: item.TotalTokens,
          complexityScore: item.complexityScore || item.ComplexityScore,
          processingVersion: item.processingVersion || item.ProcessingVersion,
        }))
        .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
        .slice(0, Number(limit)); // Apply final limit

      console.log(`Returning ${chats.length} chats after filtering`);

      const analytics = this.analyzeChatHistory(chats);

      return handlers.response.success({
        res,
        message: "User chats fetched successfully",
        data: {
          chats,
          nextPageToken: result.LastEvaluatedKey
            ? JSON.stringify(result.LastEvaluatedKey)
            : null,
          metadata: {
            count: chats.length,
            hasMore: !!result.LastEvaluatedKey,
            rawItemsFound: result.Items?.length || 0,
            filterApplied: filterByType || "none",
          },
          analytics,
        },
      });
    } catch (error) {
      console.error("Chats fetch error:", error);
      return handlers.response.error({
        res,
        message: error.message || "Failed to fetch chats",
        statusCode: 500,
      });
    }
  }
  /**
   * Enhanced metrics with drilling-specific insights
   */
  async getMetrics(req, res) {
    try {
      const cacheHitRate =
        this.metrics.totalRequests > 0
          ? this.metrics.cacheHits / this.metrics.totalRequests
          : 0;

      const drillingQueryRate =
        this.metrics.totalRequests > 0
          ? this.metrics.drillingQueries / this.metrics.totalRequests
          : 0;

      const metricsData = {
        performance: {
          totalRequests: this.metrics.totalRequests,
          cacheHitRate: Math.round(cacheHitRate * 100) / 100,
          averageResponseTime: Math.round(this.metrics.averageResponseTime),
          serviceOptimized: "drilling-reports",
        },
        drillingSpecific: {
          drillingQueries: this.metrics.drillingQueries,
          aggregationQueries: this.metrics.aggregationQueries,
          drillingQueryRate: Math.round(drillingQueryRate * 100) / 100,
          technicalTermsProcessed: this.metrics.technicalTermsProcessed,
          embeddingEnhancements: this.metrics.embeddingEnhancements,
        },
        cache: {
          embeddingCacheSize: this.embeddingCache.size,
          responseCacheSize: this.responseCache.size,
          drillingCacheSize: this.drillingQueryCache.size,
          maxCacheSize: this.MAX_CACHE_SIZE,
          cacheTTL: `${this.CACHE_TTL_MS / 1000}s (${
            this.DRILLING_CACHE_TTL_MS / 1000
          }s for drilling)`,
        },
        system: {
          uptime: process.uptime(),
          memoryUsage: process.memoryUsage(),
          nodeVersion: process.version,
          optimizationVersion: "2.0-drilling-search",
        },
      };

      return handlers.response.success({
        res,
        message: "Enhanced drilling service metrics retrieved",
        data: metricsData,
      });
    } catch (error) {
      handlers.response.error({
        res,
        message: "Failed to fetch enhanced metrics",
        statusCode: 500,
      });
    }
  }

  /**
   * Enhanced cache clearing with drilling-specific options
   */
  async clearCache(req, res) {
    try {
      const { type = "all" } = req.body;
      let cleared = [];

      if (type === "embedding" || type === "all") {
        this.embeddingCache.clear();
        cleared.push("embeddings");
      }

      if (type === "response" || type === "all") {
        this.responseCache.clear();
        cleared.push("responses");
      }

      if (type === "drilling" || type === "all") {
        this.drillingQueryCache.clear();
        cleared.push("drilling analyses");
      }

      console.log(`🧹 Cache cleared: ${cleared.join(", ")}`);

      return handlers.response.success({
        res,
        message: `Successfully cleared ${cleared.join(", ")} cache`,
        data: {
          clearedCaches: cleared,
          timestamp: new Date().toISOString(),
          serviceType: "drilling-optimized",
        },
      });
    } catch (error) {
      handlers.response.error({
        res,
        message: "Failed to clear enhanced cache",
        statusCode: 500,
      });
    }
  }

  // ========== UTILITY METHODS ==========

  /**
   * Normalize query key for consistent caching
   */
  normalizeQueryKey(query) {
    return query.toLowerCase().trim().replace(/\s+/g, " ");
  }

  /**
   * Classify query type for optimization
   */
  classifyQuery(query) {
    const lowerQuery = query.toLowerCase();

    if (this.isAggregationQuery(lowerQuery)) return "aggregation";
    if (this.isComparisonQuery(lowerQuery)) return "comparison";
    if (this.isDrillingQuery(lowerQuery)) return "drilling";

    return "general";
  }

  /**
   * Check if query is drilling-related
   */
  isDrillingQuery(query) {
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
    ];

    const lowerQuery = query.toLowerCase();
    return drillingTerms.some(term => lowerQuery.includes(term));
  }

  /**
   * Check if query requires aggregation
   */
  isAggregationQuery(query) {
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

    return aggregationTerms.some(term => query.includes(term));
  }

  /**
   * Check if query is comparison-based
   */
  isComparisonQuery(query) {
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

    return comparisonTerms.some(term => query.includes(term));
  }

  /**
   * Assess query complexity
   */
  assessQueryComplexity(query) {
    let complexity = 1;

    if (this.isAggregationQuery(query)) complexity += 2;
    if (this.isComparisonQuery(query)) complexity += 1;
    if (this.countTechnicalTerms(query) > 3) complexity += 1;
    if (query.length > 100) complexity += 1;

    return Math.min(complexity, 5);
  }

  /**
   * Count technical terms in query
   */
  countTechnicalTerms(query) {
    const technicalTerms = [
      "motor",
      "stator",
      "bit",
      "bha",
      "rop",
      "wob",
      "tfa",
      "nmdc",
      "ubho",
      "differential pressure",
      "circulation hours",
      "slide footage",
      "rotary footage",
    ];

    const lowerQuery = query.toLowerCase();
    return technicalTerms.filter(term => lowerQuery.includes(term)).length;
  }

  /**
   * Get search options based on query type
   */
  getSearchOptions(queryType, isDrilling) {
    const baseOptions = {
      limit: 10,
      scoreThreshold: 0.6,
      enableReranking: true,
    };

    if (queryType === "aggregation") {
      return {
        ...baseOptions,
        limit: 25,
        scoreThreshold: 0.5,
        queryType: "aggregation",
      };
    }

    if (isDrilling) {
      return {
        ...baseOptions,
        limit: 15,
        scoreThreshold: 0.7,
        queryType: "specific",
      };
    }

    return baseOptions;
  }

  /**
   * Build enhanced context from search results
   */
  buildEnhancedContext(results, queryType, isDrilling) {
    if (queryType === "aggregation") {
      // For aggregation queries, prioritize structured data
      const structuredResults = results.filter(
        r =>
          r.payload.content.includes("STRUCTURED DATA") ||
          r.payload.content.includes("METRICS:")
      );

      const regularResults = results.filter(
        r =>
          !r.payload.content.includes("STRUCTURED DATA") &&
          !r.payload.content.includes("METRICS:")
      );

      return [...structuredResults, ...regularResults]
        .slice(0, 20)
        .map(r => r.payload.content)
        .join("\n\n");
    }

    return results.map(r => r.payload.content).join("\n\n");
  }

  /**
   * Format sources with enhanced metadata
   */
  formatSources(results) {
    return results.map((r, idx) => ({
      FileName: r.payload?.name || `Document ${idx + 1}`,
      ChunkIndex: idx,
      Score: (r.score || 0).toFixed(2),
      ContentType: r.payload?.contentType || "general",
      HasStructuredData:
        r.payload?.content?.includes("STRUCTURED DATA") || false,
    }));
  }

  /**
   * Get system prompt based on query type
   **/
  getSystemPrompt(queryType, isDrilling) {
    if (isDrilling && queryType === "aggregation") {
      return `You are an expert drilling engineer and data analyst. Use the provided drilling report data to answer questions about motor specifications, bit performance, BHA configurations, and operational metrics.

For aggregation queries (most used, average, total, etc.):
1. Analyze ALL provided data comprehensively
2. Extract specific numerical values and technical specifications
3. Perform accurate calculations and statistical analysis
4. Provide specific numbers, percentages, and measurements
5. Cite specific examples and data points from the reports

Be precise with technical terminology and provide detailed, data-driven answers.`;
    }

    if (isDrilling) {
      return `You are an expert drilling engineer. Use the provided drilling report data to answer technical questions about drilling operations, equipment specifications, and performance metrics. Be precise with technical terminology and provide specific, actionable information based on the drilling data.`;
    }

    return `You are an intelligent AI assistant. Use only the context below to answer the question. Be accurate, clear, and concise.`;
  }

  /**
   * Build user prompt with enhanced context
   **/
  buildUserPrompt(context, query, queryType) {
    if (queryType === "aggregation") {
      return `Drilling Report Data:\n${context}\n\nAnalyze the above drilling data to answer this question: ${query}\n\nProvide specific numbers, calculations, and cite examples from the data.`;
    }

    return `Context:\n${context}\n\nQuestion: ${query}`;
  }

  /**
   * Analyze chat history for insights
   */
  analyzeChatHistory(chats) {
    const drillingChats = chats.filter(c => c.isDrillingQuery);
    const totalTechnicalTerms = chats.reduce(
      (sum, c) => sum + (c.technicalTermsCount || 0),
      0
    );

    return {
      totalChats: chats.length,
      drillingChats: drillingChats.length,
      drillingPercentage:
        chats.length > 0
          ? Math.round((drillingChats.length / chats.length) * 100)
          : 0,
      averageTechnicalTerms:
        chats.length > 0
          ? Math.round((totalTechnicalTerms / chats.length) * 10) / 10
          : 0,
      mostCommonQueryTypes: this.getMostCommonQueryTypes(chats),
    };
  }

  /**
   * Get most common query types from chat history
   */
  getMostCommonQueryTypes(chats) {
    const types = {};
    chats.forEach(chat => {
      const type = chat.queryType || "general";
      types[type] = (types[type] || 0) + 1;
    });

    return Object.entries(types)
      .sort(([, a], [, b]) => b - a)
      .slice(0, 3)
      .map(([type, count]) => ({ type, count }));
  }

  /**
   * Delete all chats for a user
   */
  async deleteAllUserChats(req, res) {
    try {
      const userId = req.user?.UserId;
      const { confirmDeletion } = req.body;

      if (!userId) {
        return handlers.response.failed({ res, message: "Missing userId" });
      }

      // Safety check - require explicit confirmation
      if (!confirmDeletion) {
        return handlers.response.failed({
          res,
          message:
            "Please confirm deletion by sending { confirmDeletion: true }",
          statusCode: 400,
        });
      }

      console.log(`🗑️ Starting deletion of all chats for user: ${userId}`);

      let deletedCount = 0;
      let lastEvaluatedKey = null;
      const batchSize = 25; // DynamoDB batch write limit

      do {
        // Query to get chat items for the user
        const queryParams = {
          TableName: this.tableName,
          IndexName: "UserIdIndex",
          KeyConditionExpression: "UserId = :userId",
          FilterExpression: "EntityType = :entityType",
          ExpressionAttributeValues: {
            ":userId": userId,
            ":entityType": "Chat",
          },
          ProjectionExpression: "PK, SK", // Only get keys for deletion
          Limit: 100, // Get more items per query
        };

        if (lastEvaluatedKey) {
          queryParams.ExclusiveStartKey = lastEvaluatedKey;
        }

        console.log(`🔍 Querying for chat items...`);
        const queryResult = await docClient.send(new QueryCommand(queryParams));

        const items = queryResult.Items || [];
        console.log(`📋 Found ${items.length} chat items in this batch`);

        if (items.length === 0) {
          break;
        }

        // Delete items in batches of 25
        for (let i = 0; i < items.length; i += batchSize) {
          const batch = items.slice(i, i + batchSize);

          const deleteRequests = batch.map(item => ({
            DeleteRequest: {
              Key: {
                PK: item.PK,
                SK: item.SK,
              },
            },
          }));

          const batchWriteParams = {
            RequestItems: {
              [this.tableName]: deleteRequests,
            },
          };

          console.log(`🗑️ Deleting batch of ${deleteRequests.length} items...`);

          // Retry batch write with exponential backoff
          await this.retryBatchWrite(batchWriteParams);
          deletedCount += deleteRequests.length;

          console.log(
            `✅ Deleted ${deleteRequests.length} items. Total: ${deletedCount}`
          );
        }

        lastEvaluatedKey = queryResult.LastEvaluatedKey;

        // Small delay between batches to avoid throttling
        if (lastEvaluatedKey) {
          await new Promise(resolve => setTimeout(resolve, 100));
        }
      } while (lastEvaluatedKey);

      console.log(
        `🎉 Deletion complete! Deleted ${deletedCount} chat items for user ${userId}`
      );

      return handlers.response.success({
        res,
        message: `Successfully deleted all user chats`,
        data: {
          userId,
          deletedCount,
          timestamp: new Date().toISOString(),
        },
      });
    } catch (error) {
      console.error("❌ Delete chats error:", error);
      return handlers.response.error({
        res,
        message: error.message || "Failed to delete user chats",
        statusCode: 500,
        data: {
          userId: req.user?.UserId,
          error: error.message,
        },
      });
    }
  }

  /**
   * Retry batch write with exponential backoff for unprocessed items
   */
  async retryBatchWrite(batchWriteParams, maxRetries = 3) {
    let attempt = 0;
    let unprocessedItems = batchWriteParams;

    while (
      attempt < maxRetries &&
      unprocessedItems.RequestItems &&
      Object.keys(unprocessedItems.RequestItems).length > 0
    ) {
      try {
        const result = await docClient.send(
          new BatchWriteCommand(unprocessedItems)
        );

        // If there are unprocessed items, retry them
        if (
          result.UnprocessedItems &&
          Object.keys(result.UnprocessedItems).length > 0
        ) {
          unprocessedItems = { RequestItems: result.UnprocessedItems };
          attempt++;

          // Exponential backoff delay
          const delay = Math.min(1000 * Math.pow(2, attempt), 5000);
          console.log(
            `⚠️ ${
              Object.values(result.UnprocessedItems)[0].length
            } unprocessed items, retrying in ${delay}ms...`
          );
          await new Promise(resolve => setTimeout(resolve, delay));
        } else {
          // All items processed successfully
          break;
        }
      } catch (error) {
        attempt++;
        if (attempt >= maxRetries) {
          throw error;
        }

        const delay = Math.min(1000 * Math.pow(2, attempt), 5000);
        console.log(
          `❌ Batch write attempt ${attempt} failed, retrying in ${delay}ms...`
        );
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }

    if (
      attempt >= maxRetries &&
      unprocessedItems.RequestItems &&
      Object.keys(unprocessedItems.RequestItems).length > 0
    ) {
      throw new Error(
        `Failed to process all items after ${maxRetries} attempts`
      );
    }
  }

  /**
   * Get count of user chats (for confirmation before deletion)
   */
  async getUserChatCount(req, res) {
    try {
      const userId = req.user?.UserId;

      if (!userId) {
        return handlers.response.failed({ res, message: "Missing userId" });
      }

      console.log(`📊 Counting chats for user: ${userId}`);

      let totalCount = 0;
      let lastEvaluatedKey = null;

      do {
        const queryParams = {
          TableName: this.tableName,
          IndexName: "UserIdIndex",
          KeyConditionExpression: "UserId = :userId",
          FilterExpression: "EntityType = :entityType",
          ExpressionAttributeValues: {
            ":userId": userId,
            ":entityType": "Chat",
          },
          Select: "COUNT", // Only count, don't return items
          Limit: 1000,
        };

        if (lastEvaluatedKey) {
          queryParams.ExclusiveStartKey = lastEvaluatedKey;
        }

        const result = await docClient.send(new QueryCommand(queryParams));
        totalCount += result.Count || 0;
        lastEvaluatedKey = result.LastEvaluatedKey;
      } while (lastEvaluatedKey);

      console.log(`📈 User ${userId} has ${totalCount} chat items`);

      return handlers.response.success({
        res,
        message: "User chat count retrieved",
        data: {
          userId,
          totalChats: totalCount,
          timestamp: new Date().toISOString(),
        },
      });
    } catch (error) {
      console.error("❌ Count chats error:", error);
      return handlers.response.error({
        res,
        message: error.message || "Failed to count user chats",
        statusCode: 500,
      });
    }
  }
}

module.exports = new EnhancedDrillingSearchService();
