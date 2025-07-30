const { docClient } = require("../config/aws");
const openaiClient = require("../config/openai");
const getEmbedding = require("../utilities/get-embedding");
const { handlers } = require("../utilities/handlers");
const { saveQuery } = require("../utilities/save-query");
const searchResults = require("../utilities/search-results");
const { QueryCommand, BatchWriteCommand } = require("@aws-sdk/lib-dynamodb");

class ChatService {
  constructor() {
    this.tableName = process.env.DYNAMODB_TABLE_NAME;
    console.log("🚀 RAG Service initialized");
  }

  async search(req, res) {
    const startTime = Date.now();
    const { query, collectionName = "document_embeddings" } = req.body;
    const userId = req.user?.UserId;

    if (!query?.trim()) {
      return handlers.response.failed({ res, message: "Query is required" });
    }

    const sanitizedQuery = query.trim();
    console.log(`🔍 Processing: "${sanitizedQuery}"`);

    try {
      // 1. Get embedding
      console.log(`🧠 Getting embedding...`);
      const embedding = await getEmbedding(sanitizedQuery, {
        model: "text-embedding-3-small",
      });

      if (!embedding) {
        throw new Error("Failed to generate embedding");
      }

      // 2. Search documents
      const results = await searchResults({
        collectionName,
        embedding,
        queryText: sanitizedQuery,
        limit: 20,
        scoreThreshold: 0.3,
      });

      console.log(`📊 Found ${results.length} results`);

      if (results.length === 0) {
        return handlers.response.success({
          res,
          message: "No relevant information found",
          data: {
            answer:
              "I couldn't find relevant information to answer your question.",
            sources: [],
            metrics: { resultsCount: 0 },
          },
        });
      }

      // 3. Build context
      const context = this.buildContext(results);

      // 4. Generate response
      console.log(`🤖 Generating response...`);
      const completion = await openaiClient.chat.completions.create({
        model: "gpt-4o",
        temperature: 0.3,
        messages: [
          {
            role: "system",
            content: `You are a specialized assistant trained to extract and answer technical questions from motor reports and directional drilling documents. These reports may contain structured tables, text blocks, scanned images, or OCR outputs.

INSTRUCTIONS:

- Always base your answers on the content of the uploaded documents, no assumptions.
- Your primary goal is to **accurately extract numeric and technical data** from the reports.
- Include values with proper **units** where available.
- **If the answer is not found**, respond with “Not specified in the documents.”
- Use the most relevant and complete source when answering.
- Mention the **document name** where the answer was found.

RECOGNIZED METRICS TO EXTRACT (when asked):
- Footage drilled (Total / Slide / Rotary)
- Average WOB (Weight on Bit)
- Average ROP (Rate of Penetration)
- Slide percentage (based on footage)
- Circulation hours
- Bit hours
- Hole size (e.g., 6.75", 8", 9.875", 12.25")
- Motor configuration (make, model, size, torque, max diff)
- Bit make/model and performance
- Date In / Out
- RPM, Flow Rate, SPP, Torque
- Section type (Vertical, Curve, Lateral, Intermediate)

EXAMPLES:

Q: "What are the circ hours in Motor Report Run 1, BHA 1?"
A: In "TAK 700-R-005 BHA#1 Motor Report.pdf", Run 1, BHA 1 lists:
- Circulation Hours: **46.62 hrs**

Q: "Which bit model had the highest ROP in 9.875\" section?"
A: In "MMR_BHA #2_Intermediate_Legacy 2632A-C3 5H_RVEN 70033.pdf":
- Bit Model: **XYZ123**
- Hole Size: **9.875"**
- ROP: **215 ft/hr**

Q: "What motor had the highest torque in 8\" hole sections?"
A: In "Tak 500-R-007 BHA #6 Motor Report.pdf":
- Motor: **IBEX**
- Size: **8"**
- Max Torque: **22,530 ft-lbs**

FORMAT:
Always list the document name and clearly formatted data fields in your answer.
If multiple documents contribute to an answer, mention each one separately.

TASK:
Based on the uploaded documents, answer the user's technical drilling question below.
`,
          },
          {
            role: "user",
            content: `Documents:
${context}

Question: ${sanitizedQuery}

Please answer based on the documents above.`,
          },
        ],
      });

      const answer = completion.choices[0].message.content;
      const totalTime = Date.now() - startTime;

      // 5. Prepare response
      const responseData = {
        answer,
        sources: this.formatSources(results),
        metrics: {
          totalRequestTimeMs: totalTime,
          resultsCount: results.length,
          tokensUsed: completion.usage?.total_tokens || 0,
        },
      };

      // 6. Save query
      await saveQuery({
        userId,
        queryText: sanitizedQuery,
        answer,
        model: "gpt-4-turbo",
        temperature: 0.1,
        totalTokens: completion.usage?.total_tokens || null,
        timestamp: new Date().toISOString(),
        sources: responseData.sources,
        metrics: responseData.metrics,
        tableName: this.tableName,
      });

      return handlers.response.success({
        res,
        message: "Answer generated successfully",
        data: responseData,
      });
    } catch (error) {
      console.error("❌ Search failed:", error);
      return handlers.response.error({
        res,
        message: "Search failed: " + error.message,
        statusCode: 500,
      });
    }
  }

  buildContext(results) {
    return results
      .slice(0, 15) // Limit context
      .map((r, index) => {
        const filename = r.payload?.name || `Document ${index + 1}`;
        return `=== ${filename} ===\n${r.payload?.content || ""}`;
      })
      .join("\n\n");
  }

  formatSources(results) {
    return results.slice(0, 5).map((r, idx) => ({
      FileName: r.payload?.name || `Document ${idx + 1}`,
      Score: (r.score || 0).toFixed(3),
      Rank: idx + 1,
    }));
  }

  async chats(req, res) {
    try {
      const userId = req.user?.UserId;
      const { limit = 10 } = req.query;

      if (!userId) {
        return handlers.response.failed({ res, message: "Missing userId" });
      }

      const params = {
        TableName: this.tableName,
        IndexName: "UserIdIndex",
        KeyConditionExpression: "UserId = :userId",
        ExpressionAttributeValues: { ":userId": userId },
        Limit: Number(limit) * 2, // Get extra to filter
        ScanIndexForward: false,
      };

      const result = await docClient.send(new QueryCommand(params));

      const chats = (result.Items || [])
        .filter(item => item.EntityType === "Chat" && item.Query)
        .map(item => ({
          id: item.QueryId,
          query: item.Query || "",
          answer: item.Answer || "",
          timestamp: item.CreatedAt || item.UpdatedAt,
          sources: item.sources || [],
        }))
        .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
        .slice(0, Number(limit));

      return handlers.response.success({
        res,
        message: "Chats fetched successfully",
        data: { chats, count: chats.length },
      });
    } catch (error) {
      console.error("❌ Chats fetch error:", error);
      return handlers.response.error({
        res,
        message: "Failed to fetch chats",
        statusCode: 500,
      });
    }
  }

  async deleteAllUserChats(req, res) {
    try {
      const userId = req.user?.UserId;
      const { confirmDeletion } = req.body;

      if (!userId) {
        return handlers.response.failed({ res, message: "Missing userId" });
      }

      if (!confirmDeletion) {
        return handlers.response.failed({
          res,
          message: "Set confirmDeletion: true to proceed",
          statusCode: 400,
        });
      }

      let deletedCount = 0;
      let lastKey = null;

      do {
        // Get items to delete
        const queryParams = {
          TableName: this.tableName,
          IndexName: "UserIdIndex",
          KeyConditionExpression: "UserId = :userId",
          FilterExpression: "EntityType = :entityType",
          ExpressionAttributeValues: {
            ":userId": userId,
            ":entityType": "Chat",
          },
          ProjectionExpression: "PK, SK",
          Limit: 25,
        };

        if (lastKey) queryParams.ExclusiveStartKey = lastKey;

        const queryResult = await docClient.send(new QueryCommand(queryParams));
        const items = queryResult.Items || [];

        if (items.length === 0) break;

        // Delete batch
        const deleteRequests = items.map(item => ({
          DeleteRequest: { Key: { PK: item.PK, SK: item.SK } },
        }));

        await docClient.send(
          new BatchWriteCommand({
            RequestItems: { [this.tableName]: deleteRequests },
          })
        );

        deletedCount += items.length;
        lastKey = queryResult.LastEvaluatedKey;

        // Small delay to avoid throttling
        if (lastKey) await new Promise(r => setTimeout(r, 100));
      } while (lastKey);

      return handlers.response.success({
        res,
        message: "All user chats deleted",
        data: { userId, deletedCount },
      });
    } catch (error) {
      console.error("❌ Delete error:", error);
      return handlers.response.error({
        res,
        message: "Failed to delete chats",
        statusCode: 500,
      });
    }
  }

  async getUserChatCount(req, res) {
    try {
      const userId = req.user?.UserId;
      if (!userId) {
        return handlers.response.failed({ res, message: "Missing userId" });
      }

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
          Select: "COUNT",
          Limit: 1000,
        };

        if (lastEvaluatedKey) {
          queryParams.ExclusiveStartKey = lastEvaluatedKey;
        }

        const result = await docClient.send(new QueryCommand(queryParams));
        totalCount += result.Count || 0;
        lastEvaluatedKey = result.LastEvaluatedKey;
      } while (lastEvaluatedKey);

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

module.exports = new ChatService();
