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
  }

  async search(req, res) {
    try {
      const { query } = req.body;
      const userId = req.user?.UserId;

      if (!query)
        return handlers.response.failed({ res, message: "Query is required" });

      const embedding = await getEmbedding(query);

      const results = await searchResults({
        collectionName: "document_embeddings",
        embedding,
        limit: 5
      });

      const relevantContents = results
        .map((r) => r.payload.content)
        .join("\n\n");

      const modelUsed = "gpt-4";
      const temperature = 0.2;

      const completion = await openaiClient.chat.completions.create({
        model: modelUsed,
        messages: [
          {
            role: "system",
            content: `You are a helpful and intelligent AI assistant. Use the provided context to answer the user's question accurately and clearly. Respond in a natural and informative manner, suitable for a human audience.
      
                      Adapt your response based on the nature of the query. If the question involves lists, rankings, summaries, or comparisons, present the information in a structured and easy-to-understand format (like bullet points, tables, or plain text) — only if it helps clarity.

                      Do not repeat the context or include unnecessary explanations. If the answer is not present in the context, simply state that the information is not available.`
          },
          {
            role: "user",
            content: `Context:\n${relevantContents}\n\nQuestion: ${query}`
          }
        ],
        temperature
      });

      const content = completion.choices[0].message.content;

      await saveQuery({
        userId,
        queryText: query,
        answer: content,
        model: modelUsed,
        temperature,
        totalTokens: completion.usage?.total_tokens || null,
        timestamp: new Date().toISOString(),
        tableName: this.tableName
      });

      handlers.response.success({
        res,
        message: "Answer from GPT",
        data: {
          answer: content
        }
      });
    } catch (error) {
      handlers.response.error({ res, message: error });
    }
  }

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
          ":entityType": "Chat"
        },
        Limit: Number(limit)
      };

      if (lastKey) {
        try {
          params.ExclusiveStartKey = JSON.parse(lastKey);
        } catch (err) {
          return handlers.response.failed({
            res,
            message: "Invalid pagination token"
          });
        }
      }

      const result = await docClient.send(new ScanCommand(params));

      const chats = (result.Items || []).map((item) => ({
        query: item.Query,
        answer: item.Answer
      }));

      return handlers.response.success({
        res,
        message: "User chats fetched successfully",
        data: {
          chats,
          nextPageToken: result.LastEvaluatedKey
            ? JSON.stringify(result.LastEvaluatedKey)
            : null
        }
      });
    } catch (error) {
      console.error("Fetch Chats Error:", error);
      return handlers.response.error({
        res,
        message: error.message || "Failed to fetch chats"
      });
    }
  }
}

module.exports = new Service();
