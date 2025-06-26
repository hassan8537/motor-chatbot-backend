const { PutCommand } = require("@aws-sdk/lib-dynamodb");
const { v4: uuidv4 } = require("uuid");
const { docClient } = require("../config/aws");

/**
 * Save a user query with metadata (model, temperature, total tokens, etc.)
 * @param {Object} payload - Data to save
 * @param {string} payload.userId
 * @param {string} payload.queryText
 * @param {string} payload.answer
 * @param {string} payload.model
 * @param {number} payload.temperature
 * @param {number} [payload.totalTokens]
 * @param {string} payload.timestamp
 * @param {string} payload.tableName
 */

const saveQuery = async ({
  userId,
  queryText,
  model,
  temperature,
  totalTokens = null,
  answer,
  timestamp,
  tableName
}) => {
  const queryId = uuidv4();
  const PK = `QUERY#${queryId}`;
  const SK = `QUERY#${queryId}USER#${userId}`;

  const params = {
    TableName: tableName,
    Item: {
      PK,
      SK,
      EntityType: "Chat",
      QueryId: queryId,
      UserId: userId,
      Query: queryText,
      Answer: answer, // ✅ Save GPT response
      Model: model,
      Temperature: temperature,
      TotalTokens: totalTokens,
      CreatedAt: timestamp
    }
  };

  try {
    await docClient.send(new PutCommand(params));
    console.log("Query saved successfully.");
    return { success: true, queryId };
  } catch (err) {
    console.error("Error saving query:", err);
    return { success: false, error: err };
  }
};

module.exports = { saveQuery };
