require("dotenv").config();
const { QueryCommand, PutCommand } = require("@aws-sdk/lib-dynamodb");
const { v4: uuidv4 } = require("uuid");
const hashPassword = require("../utilities/hash-password");
const { docClient } = require("../config/aws");

const TABLE_NAME = process.env.DYNAMODB_TABLE_NAME;
const EMAIL_INDEX_NAME = "EmailIndex"; // your GSI name for querying by Email

async function seedAdmin(req, res, next) {
  try {
    const email = "admin@example.com";
    const normalizedEmail = email.toLowerCase();

    const checkCommand = new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: EMAIL_INDEX_NAME,
      KeyConditionExpression: "#Email = :email",
      ExpressionAttributeNames: { "#Email": "Email" },
      ExpressionAttributeValues: { ":email": normalizedEmail },
      Limit: 1,
    });

    const result = await docClient.send(checkCommand);

    if (result.Items && result.Items.length > 0) {
      console.log("Admin user already exists. Skipping seeding.");
      return next();
    }

    // Seed admin user
    const uuid = uuidv4();
    const hashedPassword = await hashPassword("SecureAdmin123");

    const adminUser = {
      PK: `USER#${uuid}`,
      SK: `USER#${uuid}`,
      EntityType: "Auth",
      UserId: uuid,
      Email: normalizedEmail,
      Password: hashedPassword,
      Role: "admin",
      IsActive: true,
      CreatedAt: new Date().toISOString(),
    };

    const putCommand = new PutCommand({
      TableName: TABLE_NAME,
      Item: adminUser,
    });

    await docClient.send(putCommand);
    console.log("Admin user seeded successfully!");
    console.log("Email:", email);
    console.log("Password:", "SecureAdmin123");

    next();
  } catch (error) {
    console.error("Failed to seed admin:", error.message);
    next();
  }
}

module.exports = seedAdmin;
