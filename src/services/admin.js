const { QueryCommand, PutCommand } = require("@aws-sdk/lib-dynamodb");
const { v4: uuidv4 } = require("uuid");
const hashPassword = require("../utilities/hash-password");
const { docClient } = require("../config/aws");
const { handlers } = require("../utilities/handlers");

const TABLE_NAME = process.env.DYNAMODB_TABLE_NAME;
const EMAIL_GSI_NAME = "EmailIndex";

class Service {
  async createUser(req, res) {
    try {
      const adminUser = req.user;

      // Ensure requester is an admin
      if (!adminUser || adminUser.Role !== "admin") {
        return handlers.response.failed({
          res,
          message: "Unauthorized: Only admins can create users"
        });
      }

      const { email, password, role = "user" } = req.body;

      if (!email || !password) {
        return handlers.response.failed({
          res,
          message: "Email and password are required"
        });
      }

      const normalizedEmail = email.trim().toLowerCase();
      const existingUser = await this.getUserByEmail(normalizedEmail);

      if (existingUser) {
        return handlers.response.failed({
          res,
          message: "User with this email already exists",
          data: existingUser
        });
      }

      const uuid = uuidv4();
      const hashedPassword = await hashPassword(password);

      const newUser = {
        PK: `USER#${uuid}`,
        SK: `USER#${uuid}`,
        EntityType: "Auth",
        UserId: uuid,
        Email: normalizedEmail,
        Password: hashedPassword,
        Role: role,
        CreatedAt: new Date().toISOString()
      };

      await this.putUser(newUser);

      return handlers.response.success({
        res,
        message: "User created successfully",
        data: newUser
      });
    } catch (error) {
      console.error("Create user error:", error);
      return handlers.response.error({
        res,
        message: error.message || "Failed to create user"
      });
    }
  }

  async getUserByEmail(email) {
    const command = new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: EMAIL_GSI_NAME,
      KeyConditionExpression: "#Email = :email",
      ExpressionAttributeNames: { "#Email": "Email" },
      ExpressionAttributeValues: { ":email": email },
      Limit: 1
    });

    const result = await docClient.send(command);
    return result.Items?.[0] || null;
  }

  async putUser(user) {
    const command = new PutCommand({
      TableName: TABLE_NAME,
      Item: user
    });
    await docClient.send(command);
  }
}

module.exports = new Service();
