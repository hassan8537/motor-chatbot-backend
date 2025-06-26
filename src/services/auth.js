const { QueryCommand, UpdateCommand } = require("@aws-sdk/lib-dynamodb");
const comparePassword = require("../utilities/compare-password");
const generateToken = require("../utilities/generate-token");
const { handlers } = require("../utilities/handlers");
const { docClient } = require("../config/aws");

const TABLE_NAME = process.env.DYNAMODB_TABLE_NAME;
const EMAIL_GSI_NAME = "EmailIndex"; // Ensure this GSI is set on the Email field

class Service {
  async signIn(req, res) {
    try {
      const { email, password } = req.body;

      if (!email || !password) {
        return handlers.response.failed({
          res,
          message: "Email and password are required"
        });
      }

      const normalizedEmail = email.trim().toLowerCase();
      const user = await this.getUserByEmail(normalizedEmail);

      if (!user) {
        return handlers.response.failed({
          res,
          message: "User not found"
        });
      }

      const isPasswordMatched = await comparePassword({
        plainPassword: password,
        hashedPassword: user.Password
      });

      if (!isPasswordMatched) {
        return handlers.response.failed({
          res,
          message: "Invalid credentials"
        });
      }

      const token = generateToken({ _id: user.UserId, res });
      await this.updateSessionToken(user.PK, token);

      return handlers.response.success({
        res,
        message: "Signed In",
        data: { ...user, SessionToken: token }
      });
    } catch (error) {
      console.error("Sign-in error:", error);
      return handlers.response.error({
        res,
        message: error.message || "Something went wrong"
      });
    }
  }

  async getUserByEmail(email) {
    const params = {
      TableName: TABLE_NAME,
      IndexName: EMAIL_GSI_NAME,
      KeyConditionExpression: "#Email = :email",
      ExpressionAttributeNames: {
        "#Email": "Email"
      },
      ExpressionAttributeValues: {
        ":email": email
      },
      Limit: 1
    };

    const command = new QueryCommand(params);
    const result = await docClient.send(command);
    return result.Items?.[0] || null;
  }

  async updateSessionToken(userPK, token) {
    const command = new UpdateCommand({
      TableName: TABLE_NAME,
      Key: {
        PK: userPK,
        SK: userPK
      },
      UpdateExpression: "SET SessionToken = :token",
      ExpressionAttributeValues: {
        ":token": token
      }
    });
    await docClient.send(command);
  }
}

module.exports = new Service();
