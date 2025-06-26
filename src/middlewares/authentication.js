const { GetCommand } = require("@aws-sdk/lib-dynamodb"); // use lib-dynamodb for better compatibility
const jwt = require("jsonwebtoken");
const { docClient } = require("../config/aws");
const { handlers } = require("../utilities/handlers");

const TABLE_NAME = process.env.DYNAMODB_TABLE_NAME;

async function getUserById(userId) {
  const command = new GetCommand({
    TableName: TABLE_NAME,
    Key: {
      PK: `USER#${userId}`,
      SK: `USER#${userId}`
    }
  });

  const result = await docClient.send(command);
  return result.Item || null;
}

const authenticate = async (req, res, next) => {
  const authHeader = req.headers.authorization?.split(" ")[1];
  const cookieToken = req.cookies?.authorization;

  const token = authHeader || cookieToken;

  if (!token) {
    return res.status(401).json({
      message: {
        headers: {
          Authorization: "{{access_token}} is missing"
        }
      }
    });
  }

  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET);

    const existingUser = await getUserById(decoded._id); // _id contains the UserId from the token

    if (!existingUser) {
      return handlers.response.unauthorized({
        res,
        message: "Unauthorized: Invalid access token"
      });
    }

    req.user = existingUser;
    next();
  } catch (error) {
    console.error("Auth error:", error.message);
    return handlers.response.error({ res, message: error });
  }
};

module.exports = authenticate;
