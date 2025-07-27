const { GetCommand } = require("@aws-sdk/lib-dynamodb");
const jwt = require("jsonwebtoken");
const { docClient } = require("../config/aws");
const { handlers } = require("../utilities/handlers");

const TABLE_NAME = process.env.DYNAMODB_TABLE_NAME;

/**
 * Get user by ID from DynamoDB
 */
async function getUserById(userId) {
  try {
    const command = new GetCommand({
      TableName: TABLE_NAME,
      Key: {
        PK: `USER#${userId}`,
        SK: `USER#${userId}`,
      },
    });

    const result = await docClient.send(command);
    return result.Item || null;
  } catch (error) {
    console.error("Error fetching user by ID:", error);
    return null;
  }
}

/**
 * Authentication middleware
 */
const authenticate = async (req, res, next) => {
  try {
    // Extract token from Authorization header or cookies
    const authHeader = req.headers.authorization;
    const cookieToken = req.cookies?.authorization;

    let token = null;

    // Check Authorization header first (Bearer token)
    if (authHeader && authHeader.startsWith("Bearer ")) {
      token = authHeader.split(" ")[1];
    } else if (cookieToken) {
      token = cookieToken;
    }

    // Return standardized error if no token found
    if (!token) {
      return handlers.response.unauthenticated({
        res,
        message: "Access token is required",
        data: {
          error: "MISSING_TOKEN",
          details:
            "Please provide a valid access token in Authorization header or cookies",
        },
      });
    }

    // Verify JWT token
    let decoded;
    try {
      decoded = jwt.verify(token, process.env.JWT_SECRET);
    } catch (jwtError) {
      return handlers.response.unauthorized({
        res,
        message: "Invalid or expired access token",
        data: {
          error: "INVALID_TOKEN",
          details:
            jwtError.name === "TokenExpiredError"
              ? "Token has expired"
              : "Token is malformed or invalid",
        },
      });
    }

    // Extract user ID from token payload
    // Support both common JWT payload structures
    const userId = decoded.userId || decoded._id || decoded.id || decoded.sub;

    if (!userId) {
      return handlers.response.unauthorized({
        res,
        message: "Invalid token payload",
        data: {
          error: "INVALID_PAYLOAD",
          details: "Token does not contain valid user identifier",
        },
      });
    }

    // Fetch user from database
    const existingUser = await getUserById(userId);

    if (!existingUser) {
      return handlers.response.unauthorized({
        res,
        message: "User not found or access revoked",
        data: {
          error: "USER_NOT_FOUND",
          details:
            "The user associated with this token no longer exists or has been deactivated",
        },
      });
    }

    // Check if user is active (if IsActive field exists)
    if (existingUser.hasOwnProperty("IsActive") && !existingUser.IsActive) {
      return handlers.response.unauthorized({
        res,
        message: "Account has been deactivated",
        data: {
          error: "ACCOUNT_DEACTIVATED",
          details:
            "This user account has been deactivated. Please contact support.",
        },
      });
    }

    // Attach user to request object (remove sensitive data)
    const { Password, ...safeUser } = existingUser;
    req.user = safeUser;

    // Add token info for potential use in routes
    req.tokenInfo = {
      token,
      decoded,
      userId,
    };

    return next();
  } catch (error) {
    console.error("Authentication middleware error:", error);
    return handlers.response.error({
      res,
      message: "Authentication failed",
      data: {
        error: "AUTH_ERROR",
        details: "An error occurred during authentication process",
      },
    });
  }
};

/**
 * Optional middleware to check if user is admin
 */
const requireAdmin = (req, res, next) => {
  if (!req.user) {
    return handlers.response.unauthorized({
      res,
      message: "Authentication required",
    });
  }

  if (req.user.Role !== "admin") {
    return handlers.response.failed({
      res,
      message: "Insufficient permissions: Admin access required",
      statusCode: 403,
      data: {
        error: "INSUFFICIENT_PERMISSIONS",
        requiredRole: "admin",
        currentRole: req.user.Role,
      },
    });
  }

  next();
};

/**
 * Optional middleware to check specific roles
 */
const requireRole = allowedRoles => {
  return (req, res, next) => {
    if (!req.user) {
      return handlers.response.unauthorized({
        res,
        message: "Authentication required",
      });
    }

    const userRole = req.user.Role;
    const rolesArray = Array.isArray(allowedRoles)
      ? allowedRoles
      : [allowedRoles];

    if (!rolesArray.includes(userRole)) {
      return handlers.response.failed({
        res,
        message: `Insufficient permissions: Required role(s): ${rolesArray.join(
          ", "
        )}`,
        statusCode: 403,
        data: {
          error: "INSUFFICIENT_PERMISSIONS",
          requiredRoles: rolesArray,
          currentRole: userRole,
        },
      });
    }

    next();
  };
};

/**
 * Middleware to check if user can access resource (self or admin)
 */
const requireSelfOrAdmin = (req, res, next) => {
  if (!req.user) {
    return handlers.response.unauthorized({
      res,
      message: "Authentication required",
    });
  }

  const { userId } = req.params;
  const isAdmin = req.user.Role === "admin";
  const isSelf = req.user.UserId === userId;

  if (!isAdmin && !isSelf) {
    return handlers.response.failed({
      res,
      message:
        "Unauthorized: You can only access your own resources or be an admin",
      statusCode: 403,
      data: {
        error: "ACCESS_DENIED",
        details:
          "You can only access your own resources unless you have admin privileges",
      },
    });
  }

  next();
};

module.exports = {
  authenticate,
  requireAdmin,
  requireRole,
  requireSelfOrAdmin,
  getUserById, // Export for potential reuse
};
