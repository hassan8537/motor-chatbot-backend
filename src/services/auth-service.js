const { QueryCommand, UpdateCommand } = require("@aws-sdk/lib-dynamodb");
const comparePassword = require("../utilities/compare-password");
const generateToken = require("../utilities/generate-token");
const { handlers } = require("../utilities/handlers");
const { docClient } = require("../config/aws");
const { ScanCommand } = require("@aws-sdk/client-dynamodb");

const TABLE_NAME = process.env.DYNAMODB_TABLE_NAME;
const EMAIL_GSI_NAME = process.env.EMAIL_GSI_NAME || "EmailIndex";

class AuthService {
  constructor() {
    this.maxLoginAttempts = parseInt(process.env.MAX_LOGIN_ATTEMPTS) || 5;
    this.lockoutDuration =
      parseInt(process.env.LOCKOUT_DURATION) || 15 * 60 * 1000; // 15 minutes
    this.sessionTimeout =
      parseInt(process.env.SESSION_TIMEOUT) || 24 * 60 * 60 * 1000; // 24 hours
  }

  // Input validation and sanitization
  _validateSignInInput(email, password) {
    const errors = [];

    if (!email) {
      errors.push("Email is required");
    } else if (typeof email !== "string" || email.trim().length === 0) {
      errors.push("Email must be a non-empty string");
    } else if (!this._isValidEmail(email)) {
      errors.push("Invalid email format");
    }

    if (!password) {
      errors.push("Password is required");
    } else if (typeof password !== "string" || password.length < 6) {
      errors.push("Password must be at least 6 characters long");
    }

    return errors;
  }

  _isValidEmail(email) {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email.trim());
  }

  _normalizeEmail(email) {
    return email.trim().toLowerCase();
  }

  _sanitizeUserData(user) {
    const {
      Password,
      SessionToken,
      LoginAttempts,
      LockedUntil,
      ...sanitizedUser
    } = user;
    return sanitizedUser;
  }

  // Rate limiting and security
  _isAccountLocked(user) {
    if (!user.LockedUntil) return false;

    const now = new Date();
    const lockExpiry = new Date(user.LockedUntil);

    return now < lockExpiry;
  }

  _shouldLockAccount(loginAttempts) {
    return loginAttempts >= this.maxLoginAttempts;
  }

  async signIn(req, res) {
    try {
      const { email, password } = req.body;
      const clientIP = req.ip || req.connection.remoteAddress || "unknown";

      // Input validation
      const validationErrors = this._validateSignInInput(email, password);
      if (validationErrors.length > 0) {
        handlers.logger.warning({
          message: "Sign-in validation failed",
          errors: validationErrors,
          ip: clientIP,
        });

        return handlers.response.failed({
          res,
          message: validationErrors.join(", "),
          statusCode: 400,
        });
      }

      const normalizedEmail = this._normalizeEmail(email);

      // Get user with rate limiting info
      const user = await this.getUserByEmail(normalizedEmail);

      if (!user) {
        // Log failed attempt for monitoring
        handlers.logger.security({
          message: "Sign-in attempt with non-existent email",
          email: normalizedEmail,
          ip: clientIP,
        });

        // Use generic message to prevent email enumeration
        return handlers.response.failed({
          res,
          message: "Invalid credentials",
          statusCode: 401,
        });
      }

      // Check if account is locked
      if (this._isAccountLocked(user)) {
        handlers.logger.security({
          message: "Sign-in attempt on locked account",
          userId: user.UserId,
          email: normalizedEmail,
          ip: clientIP,
        });

        return handlers.response.failed({
          res,
          message:
            "Account is temporarily locked due to multiple failed attempts. Please try again later.",
          statusCode: 423,
        });
      }

      // Verify password
      const isPasswordMatched = await comparePassword({
        plainPassword: password,
        hashedPassword: user.Password,
      });

      if (!isPasswordMatched) {
        // Handle failed login attempt
        await this._handleFailedLogin(user);

        handlers.logger.security({
          message: "Failed sign-in attempt - incorrect password",
          userId: user.UserId,
          email: normalizedEmail,
          attempts: (user.LoginAttempts || 0) + 1,
          ip: clientIP,
        });

        return handlers.response.failed({
          res,
          message: "Invalid credentials",
          statusCode: 401,
        });
      }

      // Successful login - reset failed attempts and generate token
      const token = generateToken({ _id: user.UserId, res });
      const sessionExpiry = new Date(Date.now() + this.sessionTimeout);

      await this._handleSuccessfulLogin(user.PK, token, sessionExpiry);

      handlers.logger.success({
        message: "Successful sign-in",
        userId: user.UserId,
        email: normalizedEmail,
        ip: clientIP,
      });

      // Return sanitized user data
      const sanitizedUser = this._sanitizeUserData(user);

      return handlers.response.success({
        res,
        message: "Sign-in successful",
        data: {
          user: sanitizedUser,
          token,
          expiresAt: sessionExpiry.toISOString(),
        },
      });
    } catch (error) {
      console.error(error);

      handlers.logger.error({
        message: "Sign-in service error",
        error: error.stack,
        body: req.body,
      });

      return handlers.response.error({
        res,
        message: "Authentication service temporarily unavailable",
        statusCode: 503,
      });
    }
  }

  async getUserByEmail(email) {
    try {
      const params = {
        TableName: TABLE_NAME,
        IndexName: EMAIL_GSI_NAME,
        KeyConditionExpression: "#Email = :email",
        ExpressionAttributeNames: {
          "#Email": "Email",
        },
        ExpressionAttributeValues: {
          ":email": email,
        },
        Limit: 1,
        // Only fetch necessary fields for initial lookup
        ProjectionExpression:
          "PK, SK, UserId, Email, Password, LoginAttempts, LockedUntil, #Role, #Status, CreatedAt",
        ExpressionAttributeNames: {
          "#Email": "Email",
          "#Role": "Role",
          "#Status": "Status",
        },
      };

      const command = new QueryCommand(params);
      const result = await docClient.send(command);

      return result.Items?.[0] || null;
    } catch (error) {
      handlers.logger.error({
        message: "Failed to fetch user by email",
        email,
        error: error.stack,
      });
      throw new Error("Database query failed");
    }
  }

  async _handleFailedLogin(user) {
    const loginAttempts = (user.LoginAttempts || 0) + 1;
    const shouldLock = this._shouldLockAccount(loginAttempts);

    const updateParams = {
      TableName: TABLE_NAME,
      Key: {
        PK: user.PK,
        SK: user.SK,
      },
      UpdateExpression:
        "SET LoginAttempts = :attempts, LastFailedLogin = :timestamp",
      ExpressionAttributeValues: {
        ":attempts": loginAttempts,
        ":timestamp": new Date().toISOString(),
      },
    };

    // Lock account if max attempts reached
    if (shouldLock) {
      const lockUntil = new Date(Date.now() + this.lockoutDuration);
      updateParams.UpdateExpression += ", LockedUntil = :lockUntil";
      updateParams.ExpressionAttributeValues[":lockUntil"] =
        lockUntil.toISOString();
    }

    const command = new UpdateCommand(updateParams);
    await docClient.send(command);
  }

  async _handleSuccessfulLogin(userPK, token, sessionExpiry) {
    const updateParams = {
      TableName: TABLE_NAME,
      Key: {
        PK: userPK,
        SK: userPK,
      },
      UpdateExpression: `
        SET SessionToken = :token,
            SessionExpiry = :expiry,
            LastLogin = :timestamp
        REMOVE LoginAttempts, LockedUntil, LastFailedLogin
      `,
      ExpressionAttributeValues: {
        ":token": token,
        ":expiry": sessionExpiry.toISOString(),
        ":timestamp": new Date().toISOString(),
      },
    };

    const command = new UpdateCommand(updateParams);
    await docClient.send(command);
  }

  // Additional utility methods
  async validateSession(token, userId) {
    try {
      const params = {
        TableName: TABLE_NAME,
        Key: {
          PK: `USER#${userId}`,
          SK: `USER#${userId}`,
        },
        ProjectionExpression: "SessionToken, SessionExpiry, #Status",
        ExpressionAttributeNames: {
          "#Status": "Status",
        },
      };

      const command = new QueryCommand(params);
      const result = await docClient.send(command);
      const user = result.Item;

      if (!user || user.SessionToken !== token) {
        return { valid: false, reason: "Invalid session" };
      }

      if (user.SessionExpiry && new Date() > new Date(user.SessionExpiry)) {
        return { valid: false, reason: "Session expired" };
      }

      if (user.Status === "inactive") {
        return { valid: false, reason: "Account inactive" };
      }

      return { valid: true };
    } catch (error) {
      handlers.logger.error({
        message: "Session validation error",
        error: error.stack,
        userId,
      });
      return { valid: false, reason: "Validation error" };
    }
  }

  async signOut(req, res) {
    try {
      const userId = req.user?.UserId;

      if (!userId) {
        return handlers.response.unauthorized({
          res,
          message: "Unauthorized: User not authenticated",
          data: { error: "MISSING_USER_ID" },
        });
      }

      const command = new UpdateCommand({
        TableName: TABLE_NAME,
        Key: {
          PK: `USER#${userId}`,
          SK: `USER#${userId}`,
        },
        UpdateExpression:
          "REMOVE SessionToken, SessionExpiry SET LastLogout = :timestamp",
        ExpressionAttributeValues: {
          ":timestamp": new Date().toISOString(),
        },
      });

      await docClient.send(command);

      return handlers.response.success({
        res,
        message: "Sign-out successful",
      });
    } catch (error) {
      handlers.logger.error({
        message: "Sign-out error",
        error: error.stack,
        userId: req.user?.UserId || "unknown",
      });

      return handlers.response.error({
        res,
        message: "Failed to sign out",
        statusCode: 500,
        data: {
          error: "SIGNOUT_FAILED",
        },
      });
    }
  }

  // Health check for the service
  async healthCheck(req, res) {
    try {
      const params = {
        TableName: TABLE_NAME,
        Limit: 1,
      };

      const command = new ScanCommand(params);
      await docClient.send(command);

      return handlers.response.success({
        res,
        message: "Auth service is healthy",
        data: {
          healthy: true,
          service: "AuthService",
        },
      });
    } catch (error) {
      console.error("[healthCheck] DB connection failed:", error);

      return handlers.response.success({
        res,
        message: "Auth service is unhealthy",
        data: {
          healthy: false,
          service: "AuthService",
        },
      });
    }
  }
}

module.exports = new AuthService();
