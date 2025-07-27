const {
  QueryCommand,
  PutCommand,
  DeleteCommand,
  UpdateCommand,
  ScanCommand,
} = require("@aws-sdk/lib-dynamodb");
const { v4: uuidv4 } = require("uuid");
const hashPassword = require("../utilities/hash-password");
const { docClient } = require("../config/aws");
const { handlers } = require("../utilities/handlers");
const parseDateRange = require("../utilities/parse-date-range");

const TABLE_NAME = process.env.DYNAMODB_TABLE_NAME;
const EMAIL_GSI_NAME = "EmailIndex";
const USER_ID_GSI_NAME = "UserIdIndex";

class UserService {
  /**
   * Create a new user (Admin only)
   */
  async createUser(req, res) {
    try {
      const adminUser = req.user;

      if (!this.isAdmin(adminUser)) {
        return handlers.response.failed({
          res,
          message: "Unauthorized: Only admins can create users",
          statusCode: 403,
        });
      }

      const { email, password, role = "user", firstName, lastName } = req.body;

      const validation = this.validateUserInput({ email, password });
      if (!validation.isValid) {
        return handlers.response.failed({
          res,
          message: validation.message,
          statusCode: 400,
        });
      }

      const normalizedEmail = email.trim().toLowerCase();
      const existingUser = await this.getUserByEmail(normalizedEmail);

      if (existingUser) {
        return handlers.response.failed({
          res,
          message: "User with this email already exists",
          statusCode: 409,
        });
      }

      const userId = uuidv4();
      const hashedPassword = await hashPassword(password);

      const newUser = {
        PK: `USER#${userId}`,
        SK: `USER#${userId}`,
        EntityType: "Auth",
        UserId: userId,
        Email: normalizedEmail,
        Password: hashedPassword,
        Role: this.validateRole(role),
        FirstName: firstName?.trim() || null,
        LastName: lastName?.trim() || null,
        IsActive: true,
        CreatedAt: new Date().toISOString(),
        UpdatedAt: new Date().toISOString(),
      };

      await this.putUser(newUser);

      // Remove password from response
      const { Password, ...userResponse } = newUser;

      return handlers.response.success({
        res,
        message: "User created successfully",
        data: userResponse,
        statusCode: 201,
      });
    } catch (error) {
      console.error("Create user error:", error);
      return handlers.response.error({
        res,
        message: error.message || "Failed to create user",
      });
    }
  }

  /**
   * Get user by ID
   */
  async getUserById(req, res) {
    try {
      const { userId } = req.params;
      const requestingUser = req.user;

      if (!userId) {
        return handlers.response.failed({
          res,
          message: "Missing userId in request parameters",
          statusCode: 400,
        });
      }

      console.log(requestingUser);
      console.log(userId);

      // Users can only view their own profile unless they're admin
      if (!this.isAdmin(requestingUser) && requestingUser.UserId !== userId) {
        return handlers.response.unauthorized({
          res,
          message: "Unauthorized: You can only view your own profile",
          statusCode: 403,
        });
      }

      const user = await this.findUserById(userId);

      if (!user) {
        return handlers.response.failed({
          res,
          message: "User not found",
          statusCode: 404,
        });
      }

      // Remove password from response
      const { Password, ...userResponse } = user;

      return handlers.response.success({
        res,
        message: "User retrieved successfully",
        data: userResponse,
      });
    } catch (error) {
      console.error("Get user error:", error);
      return handlers.response.error({
        res,
        message: error.message || "Failed to retrieve user",
      });
    }
  }

  /**
   * Get all users (Admin only)
   */
  async getAllUsers(req, res) {
    try {
      const adminUser = req.user;

      if (!this.isAdmin(adminUser)) {
        return handlers.response.failed({
          res,
          message: "Unauthorized: Only admins can view all users",
          statusCode: 403,
        });
      }

      const { page = 1, limit = 10, role, isActive } = req.query;
      const pageNum = parseInt(page);
      const limitNum = parseInt(limit);

      let filterExpression = "EntityType = :entityType";
      const expressionAttributeValues = {
        ":entityType": "Auth",
      };

      // Add role filter if specified
      if (role) {
        filterExpression += " AND #role = :role";
        expressionAttributeValues[":role"] = role;
      }

      // Add active status filter if specified
      if (isActive !== undefined) {
        filterExpression += " AND IsActive = :isActive";
        expressionAttributeValues[":isActive"] = isActive === "true";
      }

      const params = {
        TableName: TABLE_NAME,
        FilterExpression: filterExpression,
        ExpressionAttributeValues: expressionAttributeValues,
      };

      // Add role to expression attribute names if used
      if (role) {
        params.ExpressionAttributeNames = { "#role": "Role" };
      }

      const result = await docClient.send(new ScanCommand(params));
      const users = result.Items || [];

      // Remove passwords from all users
      const sanitizedUsers = users.map(({ Password, ...user }) => user);

      // Simple pagination (for better performance, consider using DynamoDB pagination)
      const startIndex = (pageNum - 1) * limitNum;
      const endIndex = startIndex + limitNum;
      const paginatedUsers = sanitizedUsers.slice(startIndex, endIndex);

      return handlers.response.success({
        res,
        message: "Users retrieved successfully",
        data: {
          users: paginatedUsers,
          pagination: {
            page: pageNum,
            limit: limitNum,
            total: sanitizedUsers.length,
            totalPages: Math.ceil(sanitizedUsers.length / limitNum),
          },
        },
      });
    } catch (error) {
      console.error("Get all users error:", error);
      return handlers.response.error({
        res,
        message: error.message || "Failed to retrieve users",
      });
    }
  }

  /**
   * Update user
   */
  async updateUser(req, res) {
    try {
      const { userId } = req.params;
      const requestingUser = req.user;
      const updates = req.body;

      if (!userId) {
        return handlers.response.failed({
          res,
          message: "Missing userId in request parameters",
          statusCode: 400,
        });
      }

      // Users can only update their own profile unless they're admin
      if (!this.isAdmin(requestingUser) && requestingUser.UserId !== userId) {
        return handlers.response.failed({
          res,
          message: "Unauthorized: You can only update your own profile",
          statusCode: 403,
        });
      }

      const existingUser = await this.findUserById(userId);
      if (!existingUser) {
        return handlers.response.failed({
          res,
          message: "User not found",
          statusCode: 404,
        });
      }

      // Convert camelCase to PascalCase helper function
      const toPascalCase = str => {
        return str.charAt(0).toUpperCase() + str.slice(1);
      };

      // Convert camelCase updates to PascalCase
      const convertedUpdates = {};
      Object.keys(updates).forEach(key => {
        const pascalKey = toPascalCase(key);
        convertedUpdates[pascalKey] = updates[key];
      });

      // Validate updates (using original camelCase keys for validation)
      const allowedUpdates = this.getAllowedUpdates(requestingUser, userId);
      const invalidUpdates = Object.keys(updates).filter(
        key => !allowedUpdates.includes(key)
      );

      if (invalidUpdates.length > 0) {
        return handlers.response.failed({
          res,
          message: `Invalid update fields: ${invalidUpdates.join(", ")}`,
          statusCode: 400,
        });
      }

      // Check if email is being updated and if it already exists
      if (convertedUpdates.Email) {
        const normalizedEmail = convertedUpdates.Email.trim().toLowerCase();
        const existingEmailUser = await this.getUserByEmail(normalizedEmail);

        if (existingEmailUser && existingEmailUser.UserId !== userId) {
          return handlers.response.failed({
            res,
            message: "Email already exists",
            statusCode: 409,
          });
        }
        convertedUpdates.Email = normalizedEmail;
      }

      // Hash password if being updated
      if (convertedUpdates.Password) {
        convertedUpdates.Password = await hashPassword(
          convertedUpdates.Password
        );
      }

      // Validate and normalize role if being updated
      if (convertedUpdates.Role) {
        convertedUpdates.Role = this.validateRole(convertedUpdates.Role);
      }

      // Build update expression
      const updateExpression = [];
      const expressionAttributeNames = {};
      const expressionAttributeValues = {};

      Object.keys(convertedUpdates).forEach((key, index) => {
        const attributeName = `#attr${index}`;
        const attributeValue = `:val${index}`;

        updateExpression.push(`${attributeName} = ${attributeValue}`);
        expressionAttributeNames[attributeName] = key;
        expressionAttributeValues[attributeValue] = convertedUpdates[key];
      });

      // Always update the UpdatedAt field
      updateExpression.push(`#updatedAt = :updatedAt`);
      expressionAttributeNames["#updatedAt"] = "UpdatedAt";
      expressionAttributeValues[":updatedAt"] = new Date().toISOString();

      const updateParams = {
        TableName: TABLE_NAME,
        Key: {
          PK: `USER#${userId}`,
          SK: `USER#${userId}`,
        },
        UpdateExpression: `SET ${updateExpression.join(", ")}`,
        ExpressionAttributeNames: expressionAttributeNames,
        ExpressionAttributeValues: expressionAttributeValues,
        ReturnValues: "ALL_NEW",
      };

      const result = await docClient.send(new UpdateCommand(updateParams));

      // Remove password from response
      const { Password, ...updatedUser } = result.Attributes;

      return handlers.response.success({
        res,
        message: "User updated successfully",
        data: updatedUser,
      });
    } catch (error) {
      console.error("Update user error:", error);
      return handlers.response.error({
        res,
        message: error.message || "Failed to update user",
      });
    }
  }

  /**
   * Delete user (Soft delete by default)
   */
  async deleteUser(req, res) {
    try {
      const { userId } = req.params;
      const { permanent = false } = req.query;
      const requestingUser = req.user;

      if (!userId) {
        return handlers.response.failed({
          res,
          message: "Missing userId in request parameters",
          statusCode: 400,
        });
      }

      // Only admins can delete users
      if (!this.isAdmin(requestingUser)) {
        return handlers.response.failed({
          res,
          message: "Unauthorized: Only admins can delete users",
          statusCode: 403,
        });
      }

      // Prevent admin from deleting themselves
      if (requestingUser.UserId === userId) {
        return handlers.response.failed({
          res,
          message: "You cannot delete your own account",
          statusCode: 400,
        });
      }

      const existingUser = await this.findUserById(userId);
      if (!existingUser) {
        return handlers.response.failed({
          res,
          message: "User not found",
          statusCode: 404,
        });
      }

      if (permanent === "true") {
        // Hard delete
        await this.hardDeleteUser(userId);
        return handlers.response.success({
          res,
          message: "User permanently deleted",
        });
      } else {
        // Soft delete
        await this.softDeleteUser(userId);
        return handlers.response.success({
          res,
          message: "User deactivated successfully",
        });
      }
    } catch (error) {
      console.error("Delete user error:", error);
      return handlers.response.error({
        res,
        message: error.message || "Failed to delete user",
      });
    }
  }

  /**
   * Get user files
   */
  async getUserFiles(req, res) {
    try {
      const { userId } = req.params;
      const requestingUser = req.user;

      if (!userId) {
        return handlers.response.failed({
          res,
          message: "Missing userId in request parameters",
          statusCode: 400,
        });
      }

      // Users can only view their own files unless they're admin
      if (!this.isAdmin(requestingUser) && requestingUser.UserId !== userId) {
        return handlers.response.failed({
          res,
          message: "Unauthorized: You can only view your own files",
          statusCode: 403,
        });
      }

      const command = new QueryCommand({
        TableName: TABLE_NAME,
        IndexName: USER_ID_GSI_NAME,
        KeyConditionExpression: "#userId = :userId",
        FilterExpression: "#entityType = :entityType",
        ExpressionAttributeNames: {
          "#userId": "UserId",
          "#entityType": "EntityType",
        },
        ExpressionAttributeValues: {
          ":userId": userId,
          ":entityType": "File",
        },
      });

      const result = await docClient.send(command);

      return handlers.response.success({
        res,
        message: "Files retrieved successfully",
        data: result.Items || [],
      });
    } catch (error) {
      console.error("Error fetching user files:", error);
      return handlers.response.error({
        res,
        message: error.message || "Failed to fetch user files",
      });
    }
  }

  /**
   * Get user query statistics
   */
  async getTotalUserQueries(req, res) {
    try {
      const { startDate, endDate } = req.query;
      const { userId } = req.params;
      const requestingUser = req.user;

      if (!userId) {
        return handlers.response.failed({
          res,
          message: "Missing userId in request parameters",
          statusCode: 400,
        });
      }

      // Users can only view their own stats unless they're admin
      if (!this.isAdmin(requestingUser) && requestingUser.UserId !== userId) {
        return handlers.response.failed({
          res,
          message: "Unauthorized: You can only view your own statistics",
          statusCode: 403,
        });
      }

      const { fromDate, toDate } = parseDateRange(startDate, endDate);

      const params = {
        TableName: TABLE_NAME,
        FilterExpression:
          "UserId = :userId AND begins_with(PK, :prefix) AND CreatedAt BETWEEN :fromDate AND :toDate",
        ExpressionAttributeValues: {
          ":userId": userId,
          ":prefix": "QUERY#",
          ":fromDate": fromDate,
          ":toDate": toDate,
        },
      };

      const data = await docClient.send(new ScanCommand(params));

      // Create daily breakdown
      const dateMap = {};
      for (const item of data.Items || []) {
        const dateOnly = new Date(item.CreatedAt).toISOString().split("T")[0];
        dateMap[dateOnly] = (dateMap[dateOnly] || 0) + 1;
      }

      const breakdown = Object.entries(dateMap)
        .map(([date, totalQueries]) => ({
          date,
          totalQueries,
        }))
        .sort((a, b) => new Date(a.date) - new Date(b.date));

      return handlers.response.success({
        res,
        message: "Query statistics retrieved successfully",
        data: {
          total: data.Items.length,
          breakdown,
          dateRange: { fromDate, toDate },
        },
      });
    } catch (error) {
      console.error("Get user queries error:", error);
      return handlers.response.error({
        res,
        message: error.message || "Failed to retrieve query statistics",
      });
    }
  }

  // Helper methods
  async getUserByEmail(email) {
    const command = new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: EMAIL_GSI_NAME,
      KeyConditionExpression: "#Email = :email",
      ExpressionAttributeNames: { "#Email": "Email" },
      ExpressionAttributeValues: { ":email": email },
      Limit: 1,
    });

    const result = await docClient.send(command);
    return result.Items?.[0] || null;
  }

  async findUserById(userId) {
    const command = new QueryCommand({
      TableName: TABLE_NAME,
      KeyConditionExpression: "PK = :pk AND SK = :sk",
      ExpressionAttributeValues: {
        ":pk": `USER#${userId}`,
        ":sk": `USER#${userId}`,
      },
    });

    const result = await docClient.send(command);
    return result.Items?.[0] || null;
  }

  async putUser(user) {
    const command = new PutCommand({
      TableName: TABLE_NAME,
      Item: user,
    });
    await docClient.send(command);
  }

  async softDeleteUser(userId) {
    const updateParams = {
      TableName: TABLE_NAME,
      Key: {
        PK: `USER#${userId}`,
        SK: `USER#${userId}`,
      },
      UpdateExpression: "SET IsActive = :isActive, UpdatedAt = :updatedAt",
      ExpressionAttributeValues: {
        ":isActive": false,
        ":updatedAt": new Date().toISOString(),
      },
    };

    await docClient.send(new UpdateCommand(updateParams));
  }

  async hardDeleteUser(userId) {
    const deleteParams = {
      TableName: TABLE_NAME,
      Key: {
        PK: `USER#${userId}`,
        SK: `USER#${userId}`,
      },
    };

    await docClient.send(new DeleteCommand(deleteParams));
  }

  isAdmin(user) {
    return user && user.Role === "admin";
  }

  validateRole(role) {
    const validRoles = ["admin", "user", "moderator"];
    return validRoles.includes(role) ? role : "user";
  }

  validateUserInput({ email, password }) {
    if (!email || !password) {
      return {
        isValid: false,
        message: "Email and password are required",
      };
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      return {
        isValid: false,
        message: "Invalid email format",
      };
    }

    if (password.length < 8) {
      return {
        isValid: false,
        message: "Password must be at least 8 characters long",
      };
    }

    return { isValid: true };
  }

  getAllowedUpdates(requestingUser, targetUserId) {
    const baseUpdates = ["firstName", "lastName", "FirstName", "LastName"];

    if (this.isAdmin(requestingUser)) {
      return [
        ...baseUpdates,
        "email",
        "Email",
        "password",
        "Password",
        "role",
        "Role",
        "isActive",
        "IsActive",
      ];
    }

    // Non-admin users can only update their own basic info and password
    if (requestingUser.UserId === targetUserId) {
      return [...baseUpdates, "password", "Password"];
    }

    return [];
  }
}

module.exports = new UserService();
