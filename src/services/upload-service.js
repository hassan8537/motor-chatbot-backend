const { s3Client, docClient } = require("../config/aws");
const { handlers } = require("../utilities/handlers");
const {
  PutObjectCommand,
  ListObjectsV2Command,
  DeleteObjectCommand,
  HeadObjectCommand,
} = require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
const {
  deleteEmbeddingsByPayloadKey,
  createQdrantIndex,
} = require("../utilities/qdrant-functions");
const { QueryCommand, DeleteCommand } = require("@aws-sdk/lib-dynamodb");

class S3Service {
  constructor() {
    this.bucket = process.env.BUCKET_NAME;
    this.dynamoTableName = process.env.DYNAMODB_TABLE_NAME || "Chatbot";
    this.defaultPresignedUrlExpiration = 300;
    this.defaultCollectionName = "document_embeddings";
    this.allowedFileTypes = new Set(["application/pdf"]);
    this.dynamoClient = docClient;
  }

  _validateKey(key) {
    return (
      typeof key === "string" &&
      key.trim().length > 0 &&
      !/[<>:"|?*]/.test(key) &&
      !key.includes("../")
    );
  }

  _formatSize(bytes) {
    if (!bytes) return "0 Bytes";
    const units = ["Bytes", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return parseFloat((bytes / Math.pow(1024, i)).toFixed(2)) + " " + units[i];
  }

  _createS3Url(key) {
    return `https://${this.bucket}.s3.amazonaws.com/${encodeURIComponent(key)}`;
  }

  async uploadFilesToS3(req, res) {
    try {
      const { key, fileType, expiresIn } = req.body;

      if (!key || !fileType || !this.allowedFileTypes.has(fileType)) {
        return handlers.response.failed({
          res,
          message: "Invalid file type or key",
          statusCode: 400,
        });
      }

      // Check if file already exists
      try {
        await s3Client.send(
          new HeadObjectCommand({ Bucket: this.bucket, Key: key })
        );
        return handlers.response.failed({
          res,
          message: "File already exists",
          statusCode: 409,
        });
      } catch (err) {
        if (err.name !== "NotFound") throw err;
      }

      const command = new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        ContentType: fileType,
        ACL: "public-read",
      });

      const url = await getSignedUrl(s3Client, command, {
        expiresIn: expiresIn || this.defaultPresignedUrlExpiration,
      });

      return handlers.response.success({
        res,
        message: "URL generated successfully",
        data: { url, key },
      });
    } catch (error) {
      console.error("Upload URL generation failed:", error);
      return handlers.response.error({
        res,
        message: "Failed to generate upload URL",
        statusCode: 500,
      });
    }
  }

  async getUploadedFiles(req, res) {
    try {
      const { prefix, maxKeys = 1000, continuationToken } = req.query;

      const data = await s3Client.send(
        new ListObjectsV2Command({
          Bucket: this.bucket,
          MaxKeys: Math.min(+maxKeys, 1000), // Cap at 1000 for performance
          Prefix: prefix,
          ContinuationToken: continuationToken,
        })
      );

      const files = (data.Contents || []).map(
        ({ Key, LastModified, Size, ETag }) => ({
          Key,
          FileName: Key.split("/").pop(),
          LastModified,
          Size,
          SizeFormatted: this._formatSize(Size),
          Url: this._createS3Url(Key),
          ETag: ETag?.replace(/"/g, ""),
        })
      );

      return handlers.response.success({
        res,
        message: "Files retrieved successfully",
        data: {
          files,
          count: files.length,
          isTruncated: data.IsTruncated,
          nextContinuationToken: data.NextContinuationToken,
        },
      });
    } catch (error) {
      console.error("Failed to list files:", error);
      return handlers.response.error({
        res,
        message: "Failed to list files",
        statusCode: 500,
      });
    }
  }

  async deleteFileFromS3AndQdrant(req, res) {
    const { key, collectionName = this.defaultCollectionName } = req.body;

    // Validate input
    if (!key || !this._validateKey(key)) {
      return handlers.response.failed({
        res,
        message: "Invalid or missing key parameter",
        statusCode: 400,
      });
    }

    try {
      // Step 1: Verify file exists in S3
      await this._verifyS3FileExists(key);

      // Step 2: Get DynamoDB record and ensure Qdrant collection exists
      const [dynamoRecord] = await Promise.all([
        this._getDynamoRecord(key),
        createQdrantIndex(collectionName),
      ]);

      if (!dynamoRecord) {
        return handlers.response.failed({
          res,
          message: "File record not found in database",
          statusCode: 404,
        });
      }

      console.log("Found record for deletion:", {
        fileId: dynamoRecord.FileId,
        key,
      });

      // Step 3: Perform all deletions atomically
      await this._performAtomicDeletion(key, dynamoRecord, collectionName);

      return handlers.response.success({
        res,
        message: "File deleted successfully from all sources",
        data: {
          key,
          fileId: dynamoRecord.FileId,
          deletedFromS3: true,
          deletedFromQdrant: true,
          deletedFromDynamoDB: true,
        },
      });
    } catch (error) {
      console.error("Deletion failed:", error);

      // Map specific errors to appropriate status codes
      const statusCode = this._getErrorStatusCode(error);

      return handlers.response.error({
        res,
        message: `Deletion failed: ${error.message}`,
        statusCode,
      });
    }
  }

  async _verifyS3FileExists(key) {
    try {
      await s3Client.send(
        new HeadObjectCommand({ Bucket: this.bucket, Key: key })
      );
    } catch (err) {
      if (err.name === "NotFound") {
        const error = new Error("File not found in S3");
        error.name = "FileNotFound";
        throw error;
      }
      throw err;
    }
  }

  async _performAtomicDeletion(key, dynamoRecord, collectionName) {
    // Prepare all deletion operations
    const deletionPromises = [
      // Delete from S3
      s3Client
        .send(new DeleteObjectCommand({ Bucket: this.bucket, Key: key }))
        .catch(err => {
          const error = new Error(`S3 deletion failed: ${err.message}`);
          error.source = "S3";
          throw error;
        }),

      // Delete from Qdrant
      deleteEmbeddingsByPayloadKey({
        collectionName,
        key: "key",
        value: key,
      }).catch(err => {
        const error = new Error(`Qdrant deletion failed: ${err.message}`);
        error.source = "Qdrant";
        throw error;
      }),

      // Delete from DynamoDB
      this.dynamoClient
        .send(
          new DeleteCommand({
            TableName: this.dynamoTableName,
            Key: { PK: dynamoRecord.PK, SK: dynamoRecord.SK },
          })
        )
        .catch(err => {
          const error = new Error(`DynamoDB deletion failed: ${err.message}`);
          error.source = "DynamoDB";
          throw error;
        }),
    ];

    // Execute all deletions - if any fails, all fail
    try {
      await Promise.all(deletionPromises);
    } catch (error) {
      // Enhance error message with source information
      throw new Error(
        `${error.source || "Unknown"} deletion failed: ${error.message}`
      );
    }
  }

  async _getDynamoRecord(s3Key) {
    const command = new QueryCommand({
      TableName: this.dynamoTableName,
      IndexName: "S3KeyIndex",
      KeyConditionExpression: "#S3Key = :s3k",
      ExpressionAttributeNames: { "#S3Key": "S3Key" },
      ExpressionAttributeValues: { ":s3k": s3Key },
      Limit: 1,
    });

    try {
      const result = await this.dynamoClient.send(command);
      return result.Items?.[0] || null;
    } catch (error) {
      throw new Error(`Failed to query DynamoDB: ${error.message}`);
    }
  }

  _getErrorStatusCode(error) {
    if (error.name === "FileNotFound" || error.message.includes("not found")) {
      return 404;
    }
    if (
      error.message.includes("ValidationException") ||
      error.message.includes("Invalid")
    ) {
      return 400;
    }
    return 500;
  }

  async healthCheck(req, res) {
    try {
      const startTime = Date.now();

      await s3Client.send(
        new ListObjectsV2Command({ Bucket: this.bucket, MaxKeys: 1 })
      );

      const responseTime = Date.now() - startTime;

      return handlers.response.success({
        res,
        message: "S3 service is healthy",
        data: {
          status: "healthy",
          bucket: this.bucket,
          responseTime: `${responseTime}ms`,
          timestamp: new Date().toISOString(),
        },
      });
    } catch (error) {
      console.error("Health check failed:", error);
      return handlers.response.error({
        res,
        message: "S3 service health check failed",
        statusCode: 503,
      });
    }
  }
}

module.exports = new S3Service();
