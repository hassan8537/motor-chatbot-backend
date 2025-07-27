const { s3Client } = require("../config/aws");
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

class S3Service {
  constructor() {
    this.bucket = process.env.BUCKET_NAME;
    this.defaultPresignedUrlExpiration = 300; // 5 minutes
    this.defaultCollectionName = "document_embeddings";
    this.allowedFileTypes = new Set(["application/pdf"]);
    this.maxFileSize = 10 * 1024 * 1024; // 10MB
  }

  // Validation helpers
  _validateFileType(fileType) {
    return this.allowedFileTypes.has(fileType.toLowerCase());
  }

  _validateKey(key) {
    // Basic validation: no empty strings, no dangerous characters
    return (
      key &&
      typeof key === "string" &&
      key.trim().length > 0 &&
      !/[<>:"|?*]/.test(key) &&
      !key.includes("../")
    );
  }

  _sanitizeKey(key) {
    return key
      .trim()
      .replace(/\s+/g, "_")
      .replace(/[<>:"|?*]/g, "");
  }

  _buildS3Url(key) {
    return `https://${this.bucket}.s3.amazonaws.com/${encodeURIComponent(key)}`;
  }

  async uploadFilesToS3(req, res) {
    try {
      const { key, fileType, expiresIn } = req.body;

      // Input validation
      if (!key || !fileType) {
        handlers.logger.failed({
          message: "Missing required fields: key and fileType",
        });
        return handlers.response.failed({
          res,
          message: "Missing required fields: key and fileType",
          statusCode: 400,
        });
      }

      // Validate and sanitize key
      if (!this._validateKey(key)) {
        return handlers.response.failed({
          res,
          message:
            "Invalid key format. Key must not contain special characters or be empty",
          statusCode: 400,
        });
      }

      // Validate file type
      if (!this._validateFileType(fileType)) {
        return handlers.response.failed({
          res,
          message: `Unsupported file type: ${fileType}. Allowed types: ${Array.from(
            this.allowedFileTypes
          ).join(", ")}`,
          statusCode: 400,
        });
      }

      const sanitizedKey = this._sanitizeKey(key);
      const urlExpiration = expiresIn || this.defaultPresignedUrlExpiration;

      // Check if file already exists (optional - remove if you want to allow overwrites)
      try {
        await s3Client.send(
          new HeadObjectCommand({
            Bucket: this.bucket,
            Key: sanitizedKey,
          })
        );

        return handlers.response.failed({
          res,
          message: "File with this key already exists",
          statusCode: 409,
        });
      } catch (headError) {
        // File doesn't exist, continue with upload
        if (headError.name !== "NotFound") {
          throw headError;
        }
      }

      const command = new PutObjectCommand({
        Bucket: this.bucket,
        Key: sanitizedKey,
        ContentType: fileType,
        ACL: "public-read", // FIXED: Changed from "public" to "public-read"
      });

      const presignedUrl = await getSignedUrl(s3Client, command, {
        expiresIn: urlExpiration,
      });

      handlers.logger.success({
        message: `Pre-signed URL generated for key: ${sanitizedKey}`,
      });

      return handlers.response.success({
        res,
        message: "Pre-signed URL generated successfully",
        data: {
          url: presignedUrl,
          key: sanitizedKey,
          expiresIn: urlExpiration,
        },
      });
    } catch (error) {
      handlers.logger.error({
        message: `Upload URL generation failed: ${error.message}`,
        error: error.stack,
      });

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

      const command = new ListObjectsV2Command({
        Bucket: this.bucket,
        MaxKeys: Math.min(parseInt(maxKeys) || 1000, 1000), // Cap at 1000
        Prefix: prefix || undefined,
        ContinuationToken: continuationToken || undefined,
      });

      const data = await s3Client.send(command);

      if (!data.Contents || data.Contents.length === 0) {
        return handlers.response.success({
          res,
          message: "No files found",
          data: {
            files: [],
            count: 0,
            isTruncated: false,
          },
        });
      }

      const files = Array.from(
        new Map(
          data.Contents.map(item => [
            item.Key,
            {
              Key: item.Key,
              FileName: item.Key.split("/").pop(),
              LastModified: item.LastModified,
              Size: item.Size,
              SizeFormatted: this._formatFileSize(item.Size),
              Url: this._buildS3Url(item.Key),
              ETag: item.ETag?.replace(/"/g, ""),
            },
          ])
        ).values()
      );

      return handlers.response.success({
        res,
        message: "Files retrieved successfully",
        data: {
          files,
          count: files.length,
          isTruncated: data.IsTruncated || false,
          nextContinuationToken: data.NextContinuationToken || null,
        },
      });
    } catch (error) {
      handlers.logger.error({
        message: `Failed to list files: ${error.message}`,
        error: error.stack,
      });

      return handlers.response.error({
        res,
        message: "Failed to retrieve files",
        statusCode: 500,
      });
    }
  }

  async deleteFileFromS3AndQdrant(req, res) {
    try {
      const { key, collectionName = this.defaultCollectionName } = req.body;

      if (!key) {
        return handlers.response.failed({
          res,
          message: "Missing required field: key",
          statusCode: 400,
        });
      }

      if (!this._validateKey(key)) {
        return handlers.response.failed({
          res,
          message: "Invalid key format",
          statusCode: 400,
        });
      }

      // Check if file exists before attempting deletion
      try {
        await s3Client.send(
          new HeadObjectCommand({
            Bucket: this.bucket,
            Key: key,
          })
        );
      } catch (headError) {
        if (headError.name === "NotFound") {
          return handlers.response.failed({
            res,
            message: "File not found in S3",
            statusCode: 404,
          });
        }
        throw headError;
      }

      // Parallel operations for better performance
      const [s3Result, qdrantResult] = await Promise.allSettled([
        // Delete from S3
        s3Client.send(
          new DeleteObjectCommand({
            Bucket: this.bucket,
            Key: key,
          })
        ),

        // Delete embeddings from Qdrant
        this._deleteFromQdrant(key, collectionName),
      ]);

      // Check results
      const s3Success = s3Result.status === "fulfilled";
      const qdrantSuccess = qdrantResult.status === "fulfilled";

      if (!s3Success && !qdrantSuccess) {
        throw new Error(
          `Both S3 and Qdrant deletions failed. S3: ${s3Result.reason?.message}, Qdrant: ${qdrantResult.reason?.message}`
        );
      }

      const warnings = [];
      if (!s3Success)
        warnings.push(`S3 deletion failed: ${s3Result.reason?.message}`);
      if (!qdrantSuccess)
        warnings.push(
          `Qdrant deletion failed: ${qdrantResult.reason?.message}`
        );

      handlers.logger.success({
        message: `File deletion completed for key: ${key}`,
        warnings: warnings.length > 0 ? warnings : undefined,
      });

      return handlers.response.success({
        res,
        message:
          warnings.length > 0
            ? "File deletion completed with warnings"
            : "File and embeddings deleted successfully",
        data: {
          key,
          deletedFromS3: s3Success,
          deletedFromQdrant: qdrantSuccess,
          deletedEmbeddings: qdrantSuccess
            ? qdrantResult.value?.deletedCount || 0
            : 0,
          warnings: warnings.length > 0 ? warnings : undefined,
        },
      });
    } catch (error) {
      handlers.logger.error({
        message: `Deletion failed for key: ${req.body?.key}`,
        error: error.stack,
      });

      return handlers.response.error({
        res,
        message: "Failed to delete file",
        statusCode: 500,
      });
    }
  }

  // Helper method for Qdrant deletion
  async _deleteFromQdrant(key, collectionName) {
    try {
      console.log(key, collectionName);

      await createQdrantIndex(collectionName);
      return await deleteEmbeddingsByPayloadKey({
        collectionName,
        key: "key",
        value: key,
      });
    } catch (error) {
      console.error(`Qdrant deletion error for key ${key}:`, error);
      throw error;
    }
  }

  // Helper method to format file sizes
  _formatFileSize(bytes) {
    if (bytes === 0) return "0 Bytes";
    const k = 1024;
    const sizes = ["Bytes", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
  }

  // Health check method
  async healthCheck(req, res) {
    try {
      await s3Client.send(
        new ListObjectsV2Command({
          Bucket: this.bucket,
          MaxKeys: 1,
        })
      );

      return handlers.response.success({
        res,
        message: "S3 service is healthy",
        data: { status: "healthy", bucket: this.bucket },
      });
    } catch (error) {
      return handlers.response.error({
        res,
        message: "S3 service health check failed",
        statusCode: 503,
      });
    }
  }
}

module.exports = new S3Service();
