const { s3Client } = require("../config/aws");
const { handlers } = require("../utilities/handlers");
const {
  PutObjectCommand,
  ListObjectsV2Command
} = require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");

class Service {
  constructor() {
    this.bucket = process.env.BUCKET_NAME;
  }

  async uploadFilesToS3(req, res) {
    const { key, fileType } = req.body;

    if (!key || !fileType) {
      handlers.logger.failed({
        message: "Missing key or fileType in request body"
      });
      return handlers.response.failed({
        res,
        message: "Missing key or fileType"
      });
    }

    try {
      const command = new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        ContentType: fileType,
        ACL: "public-read"
      });

      const presignedUrl = await getSignedUrl(s3Client, command, {
        expiresIn: 300
      });

      return handlers.response.success({
        res,
        message: "Pre-signed URL generated successfully",
        data: { url: presignedUrl, key }
      });
    } catch (err) {
      return handlers.response.error({ res, message: err });
    }
  }

  async getUploadedFiles(req, res) {
    try {
      const command = new ListObjectsV2Command({
        Bucket: this.bucket
      });

      const data = await s3Client.send(command);

      const files =
        data.Contents?.map((item) => {
          const key = item.Key;
          const fileName = key.split("/").pop(); // Extract file name from key

          return {
            Key: key,
            FileName: fileName,
            LastModified: item.LastModified,
            Size: item.Size,
            Url: `https://${this.bucket}.s3.amazonaws.com/${key}`
          };
        }) || [];

      return handlers.response.success({
        res,
        message: "Files retrieved successfully",
        data: files
      });
    } catch (err) {
      return handlers.response.error({
        res,
        message: "Failed to list files: " + err.message
      });
    }
  }
}

module.exports = new Service();
