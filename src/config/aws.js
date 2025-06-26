const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient } = require("@aws-sdk/lib-dynamodb");
const { S3Client } = require("@aws-sdk/client-s3");
const { TextractClient } = require("@aws-sdk/client-textract");

const clientConfig = {
  region: process.env.AWS_DEFAULT_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  }
};

const dynamoClient = new DynamoDBClient(clientConfig);

const docClient = DynamoDBDocumentClient.from(dynamoClient);

const s3Client = new S3Client(clientConfig);

const textractClient = new TextractClient(clientConfig);

module.exports = {
  dynamoClient,
  docClient,
  s3Client,
  textractClient
};
