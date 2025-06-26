const { textractClient } = require("../config/aws");
const {
  StartDocumentAnalysisCommand,
  StartDocumentTextDetectionCommand
} = require("@aws-sdk/client-textract");

/**
 * Starts both Document Analysis (FORMS + TABLES) and Text Detection jobs.
 * @param {string} bucket - S3 bucket name
 * @param {string} key - S3 object key
 * @returns {Promise<{ analysisJobId: string, textJobId: string }>}
 */
async function startDocumentAnalysis(bucket, key) {
  const documentLocation = { S3Object: { Bucket: bucket, Name: key } };

  // Start Document Analysis job (FORMS + TABLES)
  const analysisCommand = new StartDocumentAnalysisCommand({
    DocumentLocation: documentLocation,
    FeatureTypes: ["TABLES", "FORMS"]
  });
  const analysisResult = await textractClient.send(analysisCommand);

  // Start Text Detection job
  const textCommand = new StartDocumentTextDetectionCommand({
    DocumentLocation: documentLocation
  });
  const textResult = await textractClient.send(textCommand);

  return {
    analysisJobId: analysisResult.JobId,
    textJobId: textResult.JobId
  };
}

module.exports = startDocumentAnalysis;
