const { textractClient } = require("../config/aws");
const {
  GetDocumentAnalysisCommand,
  GetDocumentTextDetectionCommand
} = require("@aws-sdk/client-textract");

async function wait(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

async function pollTextractJob(JobId, type, maxRetries = 40, interval = 3000) {
  let retries = 0;
  let nextToken = null;
  let blocks = [];

  while (true) {
    const params = { JobId };
    if (nextToken) params.NextToken = nextToken;

    const command =
      type === "ANALYSIS"
        ? new GetDocumentAnalysisCommand(params)
        : new GetDocumentTextDetectionCommand(params);

    const result = await textractClient.send(command);

    if (result.JobStatus === "FAILED") {
      throw new Error(`Textract ${type} job failed`);
    } else if (result.JobStatus === "SUCCEEDED") {
      blocks = blocks.concat(result.Blocks || []);
      nextToken = result.NextToken;

      if (!nextToken) {
        return blocks;
      }
    } else {
      if (++retries > maxRetries) {
        throw new Error(`Textract ${type} job timeout`);
      }
      await wait(interval);
    }
  }
}

/**
 * Get and combine results of both analysis and text detection jobs
 * @param {Object} params
 * @param {string} params.analysisJobId
 * @param {string} params.textJobId
 */
async function getDocumentResults({ analysisJobId, textJobId }) {
  const [analysisBlocks, textBlocks] = await Promise.all([
    pollTextractJob(analysisJobId, "ANALYSIS"),
    pollTextractJob(textJobId, "TEXT")
  ]);

  return {
    analysisBlocks,
    textBlocks,
    combinedBlocks: [...textBlocks, ...analysisBlocks]
  };
}

module.exports = getDocumentResults;
