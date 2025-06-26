const { handlers } = require("../utilities/handlers");
const startDocumentAnalysis = require("../utilities/start-document-analysis");
const getDocumentResults = require("../utilities//get-document-results");
const formatDocumentAnalysis = require("../utilities/format-document-analysis");
const chunkText = require("../utilities/chunk-text");
const getEmbedding = require("../utilities/get-embedding");
const upsertEmbedding = require("../utilities/upsert-embedding");
const { v4: uuidv4 } = require("uuid");

class Service {
  constructor() {
    this.bucket = process.env.BUCKET_NAME;
    this.tableName = process.env.DYNAMODB_TABLE_NAME;
  }

  // Start both analysis and text detection jobs
  async initiateDocumentAnalysis(req, res) {
    try {
      const { key } = req.body;
      if (!key) {
        return handlers.response.failed({
          res,
          message: "S3 key is required"
        });
      }

      const { analysisJobId, textJobId } = await startDocumentAnalysis(
        this.bucket,
        key
      );

      handlers.response.success({
        res,
        message: "Document analysis and text detection jobs started",
        data: { analysisJobId, textJobId }
      });
    } catch (error) {
      handlers.response.error({ res, message: error });
    }
  }

  // Fetch both job results and upsert embeddings
  async fetchDocumentAnalysisResult(req, res) {
    try {
      const { analysisJobId, textJobId, collectionName } = req.body;
      const userId = req.user?.UserId;

      if (!analysisJobId || !textJobId) {
        return handlers.response.failed({
          res,
          message: "Both analysisJobId and textJobId are required"
        });
      }

      // Get blocks from both jobs
      const { analysisBlocks, textBlocks } = await getDocumentResults({
        analysisJobId,
        textJobId
      });

      // Extract forms & tables from analysis blocks
      const { forms, tables } = formatDocumentAnalysis(analysisBlocks);

      // Extract visible text lines from text detection
      const visibleText = textBlocks
        .filter((b) => b.BlockType === "LINE" && b.Text)
        .map((b) => b.Text)
        .join("\n");

      // Convert forms into text
      const formText = forms
        .map(({ key, value }) => `${key}: ${value}`)
        .join("\n");

      // Convert tables into text
      const tableText = Object.values(tables)
        .flat()
        .map((cell) => `Row ${cell.row}, Col ${cell.col}: ${cell.text}`)
        .join("\n");

      // Combine all extracted content
      const fullText = [visibleText, formText, tableText].join("\n\n");

      // Chunk and embed
      const chunks = chunkText(fullText, 200);

      const chunkData = await Promise.all(
        chunks.map(async (textChunk) => ({
          Content: textChunk,
          Embedding: await getEmbedding(textChunk)
        }))
      );

      await Promise.all(
        chunkData.map(({ Embedding, Content }) =>
          upsertEmbedding({
            collectionName: collectionName,
            id: uuidv4(),
            vector: Embedding,
            payload: {
              analysisJobId,
              textJobId,
              content: Content
            },
            dimension: 1536
          })
        )
      );

      handlers.response.success({
        res,
        message: "Combined analysis results saved and embedded",
        data: {
          chunks: chunkData,
          tables
        }
      });
    } catch (error) {
      handlers.response.error({ res, message: error });
    }
  }
}

module.exports = new Service();
