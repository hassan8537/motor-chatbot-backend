// lambda.js - Lambda handler for your Express app
const serverlessExpress = require("@codegenie/serverless-express");
const app = require("./app"); // Your existing Express app

// Configure for Lambda environment
if (
  process.env.AWS_LAMBDA_FUNCTION_NAME ||
  process.env.NODE_ENV === "production"
) {
  // Lambda-specific configurations
  app.set("trust proxy", true);

  // Increase timeouts for OCR processing
  app.use((req, res, next) => {
    req.setTimeout(15 * 60 * 1000); // 15 minutes
    res.setTimeout(15 * 60 * 1000);
    next();
  });

  // Add Lambda-specific logging
  app.use((req, res, next) => {
    console.log(`[${new Date().toISOString()}] ${req.method} ${req.path}`);
    next();
  });

  // Memory usage monitoring for PDF processing operations
  app.use("/api/v1/document-ai/process-pdf", (req, res, next) => {
    const memBefore = process.memoryUsage();
    console.log(
      `🔍 Memory before PDF processing: ${Math.round(
        memBefore.rss / 1024 / 1024
      )}MB`
    );

    res.on("finish", () => {
      const memAfter = process.memoryUsage();
      console.log(
        `📊 Memory after PDF processing: ${Math.round(
          memAfter.rss / 1024 / 1024
        )}MB`
      );
      console.log(
        `📈 Memory delta: ${Math.round(
          (memAfter.rss - memBefore.rss) / 1024 / 1024
        )}MB`
      );

      // Force garbage collection after PDF processing if available
      if (global.gc) {
        global.gc();
        console.log(`🗑️ Forced garbage collection completed`);
      }
    });

    next();
  });

  // Global error handler for Lambda
  app.use((error, req, res, next) => {
    console.error("Lambda error:", error);

    if (error.message?.includes("timeout")) {
      return res.status(504).json({
        success: false,
        message:
          "Processing timeout - please try with a smaller PDF or contact support",
        error: "LAMBDA_TIMEOUT",
      });
    }

    if (error.message?.includes("memory")) {
      return res.status(507).json({
        success: false,
        message:
          "Insufficient memory for processing - please try with a smaller PDF",
        error: "INSUFFICIENT_MEMORY",
      });
    }

    next(error);
  });
}

// Create the serverless express handler
const handler = serverlessExpress({ app });

module.exports = { handler };
