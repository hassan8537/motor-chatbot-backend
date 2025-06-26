const router = require("express").Router();

const controller = require("../controllers/textract");

router.post("/start", controller.initiateDocumentAnalysis.bind(controller));

router.post(
  "/results",
  controller.fetchDocumentAnalysisResult.bind(controller)
);

module.exports = router;
