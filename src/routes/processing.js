const router = require("express").Router();

const controller = require("../controllers/processing");
const { authenticate } = require("../middlewares/authentication");

router.post(
  "/pdf",
  authenticate,
  controller.processUploadedPdf.bind(controller)
);

router.get("/metrics", authenticate, controller.getMetrics.bind(controller));

router.post(
  "/clear-caches",
  authenticate,
  controller.clearCaches.bind(controller)
);

router.get("/health", controller.healthCheck.bind(controller));

router.get(
  "/stats",
  authenticate,
  controller.getProcessingStats.bind(controller)
);

module.exports = router;
