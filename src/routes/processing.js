const router = require("express").Router();

const controller = require("../controllers/processing");
const { authenticate } = require("../middlewares/authentication");

router.post(
  "/pdf",
  authenticate,
  controller.processUploadedPdf.bind(controller)
);

module.exports = router;
