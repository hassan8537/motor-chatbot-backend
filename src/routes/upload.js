const upload = require("../middlewares/multer");

const router = require("express").Router();

const controller = require("../controllers/upload");

router.post(
  "/upload",
  upload.array("files"),
  controller.uploadFilesToS3.bind(controller)
);

router.get("/files", controller.getUploadedFiles.bind(controller));

router.delete("/", controller.deleteFileFromS3AndQdrant.bind(controller));

router.get("/health", controller.healthCheck.bind(controller));

module.exports = router;
