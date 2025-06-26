const upload = require("../middlewares/multer");

const router = require("express").Router();

const controller = require("../controllers/upload");

router.post(
  "/s3",
  upload.array("files"),
  controller.uploadFilesToS3.bind(controller)
);

router.get("/s3", controller.getUploadedFiles.bind(controller));

module.exports = router;
