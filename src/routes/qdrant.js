const router = require("express").Router();

const controller = require("../controllers/qdrant");

router.get("/collections", controller.getCollections.bind(controller));

router.post("/collections", controller.createCollection.bind(controller));

router.delete(
  "/collections/:collectionName",
  controller.deleteCollection.bind(controller)
);

module.exports = router;
