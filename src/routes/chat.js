const router = require("express").Router();

const controller = require("../controllers/chat");

router.post("/search", controller.search.bind(controller));

router.get("/", controller.chats.bind(controller));

router.delete("/all", controller.delete.bind(controller));

router.get("/count", controller.count.bind(controller));

router.get("/metrics", controller.getMetrics.bind(controller));

router.post("/clear-cache", controller.clearCache.bind(controller));

module.exports = router;
