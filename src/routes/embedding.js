const router = require("express").Router();

const controller = require("../controllers/embedding");

router.post("/search", controller.search.bind(controller));

router.get("/chats", controller.getMyChats.bind(controller));

module.exports = router;
