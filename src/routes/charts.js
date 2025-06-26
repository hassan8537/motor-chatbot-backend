const router = require("express").Router();

const controller = require("../controllers/charts");

router.get("/queries/count", controller.getTotalQueries.bind(controller));

router.get("/openai/usage", controller.getUsage.bind(controller));

module.exports = router;
