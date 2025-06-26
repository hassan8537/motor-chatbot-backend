const router = require("express").Router();

const controller = require("../controllers/auth");

router.post("/signin", controller.signIn.bind(controller));

module.exports = router;
