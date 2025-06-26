const router = require("express").Router();

const controller = require("../controllers/admin");

const authenticate = require("../middlewares/authentication");

router.post("/users", authenticate, controller.createUser.bind(controller));

module.exports = router;
