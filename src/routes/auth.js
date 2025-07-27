const router = require("express").Router();

const controller = require("../controllers/auth");
const { authenticate } = require("../middlewares/authentication");

router.post("/signin", controller.signIn.bind(controller));

router.post("/signout", authenticate, controller.signOut.bind(controller));

router.get("/health", authenticate, controller.healthCheck.bind(controller));

module.exports = router;
