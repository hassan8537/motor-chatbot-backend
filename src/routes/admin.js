const router = require("express").Router();

const controller = require("../controllers/admin");
const { authenticate } = require("../middlewares/authentication");

router.post("/users", authenticate, controller.createUser.bind(controller));

router.get("/users", authenticate, controller.getAllUsers.bind(controller));

router.get(
  "/users/:userId",
  authenticate,
  controller.getUserById.bind(controller)
);

router.put(
  "/users/:userId",
  authenticate,
  controller.updateUser.bind(controller)
);

router.delete(
  "/users/:userId",
  authenticate,
  controller.deleteUser.bind(controller)
);

router.get(
  "/users/:userId/files",
  authenticate,
  controller.getUserFiles.bind(controller)
);

router.get(
  "/users/:userId/queries",
  authenticate,
  controller.getTotalUserQueries.bind(controller)
);

module.exports = router;
