const app = require("express")();

const adminRoutes = require("../routes/admin");
const authRoutes = require("../routes/auth");
const uploadRoutes = require("../routes/upload");
const chatRoutes = require("../routes/chat");
const qdrantRoutes = require("../routes/qdrant");
const chartRoutes = require("../routes/charts");
const processingRoutes = require("../routes/processing");
const { authenticate } = require("../middlewares/authentication");

app.use("/auth", authRoutes);
app.use(adminRoutes);
app.use("/s3", authenticate, uploadRoutes);
app.use("/chats", authenticate, chatRoutes);
app.use("/qdrant", authenticate, qdrantRoutes);
app.use("/charts", authenticate, chartRoutes);
app.use("/processing", processingRoutes);

module.exports = app;
