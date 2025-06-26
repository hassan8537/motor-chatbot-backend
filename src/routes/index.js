const app = require("express")();

const adminRoutes = require("../routes/admin");
const authRoutes = require("../routes/auth");
const uploadRoutes = require("../routes/upload");
const textractRoutes = require("../routes/textract");
const embeddingRoutes = require("../routes/embedding");
const qdrantRoutes = require("../routes/qdrant");
const chartRoutes = require("../routes/charts");
const authenticate = require("../middlewares/authentication");

app.use("/auth", authRoutes);
app.use("/admin", adminRoutes);
app.use("/uploads", authenticate, uploadRoutes);
app.use("/textract", authenticate, textractRoutes);
app.use("/qdrant", authenticate, embeddingRoutes);
app.use("/qdrant", authenticate, qdrantRoutes);
app.use("/charts", authenticate, chartRoutes);

module.exports = app;
