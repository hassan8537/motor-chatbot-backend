require("dotenv").config();

const express = require("express");
const cookieParser = require("cookie-parser");
const cors = require("cors");
const routes = require("./src/routes/index");
const seedAdmin = require("./src/middlewares/seeder");

const app = express();

app.use(cors());
app.use(express.json());
app.use(cookieParser());
app.use(seedAdmin);
app.use(`/api/${process.env.VERSION}`, routes);

const PORT = process.env.PORT || 3000;

app.get("/", (req, res) => {
  res.send("Chatbot is running");
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

module.exports = app;
