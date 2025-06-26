const { OpenAI } = require("openai");

const openaiClient = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY // Make sure this environment variable is set
});

module.exports = openaiClient;
