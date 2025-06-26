const bcrypt = require("bcrypt");

async function comparePassword({ plainPassword, hashedPassword }) {
  return await bcrypt.compare(plainPassword, hashedPassword);
}

module.exports = comparePassword;
