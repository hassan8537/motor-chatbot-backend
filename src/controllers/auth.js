class Controllers {
  constructor() {
    this.service = require("../services/auth");
  }

  async signIn(req, res) {
    return await this.service.signIn(req, res);
  }
}

module.exports = new Controllers();
