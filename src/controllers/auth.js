class Controllers {
  constructor() {
    this.service = require("../services/auth-service");
  }

  async signIn(req, res) {
    return await this.service.signIn(req, res);
  }

  async signOut(req, res) {
    return await this.service.signOut(req, res);
  }

  async healthCheck(req, res) {
    return await this.service.healthCheck(req, res);
  }
}

module.exports = new Controllers();
