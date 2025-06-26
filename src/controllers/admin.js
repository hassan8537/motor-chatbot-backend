class Controllers {
  constructor() {
    this.service = require("../services/admin");
  }

  async createUser(req, res) {
    return await this.service.createUser(req, res);
  }
}

module.exports = new Controllers();
