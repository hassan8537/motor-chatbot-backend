class Controllers {
  constructor() {
    this.service = require("../services/charts-service");
  }

  async getTotalQueries(req, res) {
    return await this.service.getTotalQueries(req, res);
  }

  async getUsage(req, res) {
    return await this.service.getUsage(req, res);
  }
}

module.exports = new Controllers();
