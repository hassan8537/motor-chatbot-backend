class Controllers {
  constructor() {
    this.service = require("../services/processing-service");
  }

  async processUploadedPdf(req, res) {
    return await this.service.processUploadedPdf(req, res);
  }

  async getMetrics(req, res) {
    return await this.service.getMetrics(req, res);
  }

  async clearCaches(req, res) {
    return await this.service.clearCaches(req, res);
  }

  async healthCheck(req, res) {
    return await this.service.healthCheck(req, res);
  }

  async getProcessingStats(req, res) {
    return await this.service.getProcessingStats(req, res);
  }
}

module.exports = new Controllers();
