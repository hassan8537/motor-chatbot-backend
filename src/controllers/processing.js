class Controllers {
  constructor() {
    this.service = require("../services/processing-service");
  }

  async processUploadedPdf(req, res) {
    return await this.service.processUploadedPdf(req, res);
  }
}

module.exports = new Controllers();
