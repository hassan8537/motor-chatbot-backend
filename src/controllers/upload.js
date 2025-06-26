class Controllers {
  constructor() {
    this.service = require("../services/upload");
  }

  async uploadFilesToS3(req, res) {
    return await this.service.uploadFilesToS3(req, res);
  }

  async getUploadedFiles(req, res) {
    return await this.service.getUploadedFiles(req, res);
  }
}

module.exports = new Controllers();
