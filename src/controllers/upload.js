class Controllers {
  constructor() {
    this.service = require("../services/upload-service");
  }

  async uploadFilesToS3(req, res) {
    return await this.service.uploadFilesToS3(req, res);
  }

  async getUploadedFiles(req, res) {
    return await this.service.getUploadedFiles(req, res);
  }

  async deleteFileFromS3AndQdrant(req, res) {
    return await this.service.deleteFileFromS3AndQdrant(req, res);
  }

  async healthCheck(req, res) {
    return await this.service.healthCheck(req, res);
  }
}

module.exports = new Controllers();
