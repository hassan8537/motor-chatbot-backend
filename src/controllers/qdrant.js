class Controllers {
  constructor() {
    this.service = require("../services/qdrant-service");
  }

  async getCollections(req, res) {
    return await this.service.getCollections(req, res);
  }

  async createCollection(req, res) {
    return await this.service.createCollection(req, res);
  }

  async deleteCollection(req, res) {
    return await this.service.deleteCollection(req, res);
  }
}

module.exports = new Controllers();
