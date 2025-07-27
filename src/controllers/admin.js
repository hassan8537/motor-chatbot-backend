class Controllers {
  constructor() {
    this.service = require("../services/admin-service");
  }

  async createUser(req, res) {
    return await this.service.createUser(req, res);
  }

  async getAllUsers(req, res) {
    return await this.service.getAllUsers(req, res);
  }

  async getUserById(req, res) {
    return await this.service.getUserById(req, res);
  }

  async updateUser(req, res) {
    return await this.service.updateUser(req, res);
  }

  async deleteUser(req, res) {
    return await this.service.deleteUser(req, res);
  }

  async getUserFiles(req, res) {
    return await this.service.getUserFiles(req, res);
  }

  async getTotalUserQueries(req, res) {
    return await this.service.getTotalUserQueries(req, res);
  }
}

module.exports = new Controllers();
