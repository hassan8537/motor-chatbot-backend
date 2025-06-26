class Controllers {
  constructor() {
    this.service = require("../services/embedding");
  }

  async search(req, res) {
    return await this.service.search(req, res);
  }

  async getMyChats(req, res) {
    return await this.service.chats(req, res);
  }
}

module.exports = new Controllers();
