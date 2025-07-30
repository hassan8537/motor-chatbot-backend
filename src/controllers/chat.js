class Controllers {
  constructor() {
    this.service = require("../services/chat-service");
  }

  async search(req, res) {
    return await this.service.search(req, res);
  }

  async chats(req, res) {
    return await this.service.chats(req, res);
  }

  async delete(req, res) {
    return await this.service.deleteAllUserChats(req, res);
  }

  async count(req, res) {
    return await this.service.getUserChatCount(req, res);
  }
}

module.exports = new Controllers();
