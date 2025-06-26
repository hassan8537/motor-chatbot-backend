class Controllers {
  constructor() {
    this.service = require("../services/textract");
  }

  async initiateDocumentAnalysis(req, res) {
    return await this.service.initiateDocumentAnalysis(req, res);
  }

  async fetchDocumentAnalysisResult(req, res) {
    return await this.service.fetchDocumentAnalysisResult(req, res);
  }
}

module.exports = new Controllers();
