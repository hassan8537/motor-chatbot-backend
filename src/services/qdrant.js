const qdrantClient = require("../config/qdrant");
const { handlers } = require("../utilities/handlers");

class Service {
  constructor() {
    this.qdrantClient = qdrantClient;
  }

  async getCollections(req, res) {
    try {
      const collections = await this.qdrantClient.getCollections();
      return handlers.response.success({
        res,
        message: "Success",
        data: collections
      });
    } catch (error) {
      return handlers.response.error({ res, message: error });
    }
  }

  async createCollection(req, res) {
    try {
      const { collectionName, dimension = 1536 } = req.body;
      if (!collectionName)
        return handlers.response.failed({
          res,
          message: "Collection name is required"
        });

      await this.qdrantClient.createCollection(collectionName, {
        vectors: { size: dimension, distance: "Cosine" }
      });

      return handlers.response.success({ res, message: "Collection created" });
    } catch (error) {
      return handlers.response.error({ res, message: error });
    }
  }

  async deleteCollection(req, res) {
    try {
      const { collectionName } = req.params;
      await this.qdrantClient.deleteCollection(collectionName);
      return handlers.response.success({ res, message: "Success" });
    } catch (error) {
      return handlers.response.error({ res, message: error });
    }
  }
}

module.exports = new Service();
