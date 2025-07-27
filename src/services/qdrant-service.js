const qdrantClient = require("../config/qdrant");
const { handlers } = require("../utilities/handlers");

class QdrantService {
  constructor(client = qdrantClient) {
    this.client = client;
  }

  async getCollections(req, res) {
    try {
      const collections = await this.client.getCollections();
      return handlers.response.success({
        res,
        message: "Collections retrieved successfully",
        data: collections,
      });
    } catch (error) {
      console.error("Error fetching collections:", error);
      return handlers.response.error({
        res,
        message: error.message || "Failed to retrieve collections",
      });
    }
  }

  async createCollection(req, res) {
    try {
      const {
        collectionName,
        dimension = 1536,
        distance = "Cosine",
      } = req.body;

      if (!collectionName?.trim()) {
        return handlers.response.failed({
          res,
          message: "Collection name is required and cannot be empty",
        });
      }

      // Validate dimension is a positive integer
      if (!Number.isInteger(dimension) || dimension <= 0) {
        return handlers.response.failed({
          res,
          message: "Dimension must be a positive integer",
        });
      }

      const collectionConfig = {
        vectors: {
          size: dimension,
          distance: distance,
        },
      };

      await this.client.createCollection(
        collectionName.trim(),
        collectionConfig
      );

      return handlers.response.success({
        res,
        message: `Collection '${collectionName}' created successfully`,
        data: { collectionName, dimension, distance },
      });
    } catch (error) {
      console.error("Error creating collection:", error);

      // Handle specific Qdrant errors
      if (error.message?.includes("already exists")) {
        return handlers.response.failed({
          res,
          message: `Collection '${req.body.collectionName}' already exists`,
        });
      }

      return handlers.response.error({
        res,
        message: error.message || "Failed to create collection",
      });
    }
  }

  async deleteCollection(req, res) {
    try {
      const { collectionName } = req.params;

      if (!collectionName?.trim()) {
        return handlers.response.failed({
          res,
          message: "Collection name is required",
        });
      }

      await this.client.deleteCollection(collectionName.trim());

      return handlers.response.success({
        res,
        message: `Collection '${collectionName}' deleted successfully`,
      });
    } catch (error) {
      console.error("Error deleting collection:", error);

      // Handle collection not found error
      if (error.message?.includes("not found") || error.status === 404) {
        return handlers.response.failed({
          res,
          message: `Collection '${req.params.collectionName}' not found`,
        });
      }

      return handlers.response.error({
        res,
        message: error.message || "Failed to delete collection",
      });
    }
  }

  // Additional utility methods
  async collectionExists(collectionName) {
    try {
      const collections = await this.client.getCollections();
      return (
        collections.collections?.some(col => col.name === collectionName) ||
        false
      );
    } catch (error) {
      console.error("Error checking collection existence:", error);
      return false;
    }
  }

  async getCollectionInfo(collectionName) {
    try {
      return await this.client.getCollection(collectionName);
    } catch (error) {
      console.error(
        `Error getting collection info for ${collectionName}:`,
        error
      );
      throw error;
    }
  }
}

module.exports = new QdrantService();
