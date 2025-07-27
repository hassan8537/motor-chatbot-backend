const qdrantClient = require("../config/qdrant");
const { handlers } = require("./handlers");

// Create Qdrant index for payload filtering (if not already created)
async function createQdrantIndex(collectionName) {
  try {
    await qdrantClient.createPayloadIndex(collectionName, {
      field_name: "key", // change to 'file_key' if you're using that
      field_schema: "keyword",
    });
    console.log("Qdrant payload index ensured");
  } catch (err) {
    if (
      err?.data?.status?.error?.includes("already exists") ||
      err?.message?.includes("already exists")
    ) {
      console.log("Qdrant index already exists");
    } else {
      console.error("Error creating Qdrant index:", err);
    }
  }
}

/**
 * Upsert an embedding to Qdrant
 * @param {Object} params - Parameters for upserting
 * @param {string} params.collectionName - Name of the collection
 * @param {string} params.id - Point ID
 * @param {Array} params.vector - Embedding vector
 * @param {Object} params.payload - Metadata payload
 */
async function upsertEmbeddings({ collectionName, id, vector, payload }) {
  try {
    console.log({ collectionName, id, vector, payload });

    await qdrantClient.upsert(collectionName, {
      points: [{ id, vector, payload }],
    });
    return { success: true, id };
  } catch (err) {
    console.error("Error upserting embedding:", err);
    throw err;
  }
}

/**
 * Delete embeddings from Qdrant based on a payload key-value pair
 * @param {Object} params - Parameters for deletion
 * @param {string} params.collectionName - Name of the collection
 * @param {string} params.key - The payload key to filter by
 * @param {*} params.value - The value to match
 * @param {number} [params.limit=1000] - Maximum number of points to retrieve in one batch
 * @returns {Promise<Object>} Object containing deletion results
 */
async function deleteEmbeddingsByPayloadKey({
  collectionName,
  key,
  value,
  limit = 1000,
}) {
  try {
    let totalDeleted = 0;
    let hasMore = true;
    let offset = null;

    while (hasMore) {
      // Search for points with the specific key-value pair
      const scrollResult = await qdrantClient.scroll(collectionName, {
        filter: {
          must: [
            {
              key: key,
              match: {
                value: value,
              },
            },
          ],
        },
        limit: limit,
        offset: offset,
        with_payload: false,
        with_vector: false,
      });

      if (scrollResult.points && scrollResult.points.length > 0) {
        // Extract point IDs
        const pointIds = scrollResult.points.map((point) => point.id);

        // Delete the points
        await qdrantClient.delete(collectionName, {
          points: pointIds,
        });

        totalDeleted += pointIds.length;
        console.log(
          `Deleted ${pointIds.length} embeddings for ${key}=${value}`
        );

        // Check if there are more results
        offset = scrollResult.next_page_offset;
        hasMore = !!offset;
      } else {
        hasMore = false;
      }
    }

    handlers.logger.success({
      message: `Successfully deleted ${totalDeleted} embeddings for ${key}=${value}`,
    });

    return {
      success: true,
      deletedCount: totalDeleted,
      key,
      value,
    };
  } catch (err) {
    console.error(`Error deleting embeddings by ${key}=${value}:`, err);
    throw err;
  }
}

/**
 * Delete embeddings from Qdrant based on multiple payload criteria
 * @param {Object} params - Parameters for deletion
 * @param {string} params.collectionName - Name of the collection
 * @param {Object} params.filters - Object containing key-value pairs to filter by
 * @param {number} [params.limit=1000] - Maximum number of points to retrieve in one batch
 * @returns {Promise<Object>} Object containing deletion results
 */
async function deleteEmbeddingsByMultipleFilters({
  collectionName,
  filters,
  limit = 1000,
}) {
  try {
    let totalDeleted = 0;
    let hasMore = true;
    let offset = null;

    // Build the filter conditions
    const mustConditions = Object.entries(filters).map(([key, value]) => ({
      key: key,
      match: {
        value: value,
      },
    }));

    while (hasMore) {
      // Search for points with the specific filters
      const scrollResult = await qdrantClient.scroll(collectionName, {
        filter: {
          must: mustConditions,
        },
        limit: limit,
        offset: offset,
        with_payload: false,
        with_vector: false,
      });

      if (scrollResult.points && scrollResult.points.length > 0) {
        // Extract point IDs
        const pointIds = scrollResult.points.map((point) => point.id);

        // Delete the points
        await qdrantClient.delete(collectionName, {
          points: pointIds,
        });

        totalDeleted += pointIds.length;
        console.log(
          `Deleted ${pointIds.length} embeddings for filters:`,
          filters
        );

        // Check if there are more results
        offset = scrollResult.next_page_offset;
        hasMore = !!offset;
      } else {
        hasMore = false;
      }
    }

    return {
      success: true,
      deletedCount: totalDeleted,
      filters,
    };
  } catch (err) {
    console.error("Error deleting embeddings by multiple filters:", err);
    throw err;
  }
}

/**
 * Get count of embeddings matching a payload key-value pair
 * @param {Object} params - Parameters for counting
 * @param {string} params.collectionName - Name of the collection
 * @param {string} params.key - The payload key to filter by
 * @param {*} params.value - The value to match
 * @returns {Promise<number>} Count of matching embeddings
 */
async function countEmbeddingsByPayloadKey({ collectionName, key, value }) {
  try {
    const countResult = await qdrantClient.count(collectionName, {
      filter: {
        must: [
          {
            key: key,
            match: {
              value: value,
            },
          },
        ],
      },
    });

    return countResult.count;
  } catch (err) {
    console.error(`Error counting embeddings by ${key}=${value}:`, err);
    throw err;
  }
}

/**
 * Get embeddings by payload key-value pair (with payload data)
 * @param {Object} params - Parameters for retrieval
 * @param {string} params.collectionName - Name of the collection
 * @param {string} params.key - The payload key to filter by
 * @param {*} params.value - The value to match
 * @param {number} [params.limit=100] - Maximum number of points to retrieve
 * @returns {Promise<Array>} Array of matching points
 */
async function getEmbeddingsByPayloadKey({
  collectionName,
  key,
  value,
  limit = 100,
}) {
  try {
    const scrollResult = await qdrantClient.scroll(collectionName, {
      filter: {
        must: [
          {
            key: key,
            match: {
              value: value,
            },
          },
        ],
      },
      limit: limit,
      with_payload: true,
      with_vector: false,
    });

    return scrollResult.points || [];
  } catch (err) {
    console.error(`Error getting embeddings by ${key}=${value}:`, err);
    throw err;
  }
}

module.exports = {
  createQdrantIndex,
  upsertEmbeddings,
  deleteEmbeddingsByPayloadKey,
  deleteEmbeddingsByMultipleFilters,
  countEmbeddingsByPayloadKey,
  getEmbeddingsByPayloadKey,
};
