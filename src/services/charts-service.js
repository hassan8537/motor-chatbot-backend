const { ScanCommand } = require("@aws-sdk/lib-dynamodb");
const { docClient } = require("../config/aws");
const { handlers } = require("../utilities/handlers");
const parseDateRange = require("../utilities/parse-date-range");

const TABLE_NAME = process.env.DYNAMODB_TABLE_NAME;

class Service {
  async getTotalQueries(req, res) {
    try {
      const { startDate, endDate } = req.query;
      const userId = req.user?.UserId;

      if (!userId) {
        return handlers.response.error({ res, message: "Missing userId" });
      }

      const { fromDate, toDate } = parseDateRange(startDate, endDate);

      const params = {
        TableName: TABLE_NAME,
        FilterExpression:
          "UserId = :userId AND begins_with(PK, :prefix) AND CreatedAt BETWEEN :fromDate AND :toDate",
        ExpressionAttributeValues: {
          ":userId": userId,
          ":prefix": "QUERY#",
          ":fromDate": fromDate,
          ":toDate": toDate
        }
      };

      const data = await docClient.send(new ScanCommand(params));

      // Create daily breakdown
      const dateMap = {};
      for (const item of data.Items || []) {
        const dateOnly = new Date(item.CreatedAt).toISOString().split("T")[0];
        dateMap[dateOnly] = (dateMap[dateOnly] || 0) + 1;
      }

      const breakdown = Object.entries(dateMap).map(([date, totalQueries]) => ({
        date,
        totalQueries
      }));

      return handlers.response.success({
        res,
        message: "Total queries breakdown fetched successfully",
        data: {
          total: data.Items.length,
          breakdown
        }
      });
    } catch (error) {
      return handlers.response.error({ res, message: error.message });
    }
  }

  async getUsage(req, res) {
    try {
      const { startDate, endDate } = req.query;
      const userId = req.user?.UserId;

      if (!userId) {
        return handlers.response.error({ res, message: "Missing userId" });
      }

      const { fromDate, toDate } = parseDateRange(startDate, endDate);

      const params = {
        TableName: TABLE_NAME,
        FilterExpression:
          "UserId = :userId AND CreatedAt BETWEEN :fromDate AND :toDate",
        ExpressionAttributeValues: {
          ":userId": userId,
          ":fromDate": fromDate,
          ":toDate": toDate
        }
      };

      const data = await docClient.send(new ScanCommand(params));

      // Sum TotalTokens
      const totalTokens = data.Items.reduce((sum, item) => {
        return sum + (item.TotalTokens || 0);
      }, 0);

      // Example: cost for GPT-4 is approx $0.03 per 1K tokens
      const estimatedUsd = (totalTokens / 1000) * 0.03;

      return handlers.response.success({
        res,
        data: {
          startDate: fromDate.slice(0, 10),
          endDate: toDate.slice(0, 10),
          totalTokens,
          estimatedUsd: parseFloat(estimatedUsd.toFixed(4))
        }
      });
    } catch (error) {
      return handlers.response.error({ res, message: error.message });
    }
  }
}

module.exports = new Service();
