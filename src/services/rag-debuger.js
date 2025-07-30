/**
 * Comprehensive RAG Debugging Tool for Drilling Reports
 * This will help identify exactly where the circulation hours data is being lost
 */

const getEmbedding = require("../utilities/get-embedding");
const searchResults = require("../utilities/search-results");

class RAGDrillingDebugger {
  constructor(searchService) {
    this.searchService = searchService;
  }

  /**
   * STEP 1: Debug what chunks actually exist in your database
   */
  async debugChunkContent(
    collectionName,
    searchTerms = ["circ hrs", "circulation hours", "12.25", "bha"]
  ) {
    console.log("🔍 === DEBUGGING CHUNK CONTENT ===");

    try {
      // Search with very broad criteria to see what's actually stored
      const broadResults = await searchResults({
        collectionName,
        embedding: await this.generateDebugEmbedding(
          "BHA circulation hours drilling report"
        ),
        limit: 100, // Get lots of results
        scoreThreshold: 0.1, // Very low threshold
        enableReranking: false,
      });

      console.log(`📊 Found ${broadResults.length} total chunks in database`);

      // Analyze chunks for drilling content
      const drillingChunks = broadResults.filter(r =>
        searchTerms.some(term =>
          r.payload.content?.toLowerCase().includes(term.toLowerCase())
        )
      );

      console.log(
        `🎯 Found ${drillingChunks.length} chunks containing search terms`
      );

      // Show examples of what we found
      drillingChunks.slice(0, 5).forEach((chunk, i) => {
        console.log(`\n--- CHUNK ${i + 1} ---`);
        console.log(`Score: ${chunk.score}`);
        console.log(`File: ${chunk.payload.name || "Unknown"}`);
        console.log(
          `Content Preview: ${chunk.payload.content?.substring(0, 300)}...`
        );

        // Look for circulation hours specifically
        const circMatches =
          chunk.payload.content?.match(/circ\s*hrs?\s*[:\s]*\d+\.?\d*/gi) || [];
        if (circMatches.length > 0) {
          console.log(`🎯 CIRCULATION HOURS FOUND: ${circMatches.join(", ")}`);
        }

        // Look for BHA identifiers
        const bhaMatches =
          chunk.payload.content?.match(/bha\s*[a-z0-9\-_\s]*[:\s]*\d+/gi) || [];
        if (bhaMatches.length > 0) {
          console.log(`🔧 BHA DATA FOUND: ${bhaMatches.join(", ")}`);
        }

        // Look for hole sections
        const holeMatches =
          chunk.payload.content?.match(/\d+\.?\d*\s*(?:inch|")\s*hole/gi) || [];
        if (holeMatches.length > 0) {
          console.log(`🕳️ HOLE SECTIONS FOUND: ${holeMatches.join(", ")}`);
        }
      });

      return {
        totalChunks: broadResults.length,
        drillingChunks: drillingChunks.length,
        examples: drillingChunks.slice(0, 10),
      };
    } catch (error) {
      console.error("❌ Debug chunk content failed:", error);
      return null;
    }
  }

  /**
   * STEP 2: Debug embedding similarity for your specific query
   */
  async debugQueryEmbedding(query, collectionName) {
    console.log("\n🧠 === DEBUGGING QUERY EMBEDDING ===");
    console.log(`Query: "${query}"`);

    try {
      // Test different query variations
      const queryVariations = [
        query, // Original
        "BHA circulation hours 12.25 inch hole section", // Normalized
        "bottom hole assembly circ hrs twelve point two five", // Expanded
        "drilling BHA configuration circulation time hole diameter", // Semantic
        "motor drilling assembly operating hours performance data", // Broader
      ];

      for (const testQuery of queryVariations) {
        console.log(`\n🔬 Testing query: "${testQuery}"`);

        const embedding = await this.generateDebugEmbedding(testQuery);

        const results = await searchResults({
          collectionName,
          embedding,
          limit: 20,
          scoreThreshold: 0.3, // Lower threshold for debugging
          enableReranking: false,
        });

        console.log(`📊 Results found: ${results.length}`);

        // Check top results for relevant content
        const relevantResults = results.filter(r => {
          const content = r.payload.content?.toLowerCase() || "";
          return (
            content.includes("circ") ||
            content.includes("bha") ||
            content.includes("12.25") ||
            content.includes("circulation")
          );
        });

        console.log(`🎯 Relevant results: ${relevantResults.length}`);

        if (relevantResults.length > 0) {
          console.log("🎉 FOUND RELEVANT CONTENT:");
          relevantResults.slice(0, 3).forEach((r, i) => {
            console.log(
              `  ${i + 1}. Score: ${r.score.toFixed(
                3
              )} - ${r.payload.content?.substring(0, 150)}...`
            );
          });
        }
      }
    } catch (error) {
      console.error("❌ Debug query embedding failed:", error);
    }
  }

  /**
   * STEP 3: Debug chunk preprocessing and enhancement
   */
  async debugChunkProcessing(sampleText) {
    console.log("\n✂️ === DEBUGGING CHUNK PROCESSING ===");

    try {
      // Test your chunking process
      const chunks = await chunkText(sampleText);
      console.log(`📋 Generated ${chunks.length} chunks`);

      chunks.forEach((chunk, i) => {
        console.log(`\n--- PROCESSED CHUNK ${i + 1} ---`);
        console.log(`Length: ${chunk.length} characters`);
        console.log(`Content: ${chunk.substring(0, 200)}...`);

        // Check if chunk contains circulation data
        const hasCirc = /circ\s*hrs?\s*[:\s]*\d+/i.test(chunk);
        const hasBHA = /bha/i.test(chunk);
        const hasHoleSize = /\d+\.?\d*\s*(?:inch|")/i.test(chunk);

        console.log(
          `🔍 Analysis: Circ=${hasCirc}, BHA=${hasBHA}, HoleSize=${hasHoleSize}`
        );

        if (hasCirc || hasBHA || hasHoleSize) {
          console.log("🎯 THIS CHUNK CONTAINS RELEVANT DATA!");
        }
      });

      return chunks;
    } catch (error) {
      console.error("❌ Debug chunk processing failed:", error);
      return [];
    }
  }

  /**
   * STEP 4: Test full end-to-end RAG pipeline
   */
  async debugFullPipeline(query, collectionName) {
    console.log("\n🔄 === DEBUGGING FULL PIPELINE ===");

    try {
      // Step 1: Generate embedding with different enhancements
      console.log("1️⃣ Testing embedding generation...");

      const standardEmbedding = await getEmbedding(query, {
        enhanceDrillingContext: false,
      });

      const enhancedEmbedding = await getEmbedding(query, {
        enhanceDrillingContext: true,
      });

      console.log(`📊 Standard embedding length: ${standardEmbedding.length}`);
      console.log(`📊 Enhanced embedding length: ${enhancedEmbedding.length}`);

      // Step 2: Test search with different parameters
      console.log("\n2️⃣ Testing search parameters...");

      const searchConfigs = [
        { limit: 10, threshold: 0.6, name: "Current Config" },
        { limit: 50, threshold: 0.3, name: "Broad Search" },
        { limit: 30, threshold: 0.4, name: "Medium Search" },
        { limit: 100, threshold: 0.2, name: "Very Broad" },
      ];

      for (const config of searchConfigs) {
        console.log(`\n🔬 Testing: ${config.name}`);

        const results = await searchResults({
          collectionName,
          embedding: enhancedEmbedding,
          limit: config.limit,
          scoreThreshold: config.threshold,
          enableReranking: true,
        });

        console.log(`📊 Results: ${results.length}`);

        // Check for circulation hours in results
        const circResults = results.filter(r =>
          /circ\s*hrs?\s*[:\s]*\d+/i.test(r.payload.content || "")
        );

        if (circResults.length > 0) {
          console.log(
            `🎯 FOUND ${circResults.length} CHUNKS WITH CIRCULATION HOURS!`
          );
          circResults.slice(0, 2).forEach((r, i) => {
            const matches =
              r.payload.content?.match(/circ\s*hrs?\s*[:\s]*\d+\.?\d*/gi) || [];
            console.log(
              `  ${i + 1}. Score: ${r.score.toFixed(
                3
              )} - Circ Hours: ${matches.join(", ")}`
            );
          });
        }
      }
    } catch (error) {
      console.error("❌ Debug full pipeline failed:", error);
    }
  }

  /**
   * Helper: Generate embedding for debugging
   */
  async generateDebugEmbedding(text) {
    return await getEmbedding(text, {
      enhanceDrillingContext: true,
      model: "text-embedding-3-small",
    });
  }

  /**
   * MAIN DEBUG METHOD: Run all tests
   */
  async runCompleteDiagnosis(query, collectionName) {
    console.log("🚀 === STARTING COMPLETE RAG DIAGNOSIS ===");
    console.log(`Query: "${query}"`);
    console.log(`Collection: ${collectionName}`);
    console.log("=" * 60);

    // Step 1: Check what's in the database
    const chunkAnalysis = await this.debugChunkContent(collectionName);

    // Step 2: Test query embedding variations
    await this.debugQueryEmbedding(query, collectionName);

    // Step 3: Test full pipeline
    await this.debugFullPipeline(query, collectionName);

    console.log("\n🏁 === DIAGNOSIS COMPLETE ===");
    console.log(
      "Check the logs above to identify where circulation hours data is being lost."
    );

    return chunkAnalysis;
  }
}

module.exports = RAGDrillingDebugger;
