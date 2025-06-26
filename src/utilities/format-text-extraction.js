function formatTextDetection(blocks) {
  return blocks
    .filter((block) => block.BlockType === "LINE")
    .map((line) => line.Text)
    .join("\n");
}

module.exports = formatTextDetection;
