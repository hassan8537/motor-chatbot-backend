function formatDocumentAnalysis(blocks) {
  const blockMap = Object.fromEntries(blocks.map((b) => [b.Id, b]));
  const keyMap = {};
  const valueMap = {};
  const tableCells = [];

  for (const block of blocks) {
    if (block.BlockType === "KEY_VALUE_SET") {
      const isKey = block.EntityTypes.includes("KEY");
      if (isKey) keyMap[block.Id] = block;
      else valueMap[block.Id] = block;
    }

    if (block.BlockType === "CELL") {
      tableCells.push(block);
    }
  }

  // Extract key-value pairs
  const formData = Object.values(keyMap).map((keyBlock) => {
    const valueIds = (keyBlock.Relationships || [])
      .filter((r) => r.Type === "VALUE")
      .flatMap((r) => r.Ids);
    const valueTexts = valueIds.map((id) => getText(valueMap[id], blockMap));
    return {
      key: getText(keyBlock, blockMap),
      value: valueTexts.join(" ")
    };
  });

  // Extract tables
  const tables = {};
  for (const cell of tableCells) {
    const tableId = cell.TableId || "default";
    if (!tables[tableId]) tables[tableId] = [];
    tables[tableId].push({
      row: cell.RowIndex,
      col: cell.ColumnIndex,
      text: getText(cell, blockMap)
    });
  }

  return {
    forms: formData,
    tables
  };
}

// Helper to get full text from a block
function getText(block, blockMap) {
  return (block.Relationships || [])
    .filter((r) => r.Type === "CHILD")
    .flatMap((r) => r.Ids)
    .map((id) => blockMap[id]?.Text || "")
    .join(" ");
}

module.exports = formatDocumentAnalysis;
