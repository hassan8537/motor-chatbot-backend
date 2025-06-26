function parseDateRange(startDate, endDate) {
  const pad = (n) => String(n).padStart(2, "0");
  const today = new Date();
  const defaultDate = `${pad(today.getDate())}-${pad(today.getMonth() + 1)}-${today.getFullYear()}`;

  const parse = (dateStr, endOfDay = false) => {
    const [day, month, year] = (dateStr || defaultDate).split("-");
    const iso = new Date(
      `${year}-${month}-${day}T${endOfDay ? "23:59:59.999Z" : "00:00:00.000Z"}`
    );
    if (isNaN(iso)) throw new Error("Invalid date format. Use DD-MM-YYYY.");
    return iso.toISOString();
  };

  return {
    fromDate: parse(startDate),
    toDate: parse(endDate, true)
  };
}

module.exports = parseDateRange;
