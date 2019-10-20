const fs = require("fs");
const csv = require("fast-csv");

let recent = {};
let total = 0;

const checkGap = row => {
  if (row.icao in recent) {
    let gap = Math.abs(recent[row.icao] - row.startTime);
    if (gap < 120) {
      total++;
      console.log(total, row.icao, gap);
    }
  }
  recent[row.icao] = row.startTime;
};

fs.createReadStream("dump-dev-icao.csv")
  .pipe(csv.parse({ headers: true }))
  .on("error", error => console.error(error))
  .on("data", checkGap)
  .on("end", rowCount =>
    console.log(`Parsed ${rowCount} rows - ${Object.keys(recent).length} icao`)
  );
