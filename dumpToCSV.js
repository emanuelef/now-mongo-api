const MongoClient = require("mongodb").MongoClient;
const url = require("url");
const fs = require("fs");
const Position = require("air-commons").Position;
const TimedPosition = require("air-commons").TimedPosition;
const {
  getLastString,
  cleanOperator,
  icaoAirport,
} = require("air-commons").utils;

const MONGODB_URI =
  "mongodb+srv://admin:7C4TiWt0ysNFoTmI@cluster0.xxocw.mongodb.net/lhr-passages?retryWrites=true&w=majority";
//"mongodb://admin:ashbeck19@ds343887.mlab.com:43887/lhr-passages";

let cachedDb = null;

async function connectToDatabase(uri) {
  if (cachedDb) {
    return cachedDb;
  }

  const client = await MongoClient.connect(uri, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  });
  const db = await client.db(url.parse(uri).pathname.substr(1));

  // Cache the database connection and return the connection
  cachedDb = db;
  return db;
}

async function dump() {
  const db = await connectToDatabase(MONGODB_URI);
  const collection = await db.collection("flights");
  const end = Math.floor(Date.now() / 1000);
  const start = Math.floor(Date.now() / 1000) - 24 * 60 * 60;

  console.log(await collection.stats())

  const cursor = await collection.find({
    /*startTime: { $gte: Number(start), $lt: Number(end) }*/
    startTime: { $gt: Number(1561795958) },
  });

  cont = 0;

  const dateFormatted = new Date()
    .toJSON()
    .slice(0, 10)
    .split("-")
    .reverse()
    .join("_");
  let stream = fs.createWriteStream(`dump_${dateFormatted}.csv`, {
    flags: "w",
  });

  let headerDone = false;

  const POSTION_OF_INTEREST = new Position({
    lat: 51.47676705,
    lon: -0.35027019,
    alt: 10,
  });

  while (await cursor.hasNext()) {
    let doc = await cursor.next();

    //console.log(doc.icao, doc.startTime)

    const timedPositions = doc.positions.map(
      (pos) =>
        new TimedPosition({
          lat: pos[0],
          lon: pos[1],
          alt: pos[2],
          timestamp: pos[3],
        })
    );

    /*
    const minimum = TimedPosition.getMinimumDistanceToPosition(
      timedPositions,
      POSTION_OF_INTEREST
    );
    

    doc = { ...doc, ...minimum };
    */

    const {
      icao,
      op,
      from,
      to,
      startTime,
      startLat,
      startLon,
      startAltitude,
      speedAtCreation,
      verticalSpeedAtCreation,
      wakeTurbulence,
      wDeg,
      wSpeed,
      minDistance,
      samples,
      ...partialObject
    } = doc;
    const subset = {
      icao,
      op,
      from,
      to,
      from,
      to,
      startTime,
      startLat,
      startLon,
      startAltitude,
      speedAtCreation,
      verticalSpeedAtCreation,
      wakeTurbulence,
      wDeg,
      wSpeed,
      minDistance,
      samples,
    };

    subset.op = cleanOperator(subset.op);
    subset.fromCountry = getLastString(subset.from);
    subset.toCountry = getLastString(subset.to);
    subset.from = icaoAirport(subset.from);
    subset.to = icaoAirport(subset.to);

    //console.log(cont++, subset);

    if (!headerDone) {
      stream.write(Object.keys(subset).join() + "\n");
      headerDone = true;
    }

    await stream.write(Object.values(subset).join() + "\n");
  }

  await new Promise((r) => setTimeout(r, 7000));

  await stream.end();
  process.exit();
}

dump();
