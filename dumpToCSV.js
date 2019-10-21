const MongoClient = require("mongodb").MongoClient;
const url = require("url");
const fs = require("fs");
const Position = require("air-commons").Position;
const TimedPosition = require("air-commons").TimedPosition;

const MONGODB_URI =
  "mongodb://admin:ashbeck19@ds343887.mlab.com:43887/lhr-passages";

let cachedDb = null;

const getLastString = el =>
  el
    ? el
        .split(",")
        .pop()
        .trim()
    : "";

const cleanOperator = el => (el ? el.replace(",", "").trim() : "");

async function connectToDatabase(uri) {
  if (cachedDb) {
    return cachedDb;
  }

  const client = await MongoClient.connect(uri, {
    useNewUrlParser: true,
    useUnifiedTopology: true
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

  const cursor = await collection.find({
    /*startTime: { $gte: Number(start), $lt: Number(end) }*/
  });

  cont = 0;

  let stream = fs.createWriteStream("dump-icao-samples.csv", {
    flags: "w"
  });

  let headerDone = false;

  const POSTION_OF_INTEREST = new Position({
    lat: 51.47676705,
    lon: -0.35027019,
    alt: 10
  });

  while (await cursor.hasNext()) {
    let doc = await cursor.next();

    const timedPositions = doc.positions.map(
      pos =>
        new TimedPosition({
          lat: pos[0],
          lon: pos[1],
          alt: pos[2],
          timestamp: pos[3]
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
      samples
    };

    subset.op = cleanOperator(subset.op);
    subset.from = getLastString(subset.from);
    subset.to = getLastString(subset.to);

    //console.log(cont++, subset);

    if (!headerDone) {
      stream.write(Object.keys(subset).join() + "\n");
      headerDone = true;
    }

    stream.write(Object.values(subset).join() + "\n");
  }

  stream.end();
  process.exit();
}

dump();
