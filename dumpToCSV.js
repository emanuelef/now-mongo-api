const MongoClient = require("mongodb").MongoClient;
const url = require("url");

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
    startTime: { $gte: Number(start), $lt: Number(end) }
  });

  cont = 0;

  while (await cursor.hasNext()) {
    const doc = await cursor.next();
    const {
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
      ...partialObject
    } = doc;
    const subset = {
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
      minDistance
    };

    subset.from = getLastString(subset.from);
    subset.to = getLastString(subset.to);

    console.log(cont++, subset);
  }
}

dump().then(console.log("Done!"));
