// https://zeit.co/guides/deploying-a-mongodb-powered-api-with-node-and-now

// http://localhost:3000/api/flightsNear.js?start=1567295999&end=1567382399&lat=51.451824059&lon=-0.3474

const url = require("url");
const MongoClient = require("mongodb").MongoClient;
const zlib = require("zlib");
const Position = require("air-commons").Position;
const TimedPosition = require("air-commons").TimedPosition;

let cachedDb = null;

const INTERPOLATION_DISTANCE = 100;

const minDistanceCalculation = (position, allFlights) => {
  return allFlights.map(flight => {
    const timedPositions = flight.positions.map(
      pos =>
        new TimedPosition({
          lat: pos[0],
          lon: pos[1],
          alt: pos[2],
          timestamp: pos[3]
        })
    );

    const minimum = TimedPosition.getMinimumDistanceToPosition(
      timedPositions,
      position
    );

    flightRecalculated = { ...flight, ...minimum };
    return flightRecalculated;
  });
};

const addInterpolatedPositions = (
  allFlights,
  distanceBetweenSamples = INTERPOLATION_DISTANCE
) => {
  allFlights.forEach(flight => {
    const timedPositions = flight.positions.map(
      pos =>
        new TimedPosition({
          lat: pos[0],
          lon: pos[1],
          alt: pos[2],
          timestamp: pos[3]
        })
    );

    flight.positions = TimedPosition.getSubsampledPositions(
      timedPositions,
      distanceBetweenSamples
    ).map(pos => [pos.lat, pos.lon, pos.alt, pos.timestamp]);
  });
};

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

// The main, exported, function of the endpoint,
// dealing with the request and subsequent response
module.exports = async (req, res) => {
  // Get a database connection, cached or otherwise,
  // using the connection string environment variable as the argument

  const url_parts = url.parse(req.url, true);
  const query = url_parts.query;

  /*
  if (!("start" in query && "end" in query)) {
    res.status(400);
    return;
  }
  */

  const db = await connectToDatabase(process.env.MONGODB_URI);

  const collection = await db.collection("flights");

  const end = Math.floor(Date.now() / 1000);
  const start = Math.floor(Date.now() / 1000) - 24 * 60 * 60;

  console.log(query.start, query.end);
  console.log(start, end);

  // Select the users collection from the database
  const results = await collection
    .find({ startTime: { $gte: Number(query.start), $lt: Number(query.end) } })
    .toArray();

  //console.log(results[results.length - 1]);
  //console.log(new Date(results[results.length - 1].startTime * 1000));

  let hrstart = process.hrtime();
  //addInterpolatedPositions(results);
  let hrend = process.hrtime(hrstart);
  console.info(
    "Add interpolated Execution time (hr): %ds %dms",
    hrend[0],
    hrend[1] / 1000000
  );

  const POSTION_OF_INTEREST = new Position({
    lat: query.lat,
    lon: query.lon,
    alt: 10
  });

  // remove positions
  hrstart = process.hrtime();
  let cleanedResults = minDistanceCalculation(POSTION_OF_INTEREST, results).map(
    ({ positions, ...item }) => item
  );
  hrend = process.hrtime(hrstart);
  console.info(
    "Calculate Distance Execution time (hr): %ds %dms",
    hrend[0],
    hrend[1] / 1000000
  );

  console.log("TOT", cleanedResults.length);

  res.setHeader("Content-Type", "application/json");
  res.setHeader("Content-Encoding", "gzip");
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader(
    "Access-Control-Allow-Headers",
    "Origin, X-Requested-With, Content-Type, Accept"
  );

  const buf = Buffer.from(JSON.stringify(cleanedResults));
  zlib.gzip(buf, (_, result) => {
    res.end(result);
  });
};
