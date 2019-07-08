// https://zeit.co/guides/deploying-a-mongodb-powered-api-with-node-and-now

const url = require("url");
const MongoClient = require("mongodb").MongoClient;
const zlib = require("zlib");
const Position = require("air-commons").Position;
const TimedPosition = require("air-commons").TimedPosition;

let cachedDb = null;

const HOME_POSITION_COORDINATES = {
  latitude: 51.444137,
  longitude: -0.351227,
  elevation: 16
};

const POSTION_OF_INTEREST = new Position({
  lat: HOME_POSITION_COORDINATES.latitude,
  lon: HOME_POSITION_COORDINATES.longitude,
  alt: HOME_POSITION_COORDINATES.elevation
});

const INTERPOLATION_DISTANCE = 100;

const testDistanceCalculation = (position, allFlights) => {
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

    const { minDistance } = TimedPosition.getMinimumDistanceToPosition(
      timedPositions,
      position
    );

    if (flight.minDistance < 1000) {
      console.log(
        flight.icao,
        minDistance,
        new Date(flight.startTime * 1000),
        flight.minDistance,
        flight.minDistanceAccurate,
        Math.abs(minDistance - flight.minDistance),
        flight.startTime
      );
    }
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

  const client = await MongoClient.connect(uri, { useNewUrlParser: true });
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

  if (Number(query.interpolation) === 1) {
    addInterpolatedPositions(results);
  }

  res.setHeader("Content-Type", "application/json");
  res.setHeader("Content-Encoding", "gzip");
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader(
    "Access-Control-Allow-Headers",
    "Origin, X-Requested-With, Content-Type, Accept"
  );

  const buf = Buffer.from(JSON.stringify(results));
  zlib.gzip(buf, (_, result) => {
    res.end(result);
  });
};
