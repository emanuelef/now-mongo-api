// https://zeit.co/guides/deploying-a-mongodb-powered-api-with-node-and-now

// http://localhost:3000/api/flightsNear.js?start=1567295999&end=1567382399&lat=51.451824059&lon=-0.3474

const url = require("url");
const MongoClient = require("mongodb").MongoClient;
const zlib = require("zlib");
const Position = require("air-commons").Position;
const TimedPosition = require("air-commons").TimedPosition;
const { Worker } = require("worker_threads");
const path = require("path");
const os = require("os");

let cachedDb = null;

const cpuCount = os.cpus().length;
const workerScript = path.join(__dirname, "./minDistanceCalculator.js");

//console.log("CPU", cpuCount);

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

const calculateMonDistanceWithWorker = (position, flights) => {
  return new Promise((resolve, reject) => {
    const worker = new Worker(workerScript, {
      workerData: {
        position,
        flights
      }
    });
    worker.on("message", resolve);
    worker.on("error", reject);
  });
};

async function distributeLoadAcrossWorkers(position, flights, workers) {
  const segmentsPerWorker = Math.round(flights.length / workers);
  const promises = Array(workers)
    .fill()
    .map((_, index) => {
      let arrayToSort;
      if (index === 0) {
        // the first segment
        arrayToSort = flights.slice(0, segmentsPerWorker);
      } else if (index === workers - 1) {
        // the last segment
        arrayToSort = flights.slice(segmentsPerWorker * index);
      } else {
        // intermediate segments
        arrayToSort = flights.slice(
          segmentsPerWorker * index,
          segmentsPerWorker * (index + 1)
        );
      }
      return calculateMonDistanceWithWorker(position, arrayToSort);
    });
  // merge all the segments of the array
  const segmentsResults = await Promise.all(promises);
  return segmentsResults.reduce((acc, arr) => acc.concat(arr), []);
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

  //console.log(query.start, query.end);
  //console.log(start, end);

  // Select the users collection from the database
  const results = await collection
    .find({ startTime: { $gte: Number(query.start), $lt: Number(query.end) } })
    .toArray();

  const POSTION_OF_INTEREST = new Position({
    lat: query.lat,
    lon: query.lon,
    alt: 10
  });

  hrstart = process.hrtime();
  let cleanedResults = await distributeLoadAcrossWorkers(
    POSTION_OF_INTEREST,
    results,
    cpuCount
  );

  cleanedResults = cleanedResults.map(({ _id, positions, ...item }) => item);

  hrend = process.hrtime(hrstart);
  console.info(
    "Calculate Distance Execution time (hr): %ds %dms",
    hrend[0],
    hrend[1] / 1000000
  );

  //console.log("TOT", cleanedResults.length);

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
