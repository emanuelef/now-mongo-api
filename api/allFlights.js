// https://zeit.co/guides/deploying-a-mongodb-powered-api-with-node-and-now

const url = require("url");
const MongoClient = require("mongodb").MongoClient;
const zlib = require("zlib");

let cachedDb = null;

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
  const db = await connectToDatabase(process.env.MONGODB_URI);

  const collection = await db.collection("flights");

  const end = Math.floor(Date.now() / 1000);
  const start = Math.floor(Date.now() / 1000) - 60 * 60;

  // Select the users collection from the database
  const results = await collection
    .find({ startTime: { $gte: start, $lt: end } })
    .toArray();

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
