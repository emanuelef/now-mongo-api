const { parentPort, workerData, isMainThread } = require("worker_threads");
const Position = require("air-commons").Position;
const TimedPosition = require("air-commons").TimedPosition;

const minDistanceCalculation = (position, flights) => {
  return flights.map(flight => {
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

if (!isMainThread) {
  if (!Array.isArray(workerData["flights"])) {
    throw new Error("workerData must be an array");
  }

  const POSTION_OF_INTEREST = new Position({
    lat: workerData["position"].lat,
    lon: workerData["position"].lon,
    alt: workerData["position"].alt
  });

  parentPort.postMessage(
    minDistanceCalculation(POSTION_OF_INTEREST, workerData["flights"])
  );
}
