const {parentPort, workerData, threadId} = require('worker_threads')

parentPort.on('message', rows => {
    parentPort.postMessage(compute(rows))
})

function compute(rows) {
    const distances = [
        {
          row_id: rows[0].row_id,
          vehicle_id: rows[0].vehicle_id,
          latitude: rows[0].latitude,
          longitude: rows[0].longitude,
          distance_from_prev: 0,
          worker_id: threadId
        }
    ]

    for (let i = 1; i < rows.length; i++) {
        const a = rows[i], b = rows[i - 1]

        distances.push(
            {
                row_id: a.row_id,
                vehicle_id: a.vehicle_id,
                latitude: a.latitude,
                longitude: a.longitude,
                distance_from_prev: distance(a, b),
                worker_id: threadId
            }
        )
    }

    return distances
}

function distance(a, b) {
    return Math.sqrt(
        (a.latitude - b.latitude) ** 2 +
        (a.longitude - b.longitude) ** 2
    )
}
