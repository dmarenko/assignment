const config = require('./config.json')
const {parse} = require('csv-parse/sync')
const fs = require('fs')
const {Worker} = require('worker_threads')
const {chain, groupBy, values} = require('lodash')

const fileContent = fs.readFileSync('./data.csv', 'utf8')
const parsed = parse(fileContent, {bom: true, cast: true, columns: true})
const data = chain(parsed).groupBy(row => row.vehicle_id).values().value()

const { stringify } = require('csv-stringify')

const result = []

let tasks = 0

for (let i = 0; i < config.workerCount; i++) {
    const worker = new Worker('./worker.js')

    const workerId = worker.threadId

    worker.on('message', (rows) => {

        tasks--

        result.push(...rows)

        if (data.length > 0) {
            tasks++
            worker.postMessage(data.pop())
        } else if (tasks === 0) { // processed all vehicles
            const result_sorted = result.sort((a, b) => a.row_id - b.row_id)
            stringify(result_sorted, {
              header: true
            }, function(err, data){
              fs.writeFileSync('output.csv', data)
              process.exit(0)
            })
        }
    })

    worker.on('error', error => {
        console.log(error)
    })

    if (data.length > 0) {
        tasks++
        worker.postMessage(data.pop())
    }
        
}

setTimeout(function() { }, 3600000) // don't end thread
