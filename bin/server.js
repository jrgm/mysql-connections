'use strict'

const pidusage = require('pidusage')

const config = require('../config')
const error = require('../lib/error')
const logger = require('../lib/logging')
const DB = require('../lib/db/mysql')(logger, error)
const P = require('../lib/promise')

let initialMysqlPidUsage

async function getPidUsage() {
  const stats = await pidusage(config.mysqldPid)
  return stats
}

async function shutdown() {
  logger.info('shutdown()')
  // Calculate final CPU usage for mysqld and output how much was used.
  const finalMysqlPidUsage = await getPidUsage()
  logger.info('mysqldCTime', finalMysqlPidUsage.ctime - initialMysqlPidUsage.ctime)
  process.exit(0)
}

function doQuery(db) {
  return db.getPatchLevel()
}

async function main() {
  logger.info('config', JSON.stringify(config))

  DB.connect(config)
    .done((db) => {
      //logger.info('db', JSON.stringify(db.options), db)

      setTimeout(shutdown, config.lifetime)

      const promises = []
      for (let i = 0; i < config.initialConnections; ++i) {
        const promise = doQuery(db)
        promises.push(promise)
      }

      return P.all(promises)
        .then((result) => {
          logger.info('doQuery result', JSON.stringify(result))
          // Now that all the connections have been set up, record initial cpu time for mysqld
          getPidUsage().then((usage) => {
            initialMysqlPidUsage = usage
          })
        })
        .catch((err) => {
          logger.error('db.getPatchLevel', err)
          throw err
        })
    })
}

main()

