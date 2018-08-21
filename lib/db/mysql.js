const mysql = require('mysql')
const P = require('../promise')

const REQUIRED_CHARSET = 'UTF8MB4_BIN'
const DATABASE_NAME = 'fxa'

const REQUIRED_SQL_MODES = [
  'STRICT_ALL_TABLES',
  'NO_ENGINE_SUBSTITUTION',
]

// http://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html
const ER_TOO_MANY_CONNECTIONS = 1040
const ER_DUP_ENTRY = 1062
const ER_LOCK_WAIT_TIMEOUT = 1205
const ER_LOCK_TABLE_FULL = 1206
const ER_LOCK_DEADLOCK = 1213
const ER_LOCK_ABORTED = 1689

module.exports = function (log, error) {

  var LOCK_ERRNOS = [
    ER_LOCK_WAIT_TIMEOUT,
    ER_LOCK_TABLE_FULL,
    ER_LOCK_DEADLOCK,
    ER_LOCK_ABORTED
  ]

  // make a pool of connections that we can draw from
  function MySql(options) {
    options.master.database = DATABASE_NAME
    options.slave.database = DATABASE_NAME
    this.options = options
    this.ipHmacKey = options.ipHmacKey

    // poolCluster will remove the pool after `removeNodeErrorCount` errors.
    // We don't ever want to remove a pool because we only have one pool
    // for writing and reading each. Connection errors are mostly out of our
    // control for automatic recovery so monitoring of 503s is critical.
    // Since `removeNodeErrorCount` is Infinity `canRetry` must be false
    // to prevent inifinite retry attempts.
    this.poolCluster = mysql.createPoolCluster(
      {
        removeNodeErrorCount: Infinity,
        canRetry: false
      }
    )

    if (options.charset && options.charset !== REQUIRED_CHARSET) {
      log.error('createPoolCluster.invalidCharset', { charset: options.charset })
      throw new Error('You cannot use any charset besides ' + REQUIRED_CHARSET)
    } else {
      options.charset = REQUIRED_CHARSET
    }

    options.master.charset = options.charset
    options.slave.charset = options.charset

    this.requiredModes = REQUIRED_SQL_MODES
    if (options.requiredSQLModes) {
      this.requiredModes = options.requiredSQLModes.split(',')
      this.requiredModes.forEach(mode => {
        if (! /^[A-Z0-9_]+$/.test(mode)) {
          throw new Error('Invalid SQL mode: ' + mode)
        }
      })
    }

    // Use separate pools for master and slave connections.
    this.poolCluster.add('MASTER', options.master)
    this.poolCluster.add('SLAVE', options.slave)
    this.getClusterConnection = P.promisify(this.poolCluster.getConnection, {
      context: this.poolCluster
    })


    this.statInterval = setInterval(
      reportStats.bind(this),
      options.statInterval || 15000
    )
    this.statInterval.unref()
  }

  function reportStats() {
    var nodes = Object.keys(this.poolCluster._nodes).map(
      function (name) {
        return this.poolCluster._nodes[name]
      }.bind(this)
    )
    var stats = nodes.reduce(
      function (totals, node) {
        totals.errors += node.errorCount
        totals.connections += node.pool._allConnections.length
        totals.queue += node.pool._connectionQueue.length
        totals.free += node.pool._freeConnections.length
        return totals
      },
      {
        stat: 'mysql',
        errors: 0,
        connections: 0,
        queue: 0,
        free: 0
      }
    )
    log.info('stats', stats)
  }

  // this will be called from outside this file
  MySql.connect = function(options) {
    return P.resolve().then(() => {
      var mysql = new MySql(options)

      if (! options.doInitialConnection) {
        return mysql
      }

      var DB_METADATA = 'CALL dbMetadata_1(?)'
      return mysql.readFirstResult(DB_METADATA, ['schema-patch-level'])
        .then((result) => {
          mysql.patchLevel = +result.value
          log.info('connect', {
            patchLevel: mysql.patchLevel
          })
          return mysql
        })
      })
  }

  MySql.prototype.close = function () {
    this.poolCluster.end()
    clearInterval(this.statInterval)
    return P.resolve()
  }

  MySql.prototype.ping = function () {
    return this.getConnection('MASTER')
      .then(
        function(connection) {
          var d = P.defer()
          connection.ping(
            function (err) {
              connection.release()
              return err ? d.reject(err) : d.resolve()
            }
          )
          return d.promise
        }
      )
  }

  // Methods
  const DB_METADATA = 'CALL dbMetadata_1(?)'
  MySql.prototype.getPatchLevel = function () {
    return this.readFirstResult(DB_METADATA, ['schema-patch-level'])
      .then((result) => {
        return +result.value
      })
  }

  // Internal
  MySql.prototype.singleQuery = function (poolName, sql, params) {
    return this.getConnection(poolName)
      .then(
        function (connection) {
          return query(connection, sql, params)
            .then(
              function (result) {
                connection.release()
                return result
              },
              function (err) {
                connection.release()
                throw err
              }
            )
        }
      )
  }

  MySql.prototype.multipleQueries = function (poolName, queries, finalQuery) {
    return this.getConnection(poolName)
      .then(
        function (connection) {
          var results = []
          return P.each(
            queries,
            function (q) {
              return query(connection, q.sql, q.params)
                .then(
                  function (result) {
                    results.push(result)
                  }
                )
            }
          )
          .then(
            function () {
              return results
            }
          )
          .finally(
            function () {
              if (finalQuery) {
                return query(connection, finalQuery.sql, finalQuery.params)
                  .finally(finish)
              }

              finish()

              function finish () {
                connection.release()
              }
            }
          )
        }
      )
  }

  MySql.prototype.transaction = function (fn) {
    return retryable(
      function () {
        return this.getConnection('MASTER')
          .then(
            function (connection) {
              return query(connection, 'BEGIN')
                .then(
                  function () {
                    return fn(connection)
                  }
                )
                .then(
                  function (result) {
                    return query(connection, 'COMMIT')
                      .then(function () { return result })
                  }
                )
                .catch(
                  function (err) {
                    log.error('MySql.transaction', { err: err })
                    return query(connection, 'ROLLBACK')
                      .then(function () { throw err })
                  }
                )
                .then(
                  function (result) {
                    connection.release()
                    return result
                  },
                  function (err) {
                    connection.release()
                    throw err
                  }
                )
            }
          )
      }.bind(this),
      LOCK_ERRNOS
    )
    .catch(
      function (err) {
        throw error.wrap(err)
      }
    )
  }

  MySql.prototype.readFirstResult = function (sql, params) {
    return this.read(sql, params)
      .then(function(results) {
        // instead of the result being [result], it'll be [[result...]]
        if (! results.length) { throw error.notFound() }
        if (! results[0].length) { throw error.notFound() }
        return results[0][0]
      })
  }

  MySql.prototype.readAllResults = function (sql, params) {
    return this.read(sql, params)
      .then(function(results) {
        // instead of the result being [result], it'll be [[result...]]
        if (! results.length) { throw error.notFound() }
        return results[0]
      })
  }

  MySql.prototype.read = function (sql, params) {
    return this.singleQuery('SLAVE*', sql, params)
      .catch(
        function (err) {
          log.error('MySql.read', { sql: sql, id: params, err: err })
          throw error.wrap(err)
        }
      )
  }

  MySql.prototype.readMultiple = function (queries, finalQuery) {
    return this.multipleQueries('SLAVE*', queries, finalQuery)
      .catch(
        function (err) {
          log.error('MySql.readMultiple', { err: err })
          throw error.wrap(err)
        }
      )
  }

  MySql.prototype.write = function (sql, params, resultHandler) {
    return this.singleQuery('MASTER', sql, params)
      .then(
        function (result) {
          log.trace('MySql.write', { sql: sql, result: result })
          if (resultHandler) {
            return resultHandler(result)
          }
          return {}
        },
        function (err) {
          log.error('MySql.write', { sql: sql, err: err })
          if (err.errno === ER_DUP_ENTRY) {
            err = error.duplicate()
          }
          else {
            err = error.wrap(err)
          }
          throw err
        }
      )
  }

  MySql.prototype.writeMultiple = function (queries) {
    return this.transaction(connection => {
      return P.each(queries, ({sql, params, resultHandler}) => {
        return query(connection, sql, params)
        .then(
          function (result) {
            log.trace('MySql.writeMultiple', { sql, result })
            if (resultHandler) {
              return resultHandler(result)
            }
          },
          function (err) {
            log.error('MySql.writeMultiple', { sql, err })
            if (err.errno === ER_DUP_ENTRY) {
              err = error.duplicate()
            }
            else {
              err = error.wrap(err)
            }
            throw err
          }
        )
      })
    })
    .then(() => {
      return {}
    })
  }

  MySql.prototype.getConnection = function (name) {
    return new P((resolve, reject) => {
      retryable(
        this.getClusterConnection.bind(this, name),
        [ER_TOO_MANY_CONNECTIONS, 'ECONNREFUSED', 'ETIMEDOUT', 'ECONNRESET']
      ).then((connection) => {

        if (connection._fxa_initialized) {
          return resolve(connection)
        }

        // Enforce sane defaults on every new connection.
        // These *should* be set by the database by default, but it's nice
        // to have an additional layer of protection here.
        connection.query('SELECT @@sql_mode AS mode;', (err, rows) => {
          if (err) {
            return reject(err)
          }

          const currentModes = rows[0]['mode'].split(',')
          this.requiredModes.forEach(requiredMode => {
            if (currentModes.indexOf(requiredMode) === -1) {
              currentModes.push(requiredMode)
            }
          })

          const newMode = currentModes.join(',')
          connection.query(`SET SESSION sql_mode = '${newMode}';`, (err) => {
            if (err) {
              return reject(err)
            }

            connection.query('SET NAMES utf8mb4 COLLATE utf8mb4_bin;', (err) => {
              if (err) {
                return reject(err)
              }

              connection._fxa_initialized = true
              resolve(connection)
            })
          })
        })
      })
    })
  }

  function query(connection, sql, params) {
    var d = P.defer()
    connection.query(
      sql,
      params || [],
      function (err, results) {
        if (err) { return d.reject(err) }
        d.resolve(results)
      }
    )
    return d.promise
  }

  function retryable(fn, errnos) {
    function success(result) {
      return result
    }
    function failure(err) {
      var errno = err.cause ? err.cause.errno : err.errno
      log.error('MySql.retryable', { err: err })
      if (errnos.indexOf(errno) === -1) {
        throw err
      }
      return fn()
    }
    return fn().then(success, failure)
  }

  // exposed for testing only
  MySql.prototype.retryable_ = retryable

  // Utility method for logging connection config at startup
  MySql.prototype._connectionConfig = function (poolName) {
    var exclude = [
      'pool',
      'password',
      'user',
      'host',
      'port'
    ]

    return this.getConnection(poolName)
      .then(
        function (connection) {
          return query(connection, 'SELECT 1')
            .then(
              function (result) {
                var config = {}
                Object.keys(connection.config).sort().forEach(function(key) {
                  if (exclude.indexOf(key) === -1) {
                    config[key] = connection.config[key]
                  }
                })
                connection.release()
                return config
              },
              function (err) {
                connection.release()
                throw err
              }
            )
        }
      )
  }

  // Utility method for logging charset/collation and other variables at startup
  MySql.prototype._showVariables = function (poolName) {
    var include = [
      'character_set_client',
      'character_set_connection',
      'character_set_database',
      'character_set_filesystem',
      'character_set_results',
      'character_set_server',
      'character_set_system',
      'collation_connection',
      'collation_database',
      'collation_server',
      'max_connections',
      'version',
      'wait_timeout',
      'sql_mode'
    ]

    return this.getConnection(poolName)
      .then(
        function (connection) {
          return query(connection, 'SHOW VARIABLES')
            .then(
              function(variables) {
                var vars = {}
                variables.forEach(function(v) {
                  var name = v.Variable_name
                  if (include.indexOf(name) !== -1) {
                    vars[name] = v.Value
                  }
                })
                connection.release()
                return vars
              },
              function(err) {
                connection.release()
                throw err
              }
            )
        }
      )
  }

  return MySql
}
