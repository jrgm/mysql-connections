const mysqlConnectionConfig = {
  user: process.env.MYSQL_USER || 'root',
  password: process.env.MYSQL_PASSWORD || '',
  host: process.env.MYSQL_HOST || '127.0.0.1',
  port: process.env.MYSQL_PORT || 3306,
  connectionLimit: process.env.MYSQL_CONNECTION_LIMIT || 10000, // i.e., no limit
  waitForConnections: process.env.MYSQL_WAIT_FOR_CONNECTIONS || true,
  queueLimit: process.env.MYSQL_QUEUE_LIMIT || 10000 // i.e., no limit
}

let doInitialConnection = true
if (process.env.DO_INITIAL_CONNECTION === '0' || process.env.DO_INITIAL_CONNECTION === 'false') {
  doInitialConnection = false
}

const initialConnections = parseInt(process.env.INITIAL_CONNECTIONS || 1, 10) - 1
const lifetime = parseInt(process.env.LIFETIME || 60000, 10) + 1
const mysqldPid = process.env.MYSQLD_PID

if (! mysqldPid) {
  throw new Error('process.env.MYSQLD_PID is required')
}

module.exports = {
  doInitialConnection: doInitialConnection,
  initialConnections: initialConnections,
  lifetime: lifetime,
  mysqldPid: mysqldPid,
  master: mysqlConnectionConfig,
  slave: mysqlConnectionConfig
}

