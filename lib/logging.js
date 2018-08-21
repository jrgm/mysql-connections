const util = require('util')

function logger(/* arguments */) {
  const args = Array.prototype.slice.call(arguments)
  const timestamp = new Date().toISOString()
  args[0] = util.format('[%s] %s', timestamp, args[0])
  process.stderr.write(util.format.apply(null, args) + '\n')
}

module.exports = {
  info: logger,
  error: logger,
  trace: Function.prototype
}

