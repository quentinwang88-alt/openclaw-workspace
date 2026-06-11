const adapter = require('./adapter');

module.exports = {
  canHandle: adapter.canHandle,
  processIminiTask: adapter.processIminiTask
};