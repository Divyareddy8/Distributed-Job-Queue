'use strict';

const { v4: uuidv4 } = require('uuid');

/**
 * Enum-style status constants — frozen so no accidental mutation.
 */
const Status = Object.freeze({
  PENDING:    'pending',
  PROCESSING: 'processing',
  COMPLETED:  'completed',
  FAILED:     'failed',
  DEAD:       'dead',
});

class Job {
  /**
   * @param {object} opts
   * @param {string}  [opts.id]          - UUID (auto-generated if omitted)
   * @param {string}   opts.type         - Handler key (e.g. 'sendEmail')
   * @param {object}   opts.payload      - Arbitrary data passed to the handler
   * @param {number}  [opts.priority=0]  - Higher = processed first
   * @param {number}  [opts.retries=0]   - Current retry count
   * @param {number}  [opts.maxRetries=3]
   * @param {number}  [opts.createdAt]   - Unix ms timestamp
   * @param {string}  [opts.status]      - Restored from persistence
   */
  constructor({
    id,
    type,
    payload,
    priority   = 0,
    retries    = 0,
    maxRetries = 3,
    createdAt,
    status,
  } = {}) {
    if (!type)    throw new TypeError('Job requires a `type`');
    if (!payload) throw new TypeError('Job requires a `payload`');

    this.id         = id || uuidv4();
    this.type       = type;
    this.payload    = payload;
    this.priority   = priority;
    this.retries    = retries;
    this.maxRetries = maxRetries;
    this.status     = status || Status.PENDING;
    this.createdAt  = createdAt || Date.now();
    this.updatedAt  = Date.now();
  }

  /** Mark the job's updatedAt timestamp before persisting. */
  touch() {
    this.updatedAt = Date.now();
    return this;
  }

  serialize() {
    return JSON.stringify(this);
  }

  static deserialize(str) {
    const data = JSON.parse(str);
    return new Job(data);   // constructor now restores `status` directly
  }
}

module.exports = { Job, Status };
