const { v4: uuidv4 } = require('uuid');

const Status = {
  PENDING:    'pending',
  PROCESSING: 'processing',
  COMPLETED:  'completed',
  FAILED:     'failed',
  DEAD:       'dead',
};

class Job {
  constructor({ id, type, payload, priority = 0, retries = 0, maxRetries = 3, createdAt } = {}) {
    this.id         = id || uuidv4();
    this.type       = type;
    this.payload    = payload;
    this.priority   = priority;     // higher number = more urgent
    this.retries    = retries;
    this.maxRetries = maxRetries;
    this.status     = Status.PENDING;
    this.createdAt  = createdAt || Date.now();
  }

  serialize() {
    return JSON.stringify(this);
  }

  static deserialize(str) {
    const data = JSON.parse(str);
    const job  = new Job(data);
    job.status = data.status;       // restore persisted status
    return job;
  }
}

module.exports = { Job, Status };
