const Redis  = require('ioredis');
const { Job, Status } = require('./Job');

// Redis key constants
const KEYS = {
  queue:   'jq:queue',     // sorted set  → score = -priority (low score pops first)
  delayed: 'jq:delayed',   // sorted set  → score = run-at timestamp
  dlq:     'jq:dlq',       // list        → dead jobs
  meta:    (id) => `jq:job:${id}`, // hash → { status }
};

class Queue {
  constructor(redisConfig = {}) {
    this.redis = new Redis({ host: '127.0.0.1', port: 6379, ...redisConfig });
    this.redis.on('error', (err) => console.error('[Redis]', err.message));
  }

  // ─── Producer ──────────────────────────────────────────────────────────────

  async push(job) {
    // Negate priority so that ZPOPMIN returns the highest-priority job first
    const score = -job.priority;
    await this.redis.zadd(KEYS.queue, score, job.serialize());
    await this.redis.hset(KEYS.meta(job.id), 'status', Status.PENDING);
    console.log(`[Queue] ➕ pushed  ${job.id}  type=${job.type}  priority=${job.priority}`);
    return job;
  }

  // ─── Consumer ──────────────────────────────────────────────────────────────

  // Atomically pop the highest-priority job (lowest score)
  async pop() {
    const result = await this.redis.zpopmin(KEYS.queue, 1);
    if (!result || result.length === 0) return null;
    return Job.deserialize(result[0]); // [member, score, ...]
  }

  // ─── Status helpers ────────────────────────────────────────────────────────

  async setStatus(job, status) {
    job.status = status;
    await this.redis.hset(KEYS.meta(job.id), 'status', status);
  }

  async getStatus(jobId) {
    return this.redis.hget(KEYS.meta(jobId), 'status');
  }

  // ─── Delayed retry ─────────────────────────────────────────────────────────

  // Put the job into the delayed set; score = Unix ms when it should run
  async requeueWithDelay(job, delayMs) {
    const runAt = Date.now() + delayMs;
    await this.redis.zadd(KEYS.delayed, runAt, job.serialize());
  }

  // Move any matured delayed jobs back to the main queue (call on an interval)
  async promoteDelayed() {
    const now  = Date.now();
    const jobs = await this.redis.zrangebyscore(KEYS.delayed, '-inf', now);
    for (const raw of jobs) {
      await this.redis.zrem(KEYS.delayed, raw);
      const job  = Job.deserialize(raw);
      job.status = Status.PENDING;
      await this.push(job);
    }
  }

  // ─── Dead-letter queue ─────────────────────────────────────────────────────

  async sendToDLQ(job) {
    await this.setStatus(job, Status.DEAD);
    await this.redis.lpush(KEYS.dlq, job.serialize());
    console.log(`[DLQ]   💀 buried  ${job.id}  after ${job.retries} retries`);
  }

  async getDLQJobs() {
    const items = await this.redis.lrange(KEYS.dlq, 0, -1);
    return items.map(Job.deserialize);
  }

  // ─── Stats ─────────────────────────────────────────────────────────────────

  async stats() {
    const [queued, delayed, dead] = await Promise.all([
      this.redis.zcard(KEYS.queue),
      this.redis.zcard(KEYS.delayed),
      this.redis.llen(KEYS.dlq),
    ]);
    return { queued, delayed, dead };
  }

  async close() {
    await this.redis.quit();
  }
}

module.exports = Queue;
