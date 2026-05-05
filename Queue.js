const Redis  = require('ioredis');
const { Job, Status } = require('./Job');

// Redis key constants
const KEYS = {
  queue:      'jq:queue',       // sorted set → priority queue
  delayed:    'jq:delayed',     // sorted set → delayed retries
  processing: 'jq:processing',  // sorted set → in-flight jobs (visibility timeout)
  dlq:        'jq:dlq',         // list → dead jobs
  meta:       (id) => `jq:job:${id}`,
};

class Queue {
  constructor(redisConfig = {}) {
    this.redis = new Redis({ host: '127.0.0.1', port: 6379, ...redisConfig });
    this.redis.on('error', (err) => console.error('[Redis]', err.message));
  }

  // ─── Producer ──────────────────────────────────────────────────────────────

  async push(job) {
    const score = -job.priority;
    await this.redis.zadd(KEYS.queue, score, job.serialize());
    await this.redis.hset(KEYS.meta(job.id), 'status', Status.PENDING);
    console.log(`[Queue] ➕ pushed ${job.id} type=${job.type} priority=${job.priority}`);
    return job;
  }

  // ─── SAFE POP WITH VISIBILITY TIMEOUT ──────────────────────────────────────

  async popForProcessing(visibilityTimeoutMs = 5000) {
    const result = await this.redis.zpopmin(KEYS.queue, 1);
    if (!result || result.length === 0) return null;

    const job = Job.deserialize(result[0]);

    const timeoutAt = Date.now() + visibilityTimeoutMs;

    // Move to processing set
    await this.redis.zadd(KEYS.processing, timeoutAt, job.serialize());

    return job;
  }

  // ─── ACK (remove from processing after success/failure) ─────────────────────

  async ack(job) {
    await this.redis.zrem(KEYS.processing, job.serialize());
  }

  // ─── STATUS ────────────────────────────────────────────────────────────────

  async setStatus(job, status) {
    job.status = status;
    await this.redis.hset(KEYS.meta(job.id), 'status', status);
  }

  async getStatus(jobId) {
    return this.redis.hget(KEYS.meta(jobId), 'status');
  }

  // ─── DELAYED RETRY ─────────────────────────────────────────────────────────

  async requeueWithDelay(job, delayMs) {
    const runAt = Date.now() + delayMs;
    await this.redis.zadd(KEYS.delayed, runAt, job.serialize());
  }

  async promoteDelayed() {
    const now  = Date.now();
    const jobs = await this.redis.zrangebyscore(KEYS.delayed, '-inf', now);

    for (const raw of jobs) {
      await this.redis.zrem(KEYS.delayed, raw);
      const job = Job.deserialize(raw);
      job.status = Status.PENDING;
      await this.push(job);
    }
  }

  // ─── VISIBILITY TIMEOUT RECOVERY ───────────────────────────────────────────

  async reclaimStuckJobs() {
    const now = Date.now();

    const jobs = await this.redis.zrangebyscore(KEYS.processing, '-inf', now);

    for (const raw of jobs) {
      await this.redis.zrem(KEYS.processing, raw);

      const job = Job.deserialize(raw);
      console.log(`[Recovery] ♻️ reclaim ${job.id}`);

      job.status = Status.PENDING;
      await this.push(job);
    }
  }

  // ─── DLQ ───────────────────────────────────────────────────────────────────

  async sendToDLQ(job) {
    await this.setStatus(job, Status.DEAD);
    await this.redis.lpush(KEYS.dlq, job.serialize());
    console.log(`[DLQ] 💀 buried ${job.id} after ${job.retries} retries`);
  }

  async getDLQJobs() {
    const items = await this.redis.lrange(KEYS.dlq, 0, -1);
    return items.map(Job.deserialize);
  }

  // ─── STATS ─────────────────────────────────────────────────────────────────

  async stats() {
    const [queued, delayed, processing, dead] = await Promise.all([
      this.redis.zcard(KEYS.queue),
      this.redis.zcard(KEYS.delayed),
      this.redis.zcard(KEYS.processing),
      this.redis.llen(KEYS.dlq),
    ]);

    return { queued, delayed, processing, dead };
  }

  async close() {
    await this.redis.quit();
  }
}

module.exports = Queue;