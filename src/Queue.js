const Redis  = require('ioredis');
const { Job, Status } = require('./Job');

// Redis key constants
const KEYS = {
  queue:      'jq:queue',
  delayed:    'jq:delayed',
  processing: 'jq:processing',
  dlq:        'jq:dlq',
  meta:       (id) => `jq:job:${id}`,
  data:       (id) => `jq:data:${id}`, // 🔥 NEW → store full job safely
};

class Queue {
  constructor(redisConfig = {}) {
    this.redis = new Redis({ host: '127.0.0.1', port: 6379, ...redisConfig });
    this.redis.on('error', (err) => console.error('[Redis]', err.message));
  }

  // ─── Producer ──────────────────────────────────────────────────────────────

  async push(job) {
    const score = -job.priority;

    // Store full job data separately
    await this.redis.set(KEYS.data(job.id), job.serialize());

    // Push only job ID into queue
    await this.redis.zadd(KEYS.queue, score, job.id);

    await this.redis.hset(KEYS.meta(job.id), 'status', Status.PENDING);

    console.log(`[Queue] ➕ pushed ${job.id} type=${job.type} priority=${job.priority}`);
    return job;
  }

  // ─── SAFE POP WITH VISIBILITY TIMEOUT ──────────────────────────────────────

  async popForProcessing(visibilityTimeoutMs = 5000) {
    const result = await this.redis.zpopmin(KEYS.queue, 1);
    if (!result || result.length === 0) return null;

    const jobId = result[0];

    const raw = await this.redis.get(KEYS.data(jobId));
    if (!raw) return null;

    const job = Job.deserialize(raw);

    const timeoutAt = Date.now() + visibilityTimeoutMs;

    // Move ONLY jobId into processing set
    await this.redis.zadd(KEYS.processing, timeoutAt, job.id);

    return job;
  }

  // ─── ACK (remove from processing after success/failure) ─────────────────────

  async ack(job) {
    await this.redis.zrem(KEYS.processing, job.id);

    // Optional: cleanup data after completion
    if (job.status === Status.COMPLETED || job.status === Status.DEAD) {
      await this.redis.del(KEYS.data(job.id));
    }
  }

  // ─── STATUS ────────────────────────────────────────────────────────────────

  async setStatus(job, status) {
    job.status = status;
    await this.redis.hset(KEYS.meta(job.id), 'status', status);

    // Keep updated job state in Redis
    await this.redis.set(KEYS.data(job.id), job.serialize());
  }

  async getStatus(jobId) {
    return this.redis.hget(KEYS.meta(jobId), 'status');
  }

  // ─── DELAYED RETRY ─────────────────────────────────────────────────────────

  async requeueWithDelay(job, delayMs) {
    const runAt = Date.now() + delayMs;

    await this.redis.set(KEYS.data(job.id), job.serialize());
    await this.redis.zadd(KEYS.delayed, runAt, job.id);
  }

  async promoteDelayed() {
    const now = Date.now();

    const jobIds = await this.redis.zrangebyscore(KEYS.delayed, '-inf', now);

    for (const id of jobIds) {
      await this.redis.zrem(KEYS.delayed, id);

      const raw = await this.redis.get(KEYS.data(id));
      if (!raw) continue;

      const job = Job.deserialize(raw);
      job.status = Status.PENDING;

      await this.push(job);
    }
  }

  // ─── VISIBILITY TIMEOUT RECOVERY ───────────────────────────────────────────

  async reclaimStuckJobs() {
    const now = Date.now();

    const jobIds = await this.redis.zrangebyscore(KEYS.processing, '-inf', now);

    for (const id of jobIds) {
      await this.redis.zrem(KEYS.processing, id);

      const raw = await this.redis.get(KEYS.data(id));
      if (!raw) continue;

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

    await this.redis.del(KEYS.data(job.id));
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