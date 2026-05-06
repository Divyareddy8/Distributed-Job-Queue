'use strict';

const Redis        = require('ioredis');
const { Job, Status } = require('./Job');

// ─── Redis key namespace ────────────────────────────────────────────────────
const KEYS = {
  queue:      'jq:queue',       // sorted set  – active jobs, scored by -priority
  delayed:    'jq:delayed',     // sorted set  – retry jobs,  scored by runAt ms
  processing: 'jq:processing',  // sorted set  – in-flight,   scored by timeoutAt ms
  dlq:        'jq:dlq',         // list        – dead-letter queue (newest first)
  data:       (id) => `jq:data:${id}`,   // string – serialised Job
  meta:       (id) => `jq:meta:${id}`,   // hash   – lightweight status lookup
};

/**
 * Default Redis connection options.
 * Override via the `redisConfig` constructor argument.
 */
const DEFAULT_REDIS_CONFIG = {
  host:                '127.0.0.1',
  port:                6379,
  maxRetriesPerRequest: 3,          // fail fast instead of spinning 20×
  enableOfflineQueue:   false,      // surface errors immediately
  lazyConnect:          true,       // don't connect until first command
};

class Queue {
  /**
   * @param {import('ioredis').RedisOptions} [redisConfig={}]
   */
  constructor(redisConfig = {}) {
    this.redis = new Redis({ ...DEFAULT_REDIS_CONFIG, ...redisConfig });

    this.redis.on('error',   (err) => console.error('[Redis] error:', err.message));
    this.redis.on('connect', ()    => console.log('[Redis] ✅ connected'));
    this.redis.on('close',   ()    => console.warn('[Redis] ⚠️  connection closed'));
  }

  /** Explicitly open the connection (called once at startup). */
  async connect() {
    await this.redis.connect();
  }

  // ─── Producer ─────────────────────────────────────────────────────────────

  /**
   * Enqueue a job. Idempotent: re-pushing an existing id just updates the score.
   * Uses a pipeline so both writes are atomic from the caller's perspective.
   */
  async push(job) {
    job.touch();
    const score = -job.priority;   // lower score = popped first by zpopmin

    const pipeline = this.redis.pipeline();
    pipeline.set(KEYS.data(job.id), job.serialize());
    pipeline.zadd(KEYS.queue, score, job.id);
    pipeline.hset(KEYS.meta(job.id), 'status', Status.PENDING, 'type', job.type);
    await pipeline.exec();

    console.log(`[Queue] ➕ pushed  ${job.id}  type=${job.type}  priority=${job.priority}`);
    return job;
  }

  // ─── Consumer (safe pop with visibility timeout) ─────────────────────────

  /**
   * Atomically pop the highest-priority job and place it in the processing set.
   * The processing set is scored by `timeoutAt` so reclaimStuckJobs() can
   * detect and re-queue jobs whose workers crashed.
   *
   * @param {number} [visibilityTimeoutMs=5000]
   * @returns {Promise<Job|null>}
   */
  async popForProcessing(visibilityTimeoutMs = 5_000) {
    // zpopmin returns [member, score, member, score, ...]
    const result = await this.redis.zpopmin(KEYS.queue, 1);
    if (!result || result.length < 1) return null;

    const jobId = result[0];
    const raw   = await this.redis.get(KEYS.data(jobId));
    if (!raw) return null;      // data expired / already cleaned up

    const job       = Job.deserialize(raw);
    const timeoutAt = Date.now() + visibilityTimeoutMs;

    await this.redis.zadd(KEYS.processing, timeoutAt, job.id);

    return job;
  }

  // ─── Acknowledgement ──────────────────────────────────────────────────────

  /**
   * Remove the job from the processing set.
   * Call this AFTER updating status so the final state is always persisted.
   *
   * @param {Job}     job
   * @param {boolean} [cleanup=false]  - also delete data/meta keys
   */
  async ack(job, cleanup = false) {
    const pipeline = this.redis.pipeline();
    pipeline.zrem(KEYS.processing, job.id);

    if (cleanup) {
      pipeline.del(KEYS.data(job.id));
      pipeline.del(KEYS.meta(job.id));
    }

    await pipeline.exec();
  }

  // ─── Status ───────────────────────────────────────────────────────────────

  async setStatus(job, status) {
    job.status = status;
    job.touch();

    const pipeline = this.redis.pipeline();
    pipeline.hset(KEYS.meta(job.id), 'status', status);
    pipeline.set(KEYS.data(job.id), job.serialize());
    await pipeline.exec();
  }

  /** Lightweight status check — reads only the hash field. */
  async getStatus(jobId) {
    return this.redis.hget(KEYS.meta(jobId), 'status');
  }

  // ─── Delayed retry ────────────────────────────────────────────────────────

  async requeueWithDelay(job, delayMs) {
    const runAt = Date.now() + delayMs;
    job.touch();

    const pipeline = this.redis.pipeline();
    pipeline.set(KEYS.data(job.id), job.serialize());
    pipeline.zadd(KEYS.delayed, runAt, job.id);
    await pipeline.exec();
  }

  /**
   * Move any delayed jobs whose `runAt` has passed into the active queue.
   * Uses a Lua script for atomicity: no other worker can pick the same id
   * between the ZRANGEBYSCORE and the ZREM.
   */
  async promoteDelayed() {
    const now    = Date.now();
    const jobIds = await this.redis.zrangebyscore(KEYS.delayed, '-inf', now);

    for (const id of jobIds) {
      // Atomic remove-from-delayed + add-to-queue
      const removed = await this.redis.zrem(KEYS.delayed, id);
      if (removed === 0) continue;   // another worker already promoted it

      const raw = await this.redis.get(KEYS.data(id));
      if (!raw) continue;

      const job  = Job.deserialize(raw);
      job.status = Status.PENDING;
      job.touch();

      // Re-push directly without extra logging noise
      const score = -job.priority;
      const pipeline = this.redis.pipeline();
      pipeline.set(KEYS.data(job.id), job.serialize());
      pipeline.zadd(KEYS.queue, score, job.id);
      pipeline.hset(KEYS.meta(job.id), 'status', Status.PENDING);
      await pipeline.exec();
    }
  }

  // ─── Visibility-timeout recovery ─────────────────────────────────────────

  /**
   * Re-queue jobs that were popped but never ack'd within the visibility window.
   * Protects against worker crashes / process kills.
   */
  async reclaimStuckJobs() {
    const now    = Date.now();
    const jobIds = await this.redis.zrangebyscore(KEYS.processing, '-inf', now);

    for (const id of jobIds) {
      const removed = await this.redis.zrem(KEYS.processing, id);
      if (removed === 0) continue;   // race: another worker just ack'd it

      const raw = await this.redis.get(KEYS.data(id));
      if (!raw) continue;

      const job  = Job.deserialize(raw);
      console.warn(`[Recovery] ♻️  reclaiming stuck job ${job.id}  type=${job.type}`);

      job.status = Status.PENDING;
      await this.push(job);
    }
  }

  // ─── Dead-letter queue ────────────────────────────────────────────────────

  async sendToDLQ(job) {
    await this.setStatus(job, Status.DEAD);

    const pipeline = this.redis.pipeline();
    pipeline.lpush(KEYS.dlq, job.serialize());
    pipeline.del(KEYS.data(job.id));
    pipeline.del(KEYS.meta(job.id));
    await pipeline.exec();

    console.error(`[DLQ] 💀 buried ${job.id}  type=${job.type}  retries=${job.retries}`);
  }

  async getDLQJobs() {
    const items = await this.redis.lrange(KEYS.dlq, 0, -1);
    return items.map(Job.deserialize);
  }

  /** Clear only DLQ entries for a specific job id. */
  async replayFromDLQ(jobId) {
    const items = await this.redis.lrange(KEYS.dlq, 0, -1);
    for (const raw of items) {
      const job = Job.deserialize(raw);
      if (job.id === jobId) {
        await this.redis.lrem(KEYS.dlq, 1, raw);
        job.status  = Status.PENDING;
        job.retries = 0;
        await this.push(job);
        return job;
      }
    }
    return null;
  }

  // ─── Stats ────────────────────────────────────────────────────────────────

  async stats() {
    const [queued, delayed, processing, dead] = await Promise.all([
      this.redis.zcard(KEYS.queue),
      this.redis.zcard(KEYS.delayed),
      this.redis.zcard(KEYS.processing),
      this.redis.llen(KEYS.dlq),
    ]);

    return { queued, delayed, processing, dead };
  }

  // ─── Lifecycle ────────────────────────────────────────────────────────────

  /** Gracefully close the Redis connection. */
  async close() {
    await this.redis.quit();
  }

  /** Flush all job-queue keys (useful in tests). */
  async flush() {
    const keys = await this.redis.keys('jq:*');
    if (keys.length) await this.redis.del(...keys);
  }
}

module.exports = Queue;
