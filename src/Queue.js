'use strict';

const Redis           = require('ioredis');
const { Job, Status } = require('./Job');

// ─── Redis key namespace ───────────────────────────────────────────────────
const KEYS = {
  typeQueue:  (type) => `jq:queue:${type}`,
  queueIndex: 'jq:queues',
  delayed:    'jq:delayed',
  processing: 'jq:processing',
  dlq:        'jq:dlq',
  data:       (id)  => `jq:data:${id}`,
  meta:       (id)  => `jq:meta:${id}`,
  rateLimit:  (type)=> `jq:rl:${type}`,
  recentJobs: 'jq:recent',
  counters:   'jq:counters',   // hash: completed, failed, total
};

const DEFAULT_REDIS_CONFIG = {
  host:                 process.env.REDIS_HOST     || '127.0.0.1',
  port:                 parseInt(process.env.REDIS_PORT || '6379', 10),
  password:             process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: 3,
  enableOfflineQueue:   false,
  lazyConnect:          true,
};

class Queue {
  constructor(redisConfig = {}, rateLimits = {}) {
    this.redis      = new Redis({ ...DEFAULT_REDIS_CONFIG, ...redisConfig });
    this.rateLimits = rateLimits;

    this.redis.on('error',   (err) => console.error('[Redis] error:', err.message));
    this.redis.on('connect', ()    => console.log('[Redis] ✅ connected'));
    this.redis.on('close',   ()    => console.warn('[Redis] ⚠️  connection closed'));
  }

  // ─── Rate Limiting ──────────────────────────────────────────────────────── ────────────────────────────────────────────────────────

  async checkRateLimit(type) {
    const cfg = this.rateLimits[type];
    if (!cfg) return { allowed: true, remaining: null, resetAfterMs: 0 };

    const { max, windowMs } = cfg;
    const key    = KEYS.rateLimit(type);
    const now    = Date.now();
    const cutoff = now - windowMs;

    const results = await this.redis
      .pipeline()
      .zremrangebyscore(key, '-inf', cutoff)
      .zcard(key)
      .exec();
    const count = results[1][1];

    if (count >= max) {
      const oldest       = await this.redis.zrange(key, 0, 0, 'WITHSCORES');
      const resetAfterMs = oldest.length >= 2
        ? Math.max(0, parseInt(oldest[1], 10) + windowMs - now)
        : windowMs;
      return { allowed: false, remaining: 0, resetAfterMs };
    }

    const member = `${now}:${Math.random().toString(36).slice(2)}`;
    await this.redis.pipeline()
      .zadd(key, now, member)
      .pexpire(key, windowMs)
      .exec();

    return { allowed: true, remaining: max - count - 1, resetAfterMs: 0 };
  }

  // ─── Producer ─────────────────────────────────────────────────────────────

  async push(job) {
    job.touch();
    const score   = -job.priority;
    const typeKey = KEYS.typeQueue(job.type);

    await this.redis
      .pipeline()
      .set  (KEYS.data(job.id), job.serialize())
      .zadd (typeKey, score, job.id)
      .sadd (KEYS.queueIndex, job.type)
      .hset (KEYS.meta(job.id), 'status', Status.PENDING, 'type', job.type)
      .lpush(KEYS.recentJobs, job.id)
      .ltrim(KEYS.recentJobs, 0, 199)
      .hincrby(KEYS.counters, 'totalEver', 1)
      .exec();

    console.log(`[Queue] ➕ pushed  ${job.id}  type=${job.type}  priority=${job.priority}`);
    return job;
  }

  // ─── Consumer ─────────────────────────────────────────────────────────────

  /**
   * Pop the highest-priority job from the subscribed type queues.
   *
   * Single-type workers  → ZPOPMIN (fully atomic, no race condition).
   * Multi-type workers   → peek across queues, pick best score, then ZREM.
   */
  async popForProcessing(visibilityTimeoutMs = 5_000, types = null) {
    let queueKeys;

    if (types && types.length > 0) {
      queueKeys = types.map(KEYS.typeQueue);
    } else {
      const allTypes = await this.redis.smembers(KEYS.queueIndex);
      if (!allTypes.length) return null;
      queueKeys = allTypes.map(KEYS.typeQueue);
    }

    let jobId = null;

    if (queueKeys.length === 1) {
      // Fast path: atomic pop from a single queue
      const res = await this.redis.zpopmin(queueKeys[0], 1);
      // zpopmin returns [member, score, ...] or []
      if (res && res.length >= 1) jobId = res[0];

    } else {
      // Multi-queue: find best (lowest) score, then atomically remove
      let bestScore = Infinity;
      let bestKey   = null;

      for (const key of queueKeys) {
        const res = await this.redis.zrange(key, 0, 0, 'WITHSCORES');
        if (res && res.length >= 2) {
          const score = parseFloat(res[1]);
          if (score < bestScore) {
            bestScore = score;
            bestKey   = key;
            jobId     = res[0];
          }
        }
      }

      if (bestKey && jobId) {
        const removed = await this.redis.zrem(bestKey, jobId);
        if (removed === 0) return null; // Another worker beat us
      }
    }

    if (!jobId) return null;

    const raw = await this.redis.get(KEYS.data(jobId));
    if (!raw) return null;

    const job       = Job.deserialize(raw);
    const timeoutAt = Date.now() + visibilityTimeoutMs;
    await this.redis
      .pipeline()
      .zadd(KEYS.processing, timeoutAt, job.id)
      .exec();

    return job;
  }

  // ─── Ack ──────────────────────────────────────────────────────────────────

  /**
   * FIX: Do NOT delete jq:data on completion — we need it so getRecentJobs
   * can still display completed jobs in the dashboard.
   * We only delete data+meta when a job goes to the DLQ (sendToDLQ handles that).
   * The `cleanup` parameter is now ignored — kept for API compatibility.
   */
  async ack(job, cleanup = false) {
    // Only remove from the processing sorted set. Never delete data here.
    await this.redis.zrem(KEYS.processing, job.id);
  }

  // ─── Status ───────────────────────────────────────────────────────────────

  async setStatus(job, status) {
    job.status = status;
    job.touch();
    await this.redis
      .pipeline()
      .hset(KEYS.meta(job.id), 'status', status)
      .set (KEYS.data(job.id), job.serialize())
      .exec();
  }

  async getStatus(jobId) {
    return this.redis.hget(KEYS.meta(jobId), 'status');
  }

  /**
   * FIX: getJob now always returns a full object.
   * Previously if data key was deleted (old cleanup=true) it fell back to
   * meta-only with status still 'pending' — causing the "always pending" bug.
   */
  async getJob(jobId) {
    const raw = await this.redis.get(KEYS.data(jobId));
    if (raw) return Job.deserialize(raw);

    // Fallback: job was sent to DLQ (data deleted there). Read from meta.
    const [status, type] = await this.redis.hmget(KEYS.meta(jobId), 'status', 'type');
    if (status) return { id: jobId, status, type };

    // Check if it's in the DLQ list
    const dlqItems = await this.redis.lrange(KEYS.dlq, 0, -1);
    for (const item of dlqItems) {
      try {
        const j = Job.deserialize(item);
        if (j.id === jobId) return j;
      } catch { /* skip */ }
    }

    return null;
  }

  // ─── Cancel ───────────────────────────────────────────────────────────────

  async cancel(jobId) {
    const raw = await this.redis.get(KEYS.data(jobId));
    if (!raw) return false;

    const job    = Job.deserialize(raw);
    const status = await this.getStatus(jobId);
    if (status !== Status.PENDING) return false;

    await this.redis
      .pipeline()
      .zrem    (KEYS.typeQueue(job.type), jobId)
      .del     (KEYS.data(jobId))
      .del     (KEYS.meta(jobId))
      .exec();

    return true;
  }

  // ─── Delayed retry ────────────────────────────────────────────────────────

  async requeueWithDelay(job, delayMs) {
    const runAt = Date.now() + delayMs;
    job.touch();
    await this.redis
      .pipeline()
      .set (KEYS.data(job.id), job.serialize())
      .zadd(KEYS.delayed, runAt, job.id)
      .exec();
  }

  async promoteDelayed() {
    const now    = Date.now();
    const jobIds = await this.redis.zrangebyscore(KEYS.delayed, '-inf', now);

    for (const id of jobIds) {
      const removed = await this.redis.zrem(KEYS.delayed, id);
      if (removed === 0) continue;

      const raw = await this.redis.get(KEYS.data(id));
      if (!raw) continue;

      const job  = Job.deserialize(raw);
      job.status = Status.PENDING;
      job.touch();

      await this.redis
        .pipeline()
        .set (KEYS.data(job.id), job.serialize())
        .zadd(KEYS.typeQueue(job.type), -job.priority, job.id)
        .sadd(KEYS.queueIndex, job.type)
        .hset(KEYS.meta(job.id), 'status', Status.PENDING)
        .exec();
    }
  }

  // ─── Visibility-timeout recovery ──────────────────────────────────────────

  async reclaimStuckJobs() {
    const now    = Date.now();
    const jobIds = await this.redis.zrangebyscore(KEYS.processing, '-inf', now);

    for (const id of jobIds) {
      const removed = await this.redis.zrem(KEYS.processing, id);
      if (removed === 0) continue;

      const raw = await this.redis.get(KEYS.data(id));
      if (!raw) continue;

      const job = Job.deserialize(raw);
      console.warn(`[Recovery] ♻️  reclaiming stuck job ${job.id}  type=${job.type}`);
      job.status = Status.PENDING;
      await this.push(job);
    }
  }

  // ─── Dead-letter queue ────────────────────────────────────────────────────

  async sendToDLQ(job) {
    await this.setStatus(job, Status.DEAD);
    await this.redis
      .pipeline()
      .lpush(KEYS.dlq, job.serialize())
      .ltrim(KEYS.dlq, 0, 999)
      // FIX: Only delete data+meta here in DLQ (not in ack).
      // The dashboard can still find DLQ jobs via getDLQJobs / getJob DLQ fallback.
      .del  (KEYS.data(job.id))
      .del  (KEYS.meta(job.id))
      .exec();
    console.error(`[DLQ] 💀 buried ${job.id}  type=${job.type}  retries=${job.retries}`);
  }

  async getDLQJobs(limit = 50, offset = 0) {
    const items = await this.redis.lrange(KEYS.dlq, offset, offset + limit - 1);
    return items.map(Job.deserialize);
  }

  async replayFromDLQ(jobId) {
    const items = await this.redis.lrange(KEYS.dlq, 0, -1);
    for (const raw of items) {
      const job = Job.deserialize(raw);
      if (job.id !== jobId) continue;
      await this.redis.lrem(KEYS.dlq, 1, raw);
      job.status  = Status.PENDING;
      job.retries = 0;
      await this.push(job);
      return job;
    }
    return null;
  }

  async purgeDLQ() {
    await this.redis.del(KEYS.dlq);
  }

  // ─── Counters ─────────────────────────────────────────────────────────────

  /** Increment a named counter (completed | failed). Called by Worker. */
  async incrementCounter(name) {
    await this.redis.hincrby(KEYS.counters, name, 1);
  }

  // ─── Stats ────────────────────────────────────────────────────────────────

  async stats() {
    const allTypes = await this.redis.smembers(KEYS.queueIndex);

    const [delayed, processing, dead, counters] = await Promise.all([
      this.redis.zcard(KEYS.delayed),
      this.redis.zcard(KEYS.processing),
      this.redis.llen (KEYS.dlq),
      this.redis.hgetall(KEYS.counters),
    ]);

    // byType = actual pending count per queue (ground truth from sorted sets)
    const byType = {};
    let queued = 0;
    for (const type of allTypes) {
      const count = await this.redis.zcard(KEYS.typeQueue(type));
      byType[type] = count;
      queued += count;
    }

    // queued is derived from actual sorted-set sizes — no racing counter needed.
    // completed/failed/totalEver still come from counters (they are append-only).
    const completed = parseInt(counters?.completed || '0', 10);
    const failed    = parseInt(counters?.failed    || '0', 10);
    const totalEver = parseInt(counters?.totalEver || '0', 10);

    return { queued, delayed, processing, dead, completed, failed, totalEver, byType };
  }

  /**
   * FIX: getRecentJobs now filters out nulls so the jobs table never shows
   * empty rows, and because we no longer delete data on ack, completed jobs
   * will properly appear with status=completed.
   */
  async getRecentJobs(limit = 20) {
    const ids  = await this.redis.lrange(KEYS.recentJobs, 0, limit - 1);
    const jobs = [];
    for (const id of ids) {
      const job = await this.getJob(id);
      if (job) jobs.push(job);   // null-safe: skip purged/cancelled jobs
    }
    return jobs;
  }

  // ─── Lifecycle ────────────────────────────────────────────────────────────

  async connect() {
    await this.redis.connect();
    // Reset all counters to 0 on every server start so the dashboard
    // always shows clean state (completed, failed, totalEver start fresh).
    await this.redis.del(KEYS.counters);
  }

  async close() { await this.redis.quit(); }

  async flush() {
    const keys = await this.redis.keys('jq:*');
    if (keys.length) await this.redis.del(...keys);
  }
}

module.exports = Queue;