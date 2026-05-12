'use strict';

const Redis       = require('ioredis');
const { Job, Status } = require('./Job');

// ─── Redis key namespace ───────────────────────────────────────────────────
const KEYS = {
  typeQueue:  (type) => `jq:queue:${type}`,   // sorted set per type, score = -priority
  queueIndex: 'jq:queues',                     // set  – registered type names
  delayed:    'jq:delayed',                    // sorted set – retry jobs, score = runAt ms
  processing: 'jq:processing',                 // sorted set – in-flight, score = timeoutAt ms
  dlq:        'jq:dlq',                        // list – dead-letter (newest first)
  data:       (id) => `jq:data:${id}`,         // string – serialised Job
  meta:       (id) => `jq:meta:${id}`,         // hash – status / type
  rateLimit:  (type) => `jq:rl:${type}`,       // sorted set – sliding-window rate limit
  recentJobs: 'jq:recent',                     // list – last 200 pushed job IDs
};

// ─── Lua: atomically find & pop the best job across N type queues ──────────
// All queues are scored by -priority so the lowest score = highest priority.
const POP_ACROSS_TYPES_LUA = `
local best_score  = false
local best_member = nil
local best_key    = nil

for i = 1, #KEYS do
  local r = redis.call('ZRANGE', KEYS[i], 0, 0, 'WITHSCORES')
  if #r >= 2 then
    local s = tonumber(r[2])
    if best_score == false or s < best_score then
      best_score  = s
      best_member = r[1]
      best_key    = KEYS[i]
    end
  end
end

if best_member then
  redis.call('ZREM', best_key, best_member)
  return { best_key, best_member }
end
return nil
`;

const DEFAULT_REDIS_CONFIG = {
  host:                 process.env.REDIS_HOST     || '127.0.0.1',
  port:                 parseInt(process.env.REDIS_PORT || '6379', 10),
  password:             process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: 3,
  enableOfflineQueue:   false,
  lazyConnect:          true,
};

class Queue {
  /**
   * @param {import('ioredis').RedisOptions} [redisConfig={}]
   * @param {Record<string, {max: number, windowMs: number}>} [rateLimits={}]
   *   e.g. { sendEmail: { max: 20, windowMs: 60_000 } }
   */
  constructor(redisConfig = {}, rateLimits = {}) {
    this.redis      = new Redis({ ...DEFAULT_REDIS_CONFIG, ...redisConfig });
    this.rateLimits = rateLimits;

    this.redis.defineCommand('popAcrossTypes', {
      numberOfKeys: 0,   // key count is passed dynamically
      lua:          POP_ACROSS_TYPES_LUA,
    });

    this.redis.on('error',   (err) => console.error('[Redis] error:', err.message));
    this.redis.on('connect', ()    => console.log ('[Redis] ✅ connected'));
    this.redis.on('close',   ()    => console.warn ('[Redis] ⚠️  connection closed'));
  }

  async connect() { await this.redis.connect(); }

  // ─── Rate Limiting (sliding window) ───────────────────────────────────────

  /**
   * Checks and (if allowed) records one token for the given type.
   * @returns {{ allowed: boolean, remaining: number|null, resetAfterMs: number }}
   */
  async checkRateLimit(type) {
    const cfg = this.rateLimits[type];
    if (!cfg) return { allowed: true, remaining: null, resetAfterMs: 0 };

    const { max, windowMs } = cfg;
    const key    = KEYS.rateLimit(type);
    const now    = Date.now();
    const cutoff = now - windowMs;

    // Remove expired entries then count what's left
    const results = await this.redis
      .pipeline()
      .zremrangebyscore(key, '-inf', cutoff)
      .zcard(key)
      .exec();
    const count = results[1][1];

    if (count >= max) {
      const oldest      = await this.redis.zrange(key, 0, 0, 'WITHSCORES');
      const resetAfterMs = oldest.length >= 2
        ? Math.max(0, parseInt(oldest[1], 10) + windowMs - now)
        : windowMs;
      return { allowed: false, remaining: 0, resetAfterMs };
    }

    // Record this request
    const member = `${now}:${Math.random().toString(36).slice(2)}`;
    await this.redis.pipeline().zadd(key, now, member).pexpire(key, windowMs).exec();

    return { allowed: true, remaining: max - count - 1, resetAfterMs: 0 };
  }

  // ─── Producer ─────────────────────────────────────────────────────────────

  /**
   * Enqueue a job.  Each job type lives in its own sorted set so workers
   * can subscribe to specific types without scanning unrelated jobs.
   */
  async push(job) {
    job.touch();
    const score   = -job.priority;
    const typeKey = KEYS.typeQueue(job.type);

    await this.redis
      .pipeline()
      .set   (KEYS.data(job.id), job.serialize())
      .zadd  (typeKey, score, job.id)
      .sadd  (KEYS.queueIndex, job.type)
      .hset  (KEYS.meta(job.id), 'status', Status.PENDING, 'type', job.type)
      .lpush (KEYS.recentJobs, job.id)
      .ltrim (KEYS.recentJobs, 0, 199)
      .exec();

    console.log(`[Queue] ➕ pushed  ${job.id}  type=${job.type}  priority=${job.priority}`);
    return job;
  }

  // ─── Consumer ─────────────────────────────────────────────────────────────

  /**
   * Atomically pop the highest-priority job from the specified type queues.
   * Pass `types = null` to pop across ALL registered types (global worker).
   *
   * @param {number}   [visibilityTimeoutMs=5000]
   * @param {string[]|null} [types=null]
   * @returns {Promise<Job|null>}
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

    // The Lua script expects: numkeys, key1, key2, …
    const result = await this.redis.popAcrossTypes(queueKeys.length, ...queueKeys);
    if (!result) return null;

    const jobId = result[1];
    const raw   = await this.redis.get(KEYS.data(jobId));
    if (!raw) return null;

    const job       = Job.deserialize(raw);
    const timeoutAt = Date.now() + visibilityTimeoutMs;
    await this.redis.zadd(KEYS.processing, timeoutAt, job.id);

    return job;
  }

  // ─── Ack ──────────────────────────────────────────────────────────────────

  async ack(job, cleanup = false) {
    const p = this.redis.pipeline().zrem(KEYS.processing, job.id);
    if (cleanup) p.del(KEYS.data(job.id)).del(KEYS.meta(job.id));
    await p.exec();
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

  /** Full job object, falling back to lightweight meta if data key is gone. */
  async getJob(jobId) {
    const raw = await this.redis.get(KEYS.data(jobId));
    if (raw) return Job.deserialize(raw);

    const [status, type] = await this.redis.hmget(KEYS.meta(jobId), 'status', 'type');
    return status ? { id: jobId, status, type } : null;
  }

  // ─── Cancel ───────────────────────────────────────────────────────────────

  /** Cancel a PENDING job.  Returns false if not found or not cancellable. */
  async cancel(jobId) {
    const raw = await this.redis.get(KEYS.data(jobId));
    if (!raw) return false;

    const job    = Job.deserialize(raw);
    const status = await this.getStatus(jobId);
    if (status !== Status.PENDING) return false;

    await this.redis
      .pipeline()
      .zrem(KEYS.typeQueue(job.type), jobId)
      .del (KEYS.data(jobId))
      .del (KEYS.meta(jobId))
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

      const job  = Job.deserialize(raw);
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
      .ltrim(KEYS.dlq, 0, 999)          // cap at 1 000 entries
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

  // ─── Stats ────────────────────────────────────────────────────────────────

  async stats() {
    const allTypes = await this.redis.smembers(KEYS.queueIndex);

    const [delayed, processing, dead] = await Promise.all([
      this.redis.zcard(KEYS.delayed),
      this.redis.zcard(KEYS.processing),
      this.redis.llen (KEYS.dlq),
    ]);

    const byType = {};
    for (const type of allTypes) {
      byType[type] = await this.redis.zcard(KEYS.typeQueue(type));
    }

    const queued = Object.values(byType).reduce((a, b) => a + b, 0);
    return { queued, delayed, processing, dead, byType };
  }

  async getRecentJobs(limit = 20) {
    const ids  = await this.redis.lrange(KEYS.recentJobs, 0, limit - 1);
    const jobs = [];
    for (const id of ids) {
      const job = await this.getJob(id);
      if (job) jobs.push(job);
    }
    return jobs;
  }

  // ─── Lifecycle ────────────────────────────────────────────────────────────

  async close() { await this.redis.quit(); }

  async flush() {
    const keys = await this.redis.keys('jq:*');
    if (keys.length) await this.redis.del(...keys);
  }
}

module.exports = Queue;
