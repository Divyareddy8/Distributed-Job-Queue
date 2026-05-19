'use strict';

const { Status } = require('./Job');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Worker — pulls jobs from the Queue, dispatches to registered handlers,
 * and manages retries, backoff, and dead-letter routing.
 */
class Worker {
  /**
   * @param {import('./Queue')} queue
   * @param {Record<string, (payload: any) => Promise<void>>} handlers
   * @param {object}        [opts]
   * @param {number}        [opts.concurrency=3]
   * @param {number}        [opts.pollInterval=500]       ms to wait when queue is empty
   * @param {number}        [opts.visibilityTimeout=5000] ms before a stuck job is reclaimed
   * @param {string[]|null} [opts.types=null]             job types this worker handles
   * @param {string}        [opts.workerId]               label for logs
   * @param {Function}      [opts.onJobEvent]             optional callback(event, job)
   *                                                      called after every status change;
   *                                                      used by server.js to broadcast SSE.
   */
  constructor(queue, handlers, opts = {}) {
    this.queue             = queue;
    this.handlers          = handlers;
    this.concurrency       = opts.concurrency       ?? 3;
    this.pollInterval      = opts.pollInterval      ?? 500;
    this.visibilityTimeout = opts.visibilityTimeout ?? 5_000;
    this.types             = opts.types             ?? null;
    this.workerId          = opts.workerId          ?? `w-${process.pid}`;
    // FIX: callback so the API server can broadcast SSE job-status events
    this.onJobEvent        = opts.onJobEvent        ?? null;

    this._running      = false;
    this._active       = 0;
    this._resolveDrain = null;
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────

  /** Exponential backoff: 2^retries seconds, capped at 60 s. */
  _backoffMs(retries) {
    return Math.min(Math.pow(2, retries) * 1_000, 60_000);
  }

  /** Notify the SSE broadcaster (if wired up). */
  _emit(event, job) {
    if (this.onJobEvent) {
      try { this.onJobEvent(event, job); } catch { /* never crash the worker */ }
    }
  }

  // ─── Job execution ────────────────────────────────────────────────────────

  async _processJob(job) {
    const id      = `[${this.workerId}]`;
    const handler = this.handlers[job.type];

    if (!handler) {
      console.warn(`${id} ⚠️  no handler for type="${job.type}" — sending to DLQ`);
      await this.queue.ack(job);
      await this.queue.sendToDLQ(job);
      this._emit('job:dead', job);
      return;
    }

    await this.queue.setStatus(job, Status.PROCESSING);
    this._emit('job:processing', job);
    console.log(`${id} ⚙️  start  ${job.id}  type=${job.type}  attempt=${job.retries + 1}`);

    try {
      await handler(job.payload);

      // FIX: setStatus(COMPLETED) writes the completed state to Redis,
      // then ack() ONLY removes from the processing set (no data deletion).
      await this.queue.setStatus(job, Status.COMPLETED);
      await this.queue.ack(job);           // ← no cleanup=true anymore
      await this.queue.incrementCounter('completed');
      this._emit('job:completed', job);
      console.log(`${id} ✅ done   ${job.id}`);

    } catch (err) {
      job.retries += 1;
      console.error(`${id} ❌ failed ${job.id}  retries=${job.retries}  err="${err.message}"`);

      await this.queue.ack(job);   // remove from processing regardless

      if (job.retries > job.maxRetries) {
        await this.queue.sendToDLQ(job);
        await this.queue.incrementCounter('failed');
        this._emit('job:dead', job);
      } else {
        const delay = this._backoffMs(job.retries);
        await this.queue.setStatus(job, Status.FAILED);
        this._emit('job:failed', job);
        console.log(`${id} 🔁 retry  ${job.id}  in ${delay / 1_000}s`);
        await this.queue.requeueWithDelay(job, delay);
      }
    }
  }

  // ─── Main loop ────────────────────────────────────────────────────────────

  start() {
    if (this._running) {
      console.warn(`[${this.workerId}] already running — ignoring start()`);
      return;
    }

    this._running = true;
    const typeStr = this.types ? this.types.join(',') : 'ALL';
    console.log(`[${this.workerId}] 🚀 started  concurrency=${this.concurrency}  types=${typeStr}`);

    // Background: promote delayed jobs into the active queues
    this._promoter  = setInterval(
      () => this.queue.promoteDelayed().catch(console.error), 500,
    );
    // Background: reclaim jobs whose workers crashed
    this._reclaimer = setInterval(
      () => this.queue.reclaimStuckJobs().catch(console.error), 1_000,
    );

    this._loopPromise = this._loop();
  }

  async _loop() {
    while (this._running) {
      if (this._active < this.concurrency) {
        let job = null;
        try {
          job = await this.queue.popForProcessing(this.visibilityTimeout, this.types);
        } catch (err) {
          console.error(`[${this.workerId}] pop error:`, err.message);
          await sleep(this.pollInterval);
          continue;
        }

        if (job) {
          this._active++;
          this._processJob(job).finally(() => {
            this._active--;
            if (!this._running && this._active === 0) {
              this._resolveDrain?.();
            }
          });
        } else {
          await sleep(this.pollInterval);
        }
      } else {
        await sleep(50);
      }
    }
  }

  async stop() {
    if (!this._running) return;

    this._running = false;
    clearInterval(this._promoter);
    clearInterval(this._reclaimer);

    if (this._active > 0) {
      console.log(`[${this.workerId}] 🛑 draining ${this._active} in-flight job(s)…`);
      await new Promise((resolve) => { this._resolveDrain = resolve; });
    }

    console.log(`[${this.workerId}] 🛑 stopped`);
  }
}

module.exports = Worker;
