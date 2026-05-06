'use strict';

const { Status } = require('./Job');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Worker — pulls jobs from the Queue, dispatches to registered handlers,
 * and manages retries, backoff, and dead-letter routing.
 *
 * Design goals
 * ────────────
 * • Concurrency is controlled via a simple counter (`this.active`).
 * • Each job runs in its own async "fiber" so failures are isolated.
 * • Graceful shutdown: `stop()` signals the loop, then waits for all
 *   in-flight jobs to finish before resolving.
 */
class Worker {
  /**
   * @param {import('./Queue')} queue
   * @param {Record<string, (payload: any) => Promise<void>>} handlers
   * @param {object}  [opts]
   * @param {number}  [opts.concurrency=3]
   * @param {number}  [opts.pollInterval=500]   ms to wait when queue is empty
   * @param {number}  [opts.visibilityTimeout=5000]  ms before a stuck job is reclaimed
   */
  constructor(queue, handlers, opts = {}) {
    this.queue             = queue;
    this.handlers          = handlers;
    this.concurrency       = opts.concurrency       ?? 3;
    this.pollInterval      = opts.pollInterval      ?? 500;
    this.visibilityTimeout = opts.visibilityTimeout ?? 5_000;

    this._running  = false;
    this._active   = 0;
    this._stopping = null;   // Promise that resolves when fully drained
  }

  // ─── Backoff ──────────────────────────────────────────────────────────────

  /** Exponential backoff: 2^retries seconds, capped at 60 s. */
  _backoffMs(retries) {
    return Math.min(Math.pow(2, retries) * 1_000, 60_000);
  }

  // ─── Job execution ────────────────────────────────────────────────────────

  async _processJob(job) {
    const handler = this.handlers[job.type];

    if (!handler) {
      console.warn(`[Worker] ⚠️  no handler for type="${job.type}" — sending to DLQ`);
      // Ack first so the job leaves processing, then bury it
      await this.queue.ack(job);
      await this.queue.sendToDLQ(job);
      return;
    }

    await this.queue.setStatus(job, Status.PROCESSING);
    console.log(`[Worker] ⚙️  start  ${job.id}  attempt=${job.retries + 1}`);

    try {
      await handler(job.payload);

      // ── SUCCESS ──
      // 1. Persist COMPLETED status first, THEN ack.
      //    This guarantees the final state is always in Redis before the
      //    job disappears from the processing set.
      await this.queue.setStatus(job, Status.COMPLETED);
      await this.queue.ack(job, /* cleanup= */ true);

      console.log(`[Worker] ✅ done   ${job.id}`);

    } catch (err) {
      job.retries += 1;
      console.error(`[Worker] ❌ failed ${job.id}  retries=${job.retries}  err="${err.message}"`);

      // ── FAILURE ──
      // Ack first: remove from processing regardless of what happens next.
      await this.queue.ack(job);

      if (job.retries > job.maxRetries) {
        await this.queue.sendToDLQ(job);
      } else {
        const delay = this._backoffMs(job.retries);
        await this.queue.setStatus(job, Status.FAILED);
        console.log(`[Worker] 🔁 retry  ${job.id}  in ${delay / 1_000}s`);
        await this.queue.requeueWithDelay(job, delay);
      }
    }
  }

  // ─── Main loop ────────────────────────────────────────────────────────────

  /**
   * Start processing. Idempotent: calling start() on a running worker is a no-op.
   */
  start() {
    if (this._running) {
      console.warn('[Worker] already running — ignoring start()');
      return;
    }

    this._running = true;
    console.log(`[Worker] 🚀 started  concurrency=${this.concurrency}`);

    // Promote delayed jobs every 500 ms
    this._promoter = setInterval(() => this.queue.promoteDelayed().catch(console.error), 500);

    // Reclaim stuck jobs every second
    this._reclaimer = setInterval(() => this.queue.reclaimStuckJobs().catch(console.error), 1_000);

    // Kick off the poll loop (fire-and-forget; errors are caught inside)
    this._loopPromise = this._loop();
  }

  async _loop() {
    while (this._running) {
      if (this._active < this.concurrency) {
        let job = null;
        try {
          job = await this.queue.popForProcessing(this.visibilityTimeout);
        } catch (err) {
          console.error('[Worker] pop error:', err.message);
          await sleep(this.pollInterval);
          continue;
        }

        if (job) {
          this._active++;
          this._processJob(job).finally(() => {
            this._active--;
            // If we're draining, check whether we're done
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

  /**
   * Signal the loop to stop and wait for in-flight jobs to finish.
   * @returns {Promise<void>} resolves when fully drained
   */
  async stop() {
    if (!this._running) return;

    this._running = false;
    clearInterval(this._promoter);
    clearInterval(this._reclaimer);

    if (this._active > 0) {
      console.log(`[Worker] 🛑 draining ${this._active} in-flight job(s)…`);
      await new Promise((resolve) => { this._resolveDrain = resolve; });
    }

    console.log('[Worker] 🛑 stopped');
  }
}

module.exports = Worker;
