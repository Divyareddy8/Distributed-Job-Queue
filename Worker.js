const { Status } = require('./Job');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

class Worker {
  /**
   * @param {import('./Queue')} queue
   * @param {Record<string, (payload: any) => Promise<void>>} handlers  { jobType → async fn }
   * @param {{ concurrency?: number, pollInterval?: number }} options
   */
  constructor(queue, handlers, options = {}) {
    this.queue        = queue;
    this.handlers     = handlers;
    this.concurrency  = options.concurrency  || 3;
    this.pollInterval = options.pollInterval || 500;  // ms between empty-queue polls
    this.running      = false;
    this.active       = 0;                            // slots in use
  }

  // ─── Exponential backoff ───────────────────────────────────────────────────

  backoffMs(retries) {
    // 1 s → 2 s → 4 s → 8 s …
    return Math.pow(2, retries) * 1000;
  }

  // ─── Single job lifecycle ──────────────────────────────────────────────────

  async processJob(job) {
    const handler = this.handlers[job.type];

    if (!handler) {
      console.warn(`[Worker] ⚠️  no handler for type="${job.type}" — sending to DLQ`);
      await this.queue.sendToDLQ(job);
      return;
    }

    await this.queue.setStatus(job, Status.PROCESSING);
    console.log(`[Worker] ⚙️  start   ${job.id}  type=${job.type}  attempt=${job.retries + 1}/${job.maxRetries + 1}`);

    try {
      await handler(job.payload);
      await this.queue.setStatus(job, Status.COMPLETED);
      console.log(`[Worker] ✅ done    ${job.id}`);

    } catch (err) {
      job.retries += 1;
      console.error(`[Worker] ❌ failed  ${job.id}  reason="${err.message}"  retries=${job.retries}`);

      if (job.retries > job.maxRetries) {
        await this.queue.sendToDLQ(job);
      } else {
        const delay = this.backoffMs(job.retries);
        console.log(`[Worker] 🔁 retry   ${job.id}  in ${delay / 1000}s`);
        await this.queue.setStatus(job, Status.FAILED);
        await this.queue.requeueWithDelay(job, delay);
      }
    }
  }

  // ─── Main loop ─────────────────────────────────────────────────────────────

  async start() {
    this.running = true;
    console.log(`[Worker] 🚀 started  concurrency=${this.concurrency}`);

    // Promote matured delayed jobs every 500 ms
    this._promoter = setInterval(() => this.queue.promoteDelayed(), 500);

    while (this.running) {
      if (this.active < this.concurrency) {
        const job = await this.queue.pop();
        if (job) {
          this.active++;
          // Fire-and-forget — do NOT await; that would serialize everything
          this.processJob(job).finally(() => this.active--);
        } else {
          await sleep(this.pollInterval);
        }
      } else {
        await sleep(50); // all slots busy — back off briefly
      }
    }
  }

  stop() {
    this.running = false;
    clearInterval(this._promoter);
    console.log('[Worker] 🛑 stopped');
  }
}

module.exports = Worker;
