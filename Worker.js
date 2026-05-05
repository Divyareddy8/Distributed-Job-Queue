const { Status } = require('./Job');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

class Worker {
  constructor(queue, handlers, options = {}) {
    this.queue        = queue;
    this.handlers     = handlers;
    this.concurrency  = options.concurrency  || 3;
    this.pollInterval = options.pollInterval || 500;
    this.visibilityTimeout = options.visibilityTimeout || 5000;

    this.running = false;
    this.active  = 0;
  }

  // ─── BACKOFF ───────────────────────────────────────────────────────────────

  backoffMs(retries) {
    return Math.pow(2, retries) * 1000;
  }

  // ─── JOB EXECUTION ─────────────────────────────────────────────────────────

  async processJob(job) {
    const handler = this.handlers[job.type];

    if (!handler) {
      console.warn(`[Worker] ⚠️ no handler for type="${job.type}"`);
      await this.queue.ack(job);
      await this.queue.sendToDLQ(job);
      return;
    }

    await this.queue.setStatus(job, Status.PROCESSING);
    console.log(`[Worker] ⚙️ start ${job.id} attempt=${job.retries + 1}`);

    try {
      await handler(job.payload);

      await this.queue.setStatus(job, Status.COMPLETED);
      await this.queue.ack(job); // ✅ critical

      console.log(`[Worker] ✅ done ${job.id}`);

    } catch (err) {
      job.retries += 1;

      console.error(`[Worker] ❌ failed ${job.id} retries=${job.retries}`);

      await this.queue.ack(job); // remove from processing FIRST

      if (job.retries > job.maxRetries) {
        await this.queue.sendToDLQ(job);
      } else {
        const delay = this.backoffMs(job.retries);

        await this.queue.setStatus(job, Status.FAILED);
        console.log(`[Worker] 🔁 retry ${job.id} in ${delay / 1000}s`);

        await this.queue.requeueWithDelay(job, delay);
      }
    }
  }

  // ─── MAIN LOOP ─────────────────────────────────────────────────────────────

  async start() {
    this.running = true;

    console.log(`[Worker] 🚀 started concurrency=${this.concurrency}`);

    // Delayed job promoter
    this._promoter = setInterval(() => this.queue.promoteDelayed(), 500);

    // 🔥 Visibility timeout recovery
    this._reclaimer = setInterval(() => this.queue.reclaimStuckJobs(), 1000);

    while (this.running) {
      if (this.active < this.concurrency) {
        const job = await this.queue.popForProcessing(this.visibilityTimeout);

        if (job) {
          this.active++;

          this.processJob(job).finally(() => this.active--);

        } else {
          await sleep(this.pollInterval);
        }

      } else {
        await sleep(50);
      }
    }
  }

  stop() {
    this.running = false;

    clearInterval(this._promoter);
    clearInterval(this._reclaimer);

    console.log('[Worker] 🛑 stopped');
  }
}

module.exports = Worker;