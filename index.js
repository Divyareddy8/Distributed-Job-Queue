'use strict';

/**
 * index.js — local demo / smoke-test
 *
 * Spins up an in-process API server + workers so you can try everything
 * without Docker.  For production, run workers and the API separately.
 */

const Queue  = require('./src/Queue');
const Worker = require('./src/Worker');
const { Job } = require('./src/Job');
const { createApp } = require('./src/api/server');
const http   = require('http');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const handlers = {
  async sendEmail({ to }) {
    process.stdout.write(`  → sendEmail      → ${to}\n`);
    await sleep(200);
    if (Math.random() < 0.4) throw new Error('SMTP timeout');
  },
  async resizeImage({ url }) {
    process.stdout.write(`  → resizeImage    → ${url}\n`);
    await sleep(300);
  },
  async processPayment({ amount }) {
    process.stdout.write(`  → processPayment → $${amount}\n`);
    await sleep(150);
  },
};

async function main() {
  const rateLimits = {
    sendEmail:      { max: 10, windowMs: 10_000 },
    processPayment: { max: 5,  windowMs: 10_000 },
  };

  const queue = new Queue({}, rateLimits);
  await queue.connect();
  await queue.flush();
  console.log('[demo] 🧹 flushed stale keys\n');

  // Start the dashboard / API on port 3000
  const app    = createApp(queue);
  const server = http.createServer(app);
  server.listen(3000, () => console.log('[demo] 🌐 Dashboard → http://localhost:3000\n'));

  // Start workers
  const workers = [
    new Worker(queue, handlers, { concurrency: 3, workerId: 'worker-A' }),
    new Worker(queue, handlers, { concurrency: 2, types: ['sendEmail'], workerId: 'worker-B' }),
  ];
  workers.forEach((w) => w.start());

  // Push demo jobs
  await queue.push(new Job({ type: 'sendEmail',      payload: { to: 'alice@example.com' }, priority: 1  }));
  await queue.push(new Job({ type: 'processPayment', payload: { amount: 99.99 },           priority: 10 }));
  await queue.push(new Job({ type: 'resizeImage',    payload: { url: 'banner.png' },       priority: 5  }));
  await queue.push(new Job({ type: 'sendEmail',      payload: { to: 'bob@example.com' },   priority: 1  }));
  await queue.push(new Job({ type: 'processPayment', payload: { amount: 49.00 },           priority: 10 }));
  await queue.push(new Job({ type: 'unknownJob',     payload: { foo: 'bar' },              priority: 3  }));

  // Graceful shutdown
  const shutdown = async (sig) => {
    console.log(`\n[demo] ${sig} — shutting down…`);
    await Promise.all(workers.map((w) => w.stop()));
    server.close();
    await queue.close();
    process.exit(0);
  };
  process.once('SIGINT',  () => shutdown('SIGINT'));
  process.once('SIGTERM', () => shutdown('SIGTERM'));

  // Auto-exit after 25 s for CI
  if (process.env.CI) {
    setTimeout(async () => {
      const [stats, dlq] = await Promise.all([queue.stats(), queue.getDLQJobs()]);
      console.log('\n──── Stats ────'); console.table(stats);
      console.log('\n──── DLQ ──────');
      dlq.length ? dlq.forEach((j) => console.log(`  id=${j.id} type=${j.type} retries=${j.retries}`))
                 : console.log('  (empty)');
      await shutdown('timeout');
    }, 25_000);
  }
}

main().catch((err) => { console.error('[demo] fatal:', err); process.exit(1); });
