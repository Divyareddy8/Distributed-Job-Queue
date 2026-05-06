'use strict';

const Queue   = require('./src/Queue');
const Worker  = require('./src/Worker');
const { Job } = require('./src/Job');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ─── Handlers ────────────────────────────────────────────────────────────────
// Pure async functions — no queue knowledge, easily unit-testable.

const handlers = {
  async sendEmail({ to }) {
    process.stdout.write(`  → email      → ${to}\n`);
    await sleep(200);
    if (Math.random() < 0.4) throw new Error('SMTP timeout');
  },

  async resizeImage({ url }) {
    process.stdout.write(`  → resize     → ${url}\n`);
    await sleep(300);
  },

  async processPayment({ amount }) {
    process.stdout.write(`  → payment    → $${amount}\n`);
    await sleep(150);
  },
};

// ─── Bootstrap ───────────────────────────────────────────────────────────────

async function main() {
  const queue  = new Queue();
  const worker = new Worker(queue, handlers, {
    concurrency:       3,
    pollInterval:      300,
    visibilityTimeout: 5_000,
  });

  // ── Connect to Redis BEFORE pushing any jobs ──────────────────────────────
  await queue.connect();

  // ── Flush stale data from previous runs so stats/DLQ are always fresh ────
  await queue.flush();
  console.log('[Queue] 🧹 flushed stale keys\n');

  // ── Graceful shutdown on Ctrl-C / SIGTERM ─────────────────────────────────
  const shutdown = async (signal) => {
    console.log(`\n[main] received ${signal} — shutting down…`);
    await worker.stop();
    await queue.close();
    process.exit(0);
  };
  process.once('SIGINT',  () => shutdown('SIGINT'));
  process.once('SIGTERM', () => shutdown('SIGTERM'));

  // ── Start worker ──────────────────────────────────────────────────────────
  worker.start();
  console.log('\n🚀 System started\n');

  // ── Push demo jobs ────────────────────────────────────────────────────────
  await queue.push(new Job({ type: 'sendEmail',      payload: { to: 'alice@example.com' }, priority: 1  }));
  await queue.push(new Job({ type: 'processPayment', payload: { amount: 99.99 },           priority: 10 }));
  await queue.push(new Job({ type: 'resizeImage',    payload: { url: 'banner.png' },       priority: 5  }));
  await queue.push(new Job({ type: 'sendEmail',      payload: { to: 'bob@example.com' },   priority: 1  }));
  await queue.push(new Job({ type: 'processPayment', payload: { amount: 49.00 },           priority: 10 }));
  await queue.push(new Job({ type: 'unknownJob',     payload: { foo: 'bar' },              priority: 3  }));

  // ── Simulate crash-and-recovery ───────────────────────────────────────────
  setTimeout(() => {
    console.log('\n💥 Simulating worker crash…\n');
    worker.stop();
  }, 7_000);

  setTimeout(() => {
    console.log('\n♻️  Restarting worker…\n');
    worker.start();
  }, 12_000);

  // ── Final stats + clean exit ──────────────────────────────────────────────
  setTimeout(async () => {
    const [stats, dlqJobs] = await Promise.all([
      queue.stats(),
      queue.getDLQJobs(),
    ]);

    console.log('\n──────────── Stats ────────────');
    console.table(stats);

    console.log('\n──────────── Dead-Letter Queue ────────────');
    if (dlqJobs.length === 0) {
      console.log('  (empty)');
    } else {
      dlqJobs.forEach((j) =>
        console.log(`  id=${j.id}  type=${j.type}  retries=${j.retries}`)
      );
    }

    await worker.stop();
    await queue.close();
    process.exit(0);
  }, 20_000);
}

main().catch((err) => {
  console.error('[main] fatal:', err);
  process.exit(1);
});
