const Queue  = require('./src/Queue');
const Worker = require('./src/Worker');
const { Job } = require('./src/Job');

// ─── Handlers (pure async functions — no queue knowledge) ────────────────────

const handlers = {
  sendEmail: async ({ to }) => {
    process.stdout.write(`  → email → ${to}\n`);
    await sleep(200);

    // Simulate failure
    if (Math.random() < 0.4) throw new Error('SMTP timeout');
  },

  resizeImage: async ({ url }) => {
    process.stdout.write(`  → resize → ${url}\n`);
    await sleep(300);
  },

  processPayment: async ({ amount }) => {
    process.stdout.write(`  → payment → $${amount}\n`);
    await sleep(150);
  },
};

// ─── Bootstrap ───────────────────────────────────────────────────────────────

async function main() {
  const queue = new Queue();

  const worker = new Worker(queue, handlers, {
    concurrency: 3,
    pollInterval: 300,
    visibilityTimeout: 5000, // 🔥 NEW (important)
  });

  // Start processing
  worker.start();

  console.log('\n🚀 System started (with visibility timeout enabled)\n');

  // ─── Push jobs ────────────────────────────────────────────────────────────

  await queue.push(new Job({ type: 'sendEmail',      payload: { to: 'alice@example.com' }, priority: 1  }));
  await queue.push(new Job({ type: 'processPayment', payload: { amount: 99.99 },           priority: 10 }));
  await queue.push(new Job({ type: 'resizeImage',    payload: { url: 'banner.png' },       priority: 5  }));
  await queue.push(new Job({ type: 'sendEmail',      payload: { to: 'bob@example.com' },   priority: 1  }));
  await queue.push(new Job({ type: 'processPayment', payload: { amount: 49.00 },           priority: 10 }));
  await queue.push(new Job({ type: 'unknownJob',     payload: {},                          priority: 3  }));

  // ─── 🔥 OPTIONAL: Simulate worker crash (to prove recovery) ────────────────

  setTimeout(() => {
    console.log('\n💥 Simulating worker crash...\n');
    worker.stop();
  }, 7000);

  setTimeout(() => {
    console.log('\n♻️ Restarting worker...\n');
    worker.start();
  }, 12000);

  // ─── Stats + DLQ ──────────────────────────────────────────────────────────

  setTimeout(async () => {
    const stats   = await queue.stats();
    const dlqJobs = await queue.getDLQJobs();

    console.log('\n──────────── Stats ────────────');
    console.log(stats); // now includes processing

    console.log('\n──────────── Dead-Letter Queue ────────────');
    dlqJobs.forEach((j) =>
      console.log(`  id=${j.id}  type=${j.type}  retries=${j.retries}`)
    );

    worker.stop();
    await queue.close();
    process.exit(0);
  }, 20000);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

main().catch(console.error);