const Queue  = require('./src/Queue');
const Worker = require('./src/Worker');
const { Job } = require('./src/Job');

// ─── Handlers (pure async functions — no queue knowledge) ────────────────────

const handlers = {
  sendEmail: async ({ to }) => {
    process.stdout.write(`  → email → ${to}\n`);
    await sleep(200);
    if (Math.random() < 0.4) throw new Error('SMTP timeout');   // 40 % failure rate
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
  const queue  = new Queue();
  const worker = new Worker(queue, handlers, { concurrency: 3, pollInterval: 300 });

  // Start processing in the background
  worker.start();

  // Push jobs with different priorities (higher = urgent)
  //                                           priority ↓
  await queue.push(new Job({ type: 'sendEmail',       payload: { to: 'alice@example.com' },  priority: 1  }));
  await queue.push(new Job({ type: 'processPayment',  payload: { amount: 99.99 },            priority: 10 })); // ← goes first
  await queue.push(new Job({ type: 'resizeImage',     payload: { url: 'banner.png' },        priority: 5  }));
  await queue.push(new Job({ type: 'sendEmail',       payload: { to: 'bob@example.com' },    priority: 1  }));
  await queue.push(new Job({ type: 'processPayment',  payload: { amount: 49.00 },            priority: 10 })); // ← goes second
  await queue.push(new Job({ type: 'unknownJob',      payload: {},                           priority: 3  })); // ← no handler → DLQ

  // After 20 s show queue stats and DLQ contents then exit
  setTimeout(async () => {
    const stats   = await queue.stats();
    const dlqJobs = await queue.getDLQJobs();

    console.log('\n──────────── Stats ────────────');
    console.log(stats);

    console.log('\n──────────── Dead-Letter Queue ────────────');
    dlqJobs.forEach((j) =>
      console.log(`  id=${j.id}  type=${j.type}  retries=${j.retries}`)
    );

    worker.stop();
    await queue.close();
    process.exit(0);
  }, 20_000);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

main().catch(console.error);
