'use strict';

/**
 * Standalone Worker Process
 * ─────────────────────────
 * Configure entirely via environment variables — no code changes needed
 * for different worker roles in a Kubernetes deployment.
 *
 * Environment variables:
 *   REDIS_HOST          Redis hostname         (default: 127.0.0.1)
 *   REDIS_PORT          Redis port             (default: 6379)
 *   REDIS_PASSWORD      Redis password         (optional)
 *   WORKER_TYPES        Comma-separated types this worker handles
 *                       (omit or set to "" for all types)
 *   WORKER_CONCURRENCY  Max parallel jobs      (default: 3)
 *   WORKER_ID           Display name in logs   (default: w-<PID>)
 *   POLL_INTERVAL_MS    Idle poll interval     (default: 300)
 *   VISIBILITY_TIMEOUT  Processing timeout ms  (default: 5000)
 *
 * Examples:
 *   # General-purpose worker
 *   node workers/worker.js
 *
 *   # Email-only worker, high concurrency
 *   WORKER_TYPES=sendEmail WORKER_CONCURRENCY=10 node workers/worker.js
 *
 *   # Payment worker (isolated)
 *   WORKER_TYPES=processPayment WORKER_ID=pay-worker-1 node workers/worker.js
 */

const Queue  = require('../src/Queue');
const Worker = require('../src/Worker');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ─── Handlers ────────────────────────────────────────────────────────────────
// In production, split into separate files under handlers/ and require them here.

const handlers = {
  async sendEmail({ to, subject = 'No subject', body = '' }) {
    console.log(`  [sendEmail] → ${to}`);
    await sleep(200);
    // Simulate occasional SMTP failures
    if (Math.random() < 0.3) throw new Error('SMTP timeout');
  },

  async resizeImage({ url, width = 800, height = 600 }) {
    console.log(`  [resizeImage] → ${url}  ${width}×${height}`);
    await sleep(300);
  },

  async processPayment({ amount, currency = 'USD', userId }) {
    console.log(`  [processPayment] → ${currency} ${amount}  user=${userId}`);
    await sleep(150);
    if (Math.random() < 0.1) throw new Error('Gateway timeout');
  },

  async generateReport({ reportId, format = 'pdf' }) {
    console.log(`  [generateReport] → ${reportId}  format=${format}`);
    await sleep(800);
  },
};

// ─── Bootstrap ───────────────────────────────────────────────────────────────

async function main() {
  const rawTypes  = process.env.WORKER_TYPES;
  const types     = rawTypes ? rawTypes.split(',').map((t) => t.trim()).filter(Boolean) : null;
  const concurrency     = parseInt(process.env.WORKER_CONCURRENCY   || '3',    10);
  const pollInterval    = parseInt(process.env.POLL_INTERVAL_MS      || '300',  10);
  const visibilityTimeout = parseInt(process.env.VISIBILITY_TIMEOUT  || '5000', 10);
  const workerId        = process.env.WORKER_ID || `w-${process.pid}`;

  const queue  = new Queue();
  const worker = new Worker(queue, handlers, {
    concurrency,
    pollInterval,
    visibilityTimeout,
    types,
    workerId,
  });

  await queue.connect();
  worker.start();

  // ── Graceful shutdown ──────────────────────────────────────────────────────
  const shutdown = async (signal) => {
    console.log(`\n[${workerId}] ${signal} received — shutting down gracefully…`);
    await worker.stop();
    await queue.close();
    process.exit(0);
  };

  process.once('SIGINT',  () => shutdown('SIGINT'));
  process.once('SIGTERM', () => shutdown('SIGTERM'));

  // Kubernetes sends SIGTERM; we have ~30 s before a SIGKILL arrives.
  // The worker drains in-flight jobs inside stop(), so this is safe.
}

main().catch((err) => {
  console.error('[worker] fatal:', err);
  process.exit(1);
});
