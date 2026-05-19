'use strict';

const path    = require('path');
const express = require('express');
const Queue   = require('../Queue');
const Worker  = require('../Worker');

const app = express();
app.use(express.json());

// ─── CORS ─────────────────────────────────────────────────────────────────
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,DELETE,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// ─── SSE broadcaster ──────────────────────────────────────────────────────
const sseClients = new Set();

function broadcast(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) {
    try { res.write(payload); } catch { sseClients.delete(res); }
  }
}

// ─── Stats broadcaster ────────────────────────────────────────────────────
// Always read queued count directly from Redis sorted sets (ground truth).
// The old in-memory _queuedInFlight counter was overriding s.queued and
// losing sync whenever jobs were retried/delayed, causing "queued = 0" bugs.
async function broadcastStats() {
  try {
    const s = await queue.stats();
    // s.queued comes from Redis zcard — accurate count of pending jobs
    broadcast('stats', s);
  } catch (err) {
    console.error('[SSE] stats error:', err.message);
  }
}

// ─── Queue + in-process worker ────────────────────────────────────────────
const rateLimits = {
  sendEmail:      { max: 60, windowMs: 60_000 },
  processPayment: { max: 10, windowMs: 60_000 },
  resizeImage:    { max: 30, windowMs: 60_000 },
  generateReport: { max: 5,  windowMs: 60_000 },
};

const queue = new Queue({}, rateLimits);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const handlers = {
  async sendEmail({ to, subject = 'No subject', body = '' }) {
    console.log(`  [sendEmail] → ${to}`);
    await sleep(200);
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
    await sleep(800);  // slowest job — 800 ms gives dashboard time to show it queued
  },
};

const worker = new Worker(queue, handlers, {
  concurrency:       3,
  pollInterval:      300,
  visibilityTimeout: 5_000,
  workerId:          'api-worker',
  // FIX: broadcast stats after EVERY job status change so dashboard updates instantly
  onJobEvent: (event, job) => {
    broadcast('job', { event, job });
    broadcastStats();   // push fresh Redis counts after each transition
  },
});

// ─── Routes ───────────────────────────────────────────────────────────────
const jobsRouter  = require('./routes/jobs');
const statsRouter = require('./routes/stats');

// FIX: wrap broadcast so jobs.js can call broadcastStats after push
app.use((req, _res, next) => {
  req.queue = queue;
  req.broadcast = (event, data) => {
    broadcast(event, data);
    if (event === 'job') broadcastStats();  // push fresh stats after any job event
  };
  next();
});

app.use('/api/jobs',  jobsRouter);
app.use('/api/stats', statsRouter);

// SSE endpoint
app.get('/api/events', (req, res) => {
  res.setHeader('Content-Type',      'text/event-stream');
  res.setHeader('Cache-Control',     'no-cache');
  res.setHeader('Connection',        'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  sseClients.add(res);
  req.on('close', () => sseClients.delete(res));

  // Send current stats immediately on connect
  queue.stats()
    .then(s => {
      res.write(`event: stats\ndata: ${JSON.stringify(s)}\n\n`);
    })
    .catch(() => {});
});

// Dashboard
app.use(express.static(path.join(__dirname, '../dashboard')));
app.get('/', (_req, res) =>
  res.sendFile(path.join(__dirname, '../dashboard/index.html'))
);

// ─── Boot ─────────────────────────────────────────────────────────────────
const PORT = parseInt(process.env.PORT || '3000', 10);

(async () => {
  await queue.connect();
  worker.start();

  // Background stats ticker — catches anything missed between events
  setInterval(broadcastStats, 1_500);

  app.listen(PORT, '0.0.0.0', () =>
    console.log(`[API] 🚀 listening on http://0.0.0.0:${PORT}`)
  );

  const shutdown = async (sig) => {
    console.log(`\n[API] ${sig} — shutting down…`);
    await worker.stop();
    await queue.close();
    process.exit(0);
  };
  process.once('SIGINT',  () => shutdown('SIGINT'));
  process.once('SIGTERM', () => shutdown('SIGTERM'));
})();