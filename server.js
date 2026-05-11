'use strict';

const express    = require('express');
const cors       = require('cors');
const path       = require('path');
const Queue      = require('../Queue');
const jobRoutes  = require('./routes/jobs');
const statsRoute = require('./routes/stats');

const PORT = parseInt(process.env.PORT || '3000', 10);

// ─── Rate-limit config (per job type) ────────────────────────────────────────
// Override via RATE_LIMITS env var as JSON, e.g.:
//   RATE_LIMITS='{"sendEmail":{"max":50,"windowMs":60000}}'
let RATE_LIMITS = {};
try {
  if (process.env.RATE_LIMITS) RATE_LIMITS = JSON.parse(process.env.RATE_LIMITS);
} catch { /* use defaults */ }

// ─── App factory ─────────────────────────────────────────────────────────────

function createApp(queue) {
  const app = express();

  app.use(cors());
  app.use(express.json({ limit: '1mb' }));

  // ── Static dashboard ────────────────────────────────────────────────────
  app.use(express.static(path.join(__dirname, '../dashboard')));

  // ── REST routes ─────────────────────────────────────────────────────────
  app.use('/api/jobs',  jobRoutes(queue));
  app.use('/api/stats', statsRoute(queue));

  // ── SSE: live stats stream ───────────────────────────────────────────────
  const sseClients = new Set();

  app.get('/api/events', (req, res) => {
    res.setHeader('Content-Type',                'text/event-stream');
    res.setHeader('Cache-Control',               'no-cache');
    res.setHeader('Connection',                  'keep-alive');
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.flushHeaders();

    const send = (event, data) =>
      res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);

    sseClients.add(send);
    req.on('close', () => sseClients.delete(send));

    // Push initial snapshot immediately
    queue.stats().then((s) => send('stats', s)).catch(() => {});
  });

  // Broadcast stats every 1.5 s to all connected dashboards
  setInterval(async () => {
    if (sseClients.size === 0) return;
    try {
      const s = await queue.stats();
      for (const send of sseClients) send('stats', s);
    } catch { /* Redis blip — skip tick */ }
  }, 1_500);

  // ── Health / readiness ─────────────────────────────────────────────────
  app.get('/health',    (_req, res) => res.json({ ok: true,  ts: Date.now() }));
  app.get('/ready',     async (_req, res) => {
    try {
      await queue.redis.ping();
      res.json({ ok: true });
    } catch {
      res.status(503).json({ ok: false, reason: 'redis unreachable' });
    }
  });

  // ── 404 catch-all ─────────────────────────────────────────────────────
  app.use((_req, res) => res.status(404).json({ error: 'Not found' }));

  return app;
}

// ─── Entry point ─────────────────────────────────────────────────────────────

async function startServer() {
  const queue = new Queue({}, RATE_LIMITS);
  await queue.connect();
  console.log('[API] Redis connected');

  const app    = createApp(queue);
  const server = app.listen(PORT, '0.0.0.0', () =>
    console.log(`[API] 🚀 listening on http://0.0.0.0:${PORT}`),
  );

  const shutdown = async (signal) => {
    console.log(`\n[API] ${signal} — closing…`);
    server.close();
    await queue.close();
    process.exit(0);
  };
  process.once('SIGINT',  () => shutdown('SIGINT'));
  process.once('SIGTERM', () => shutdown('SIGTERM'));
}

module.exports = { createApp };

// Allow running directly: `node src/api/server.js`
if (require.main === module) {
  startServer().catch((err) => { console.error('[API] fatal:', err); process.exit(1); });
}
