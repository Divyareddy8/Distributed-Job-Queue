'use strict';

const express  = require('express');
const { Job }  = require('../../Job');
const router   = express.Router();

// POST /api/jobs — submit a new job
router.post('/', async (req, res) => {
  const { type, payload, priority = 0, maxRetries = 3 } = req.body;

  if (!type)    return res.status(400).json({ error: '`type` is required' });
  if (!payload) return res.status(400).json({ error: '`payload` is required' });

  // Rate-limit check
  const rl = await req.queue.checkRateLimit(type);
  if (!rl.allowed) {
    return res.status(429).json({
      error:        'Rate limit exceeded',
      resetAfterMs: rl.resetAfterMs,
    });
  }

  let job;
  try {
    job = new Job({ type, payload, priority, maxRetries });
  } catch (e) {
    return res.status(400).json({ error: e.message });
  }

  await req.queue.push(job);

  // Broadcast new-job event so the dashboard Jobs page updates live
  req.broadcast('job', { event: 'job:queued', job });

  return res.status(201).json(job);
});

// GET /api/jobs/recent — last N jobs
router.get('/recent', async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit || '50', 10), 200);
  const jobs  = await req.queue.getRecentJobs(limit);
  return res.json(jobs);
});

// GET /api/jobs/dlq/list
router.get('/dlq/list', async (req, res) => {
  const limit  = Math.min(parseInt(req.query.limit  || '50',  10), 500);
  const offset = parseInt(req.query.offset || '0', 10);
  const jobs   = await req.queue.getDLQJobs(limit, offset);
  return res.json(jobs);
});

// POST /api/jobs/dlq/:id/replay
router.post('/dlq/:id/replay', async (req, res) => {
  const job = await req.queue.replayFromDLQ(req.params.id);
  if (!job) return res.status(404).json({ error: 'Job not found in DLQ' });
  req.broadcast('job', { event: 'job:queued', job });
  return res.json(job);
});

// DELETE /api/jobs/dlq/purge
router.delete('/dlq/purge', async (req, res) => {
  await req.queue.purgeDLQ();
  return res.json({ ok: true });
});

// GET /api/jobs/:id
router.get('/:id', async (req, res) => {
  const job = await req.queue.getJob(req.params.id);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  return res.json(job);
});

// DELETE /api/jobs/:id — cancel a pending job
router.delete('/:id', async (req, res) => {
  const ok = await req.queue.cancel(req.params.id);
  if (!ok) return res.status(409).json({ error: 'Cannot cancel — job is not pending or does not exist' });
  req.broadcast('job', { event: 'job:cancelled', job: { id: req.params.id } });
  return res.json({ ok: true });
});

module.exports = router;
