'use strict';

const express = require('express');
const { Job } = require('../../Job');

/**
 * Job routes — mounted at /api/jobs
 *
 * POST   /                  Submit a new job
 * GET    /recent            Last N pushed jobs
 * GET    /:id               Job detail / status
 * DELETE /:id               Cancel a PENDING job
 * GET    /dlq/list          DLQ entries
 * POST   /dlq/:id/replay    Re-queue a DLQ job
 * DELETE /dlq/purge         Clear the entire DLQ
 */
module.exports = function jobRoutes(queue) {
  const router = express.Router();

  // ── Submit ─────────────────────────────────────────────────────────────

  router.post('/', async (req, res) => {
    const { type, payload, priority = 0, maxRetries = 3 } = req.body ?? {};

    if (!type || typeof type !== 'string') {
      return res.status(400).json({ error: '`type` (string) is required' });
    }
    if (!payload || typeof payload !== 'object') {
      return res.status(400).json({ error: '`payload` (object) is required' });
    }

    // Rate-limit check — enforced inside Queue so the counter is shared
    // across all API replicas (state lives in Redis).
    const rl = await queue.checkRateLimit(type);
    if (!rl.allowed) {
      res.setHeader('Retry-After', Math.ceil(rl.resetAfterMs / 1_000));
      return res.status(429).json({
        error:        `Rate limit exceeded for type "${type}"`,
        resetAfterMs: rl.resetAfterMs,
      });
    }

    const job = new Job({
      type,
      payload,
      priority:   Number(priority),
      maxRetries: Number(maxRetries),
    });

    await queue.push(job);

    res.status(201).json({
      id:         job.id,
      type:       job.type,
      status:     job.status,
      priority:   job.priority,
      maxRetries: job.maxRetries,
      createdAt:  job.createdAt,
    });
  });

  // ── Recent jobs ────────────────────────────────────────────────────────

  router.get('/recent', async (req, res) => {
    const limit = Math.min(parseInt(req.query.limit || '20', 10), 100);
    const jobs  = await queue.getRecentJobs(limit);
    res.json(jobs);
  });

  // ── DLQ — must be before /:id to avoid param collision ────────────────

  router.get('/dlq/list', async (req, res) => {
    const limit  = Math.min(parseInt(req.query.limit  || '50', 10), 200);
    const offset = Math.max(parseInt(req.query.offset || '0',  10), 0);
    const jobs   = await queue.getDLQJobs(limit, offset);
    res.json(jobs);
  });

  router.post('/dlq/:id/replay', async (req, res) => {
    const job = await queue.replayFromDLQ(req.params.id);
    if (!job) return res.status(404).json({ error: 'Job not found in DLQ' });
    res.json({ replayed: true, id: job.id, type: job.type });
  });

  router.delete('/dlq/purge', async (_req, res) => {
    await queue.purgeDLQ();
    res.json({ purged: true });
  });

  // ── Single job ─────────────────────────────────────────────────────────

  router.get('/:id', async (req, res) => {
    const job = await queue.getJob(req.params.id);
    if (!job) return res.status(404).json({ error: 'Job not found' });
    res.json(job);
  });

  router.delete('/:id', async (req, res) => {
    const ok = await queue.cancel(req.params.id);
    if (!ok) {
      return res.status(409).json({
        error: 'Cannot cancel: job not found or not in PENDING state',
      });
    }
    res.json({ cancelled: true, id: req.params.id });
  });

  return router;
};
