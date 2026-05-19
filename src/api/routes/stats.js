'use strict';

const express = require('express');
const router  = express.Router();

// GET /api/stats
router.get('/', async (req, res) => {
  const stats = await req.queue.stats();
  return res.json(stats);
});

module.exports = router;
