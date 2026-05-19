'use strict';

const express = require('express');

/** Stats route — mounted at /api/stats */
module.exports = function statsRoute(queue) {
  const router = express.Router();

  router.get('/', async (_req, res) => {
    const s = await queue.stats();
    res.json(s);
  });

  return router;
};
