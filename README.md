# Job Queue

Minimal distributed job queue built from scratch — no Bull, no BullMQ.

## Features
- **Priority scheduling** — sorted set; higher priority runs first
- **Concurrent workers** — configurable concurrency slot pool
- **Exponential backoff** — 1 s → 2 s → 4 s → 8 s on retry
- **Dead-letter queue** — jobs that exhaust retries land here
- **Delayed jobs** — retries are time-delayed via a separate sorted set

## Architecture

```
Producer (push)
     │
     ▼
 Redis sorted set  ← score = -priority
 (jq:queue)
     │
     ▼  zpopmin
  Worker pool  (3 concurrent slots)
     │
  ┌──┴──────────────────┐
  │ success             │ failure
  ▼                     ▼
COMPLETED        retry ≤ maxRetries?
                   yes → jq:delayed (score = run-at ms)
                    no → jq:dlq (dead-letter)
```

## Files

| File | Role |
|------|------|
| `src/Job.js`   | Plain data class — serialize / deserialize |
| `src/Queue.js` | Redis adapter — push, pop, DLQ, delayed |
| `src/Worker.js`| Poll loop — concurrency, backoff, retry routing |
| `index.js`     | Demo: push jobs, define handlers, print stats |

## Setup

```bash
# 1. Redis must be running on localhost:6379
redis-server

# 2. Install deps
npm install

# 3. Run
npm start
```

## Extend it

- **API server** — expose `POST /jobs` that calls `queue.push()`
- **Dashboard** — poll `queue.stats()` and render job statuses
- **Rate limiting** — add a token bucket before `processJob()`
- **Job dependencies** — push child job inside a parent's handler
