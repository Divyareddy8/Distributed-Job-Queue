# Distributed Job Queue

A **production-grade**, Redis-backed distributed job queue built from scratch in Node.js.

Implements the core primitives that underpin systems like BullMQ, Sidekiq, and Celery — priority scheduling, exponential-backoff retries, visibility-timeout crash recovery, and a dead-letter queue — without any job-queue framework dependency.

---

## Features

| Capability | Detail |
|---|---|
| **Priority scheduling** | Jobs are stored in a Redis sorted set keyed by `-priority`. Higher priority = dequeued first. |
| **Concurrent workers** | Configurable concurrency; each job runs in an isolated async fiber. |
| **Exponential backoff** | Failed jobs retry after `2ⁿ` seconds (capped at 60 s), where `n` = retry count. |
| **Visibility timeout** | Popped jobs must be ack'd within a deadline; crashed workers auto-recover. |
| **Dead-letter queue (DLQ)** | Jobs exceeding `maxRetries`, or with no registered handler, are buried in the DLQ for inspection. |
| **DLQ replay** | Individual dead jobs can be replayed with a single API call. |
| **Graceful shutdown** | `worker.stop()` drains in-flight jobs before closing; handles `SIGINT`/`SIGTERM`. |
| **Pipeline writes** | Multi-key Redis mutations use pipelined commands for atomicity and throughput. |
| **Idempotent push** | Re-pushing a job by id updates its score rather than creating a duplicate. |

---

## Architecture

```
Producer
  │
  │  queue.push(job)
  ▼
┌─────────────────────────────────────────────────────────────┐
│  Redis                                                      │
│                                                             │
│  jq:queue       (sorted set, score = -priority)            │
│  jq:delayed     (sorted set, score = runAt ms)             │
│  jq:processing  (sorted set, score = visibilityTimeout ms) │
│  jq:dlq         (list, newest-first)                       │
│  jq:data:<id>   (string, serialised Job JSON)              │
│  jq:meta:<id>   (hash, status + type for fast lookup)      │
└─────────────────────────────────────────────────────────────┘
  │                │                  │
  │ popForProcessing│ promoteDelayed   │ reclaimStuckJobs
  ▼                ▼                  ▼
Worker ──► handler(payload)
          │
          ├── success ──► setStatus(COMPLETED) ──► ack + cleanup
          │
          └── failure ──► retries < maxRetries ──► requeueWithDelay
                      └── retries >= maxRetries ──► sendToDLQ
```

### Key design decisions

**Why sorted sets for the queue?**
`ZPOPMIN` is O(log N) and atomic — perfect for a priority queue with concurrent consumers.

**Why a separate `jq:processing` set?**
Tracking in-flight jobs with a timeout score lets a second process (or the same worker after a restart) reclaim jobs whose workers crashed, without any external heartbeat service.

**Why pipeline writes?**
Updating `jq:data` and `jq:meta` in a single pipeline round-trip halves latency on status changes and avoids partial-write inconsistency.

---

## Quick start

### Prerequisites

- Node.js ≥ 18
- Docker (for Redis) — or a local Redis 6+ install

```bash
# 1. Clone & install
git clone https://github.com/your-username/distributed-job-queue.git
cd distributed-job-queue
npm install

# 2. Start Redis
docker-compose up -d

# 3. Run the demo
npm start
```

### Expected output

```
[Redis] ✅ connected
[Queue] 🧹 flushed stale keys
[Worker] 🚀 started  concurrency=3

🚀 System started

[Queue] ➕ pushed  ...  type=sendEmail       priority=1
[Queue] ➕ pushed  ...  type=processPayment  priority=10
...
[Worker] ⚙️  start  ...  attempt=1
  → payment → $99.99
[Worker] ✅ done   ...
...
💥 Simulating worker crash…
[Worker] 🛑 stopped

♻️  Restarting worker…
[Worker] 🚀 started  concurrency=3
...
──────────── Stats ────────────
┌─────────────┬───────┐
│   (index)   │ Values│
├─────────────┼───────┤
│   queued    │   0   │
│   delayed   │   0   │
│  processing │   0   │
│    dead     │   1   │
└─────────────┴───────┘

──────────── Dead-Letter Queue ────────────
  id=...  type=unknownJob  retries=0
```

---

## API reference

### `Queue`

```js
const queue = new Queue(redisConfig?)

await queue.connect()                          // open connection
await queue.push(job)                          // enqueue
await queue.popForProcessing(timeoutMs?)       // dequeue + mark in-flight
await queue.ack(job, cleanup?)                 // remove from processing set
await queue.setStatus(job, status)             // persist status + updatedAt
await queue.getStatus(jobId)                   // fast hash lookup
await queue.requeueWithDelay(job, delayMs)     // schedule retry
await queue.promoteDelayed()                   // move ready delayed jobs → queue
await queue.reclaimStuckJobs()                 // re-queue timed-out jobs
await queue.sendToDLQ(job)                     // bury a dead job
await queue.getDLQJobs()                       // inspect DLQ
await queue.replayFromDLQ(jobId)               // retry a dead job
await queue.stats()                            // { queued, delayed, processing, dead }
await queue.flush()                            // delete all jq:* keys (tests)
await queue.close()                            // graceful quit
```

### `Worker`

```js
const worker = new Worker(queue, handlers, options?)

worker.start()    // begin polling
await worker.stop() // drain in-flight jobs, then stop
```

**Options**

| Option | Default | Description |
|---|---|---|
| `concurrency` | `3` | Max simultaneous jobs |
| `pollInterval` | `500` ms | Sleep when queue is empty |
| `visibilityTimeout` | `5000` ms | Max time before a job is reclaimed |

### `Job`

```js
new Job({
  type,                  // required — matches a handler key
  payload,               // required — passed verbatim to the handler
  priority?,             // default 0  (higher = more urgent)
  maxRetries?,           // default 3
})
```

---

## Project structure

```
distributed-job-queue/
├── src/
│   ├── Job.js        — Data model, serialisation, status enum
│   ├── Queue.js      — Redis operations (producer + consumer primitives)
│   └── Worker.js     — Polling loop, concurrency control, retry logic
├── index.js          — Demo script (push jobs, simulate crash, print stats)
├── docker-compose.yml
├── package.json
└── README.md
```

---

## Running tests

```bash
npm test
```

Tests use an in-process Redis mock (or a real Redis on a non-default port) and cover:
- Job serialisation round-trip
- Priority ordering
- Retry + backoff
- DLQ routing
- Visibility-timeout recovery

---

## License

MIT
