# Distributed Job Queue v2

A production-grade, Redis-backed job queue with a REST API, live dashboard, specialised multi-workers, per-type rate limiting, priority queues, and full Kubernetes support.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Clients / Services                      │
└────────────────────┬────────────────────────────────────────────┘
                     │  POST /api/jobs
                     ▼
         ┌───────────────────────┐
         │   REST API (Express)   │  ← scales horizontally
         │   + Dashboard (SSE)   │
         └───────────┬───────────┘
                     │ push / ack / setStatus
                     ▼
         ┌───────────────────────┐
         │         Redis          │
         │  jq:queue:<type>  ← per-type sorted sets (score = -priority)
         │  jq:delayed           ← retry backlog
         │  jq:processing        ← visibility timeout tracking
         │  jq:dlq               ← dead-letter list
         │  jq:data:<id>         ← serialised Job
         │  jq:meta:<id>         ← lightweight status hash
         │  jq:rl:<type>         ← sliding-window rate limit
         └───────────┬───────────┘
                     │ popForProcessing (Lua atomic)
          ┌──────────┼──────────┐
          ▼          ▼          ▼
   ┌────────────┐ ┌──────────┐ ┌─────────────┐
   │  General   │ │  Email   │ │   Payment   │
   │  Worker    │ │  Worker  │ │   Worker    │
   │ (all types)│ │ email-   │ │ payment-    │
   │ concur=4   │ │ only     │ │ only        │
   │            │ │ concur=10│ │ concur=2    │
   └────────────┘ └──────────┘ └─────────────┘
```

## Quick start

### Local (no Docker)

```bash
# 1. Start Redis
docker run -p 6379:6379 redis:7-alpine

# 2. Install deps
npm install

# 3. Run the demo (API + workers in one process)
npm start
# → Dashboard at http://localhost:3000
```

### Docker Compose (recommended for dev)

```bash
npm run docker:up
# Services:  redis, api (port 3000), worker-general, worker-email, worker-payment
# Dashboard: http://localhost:3000

npm run docker:logs    # tail all logs
npm run docker:down    # stop + remove volumes
```

### Run workers by role

```bash
# General worker (handles any type)
npm run worker

# Email-only worker
npm run worker:email

# Payment-only worker
npm run worker:payment
```

---

## REST API

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/jobs` | Submit a job |
| `GET`  | `/api/jobs/recent` | Last 20 pushed jobs |
| `GET`  | `/api/jobs/:id` | Job detail / status |
| `DELETE` | `/api/jobs/:id` | Cancel a PENDING job |
| `GET`  | `/api/jobs/dlq/list` | DLQ entries |
| `POST` | `/api/jobs/dlq/:id/replay` | Re-queue a DLQ job |
| `DELETE` | `/api/jobs/dlq/purge` | Clear entire DLQ |
| `GET`  | `/api/stats` | Queue stats (overall + per-type) |
| `GET`  | `/api/events` | SSE stream (live stats) |
| `GET`  | `/health` | Liveness probe |
| `GET`  | `/ready` | Readiness probe (pings Redis) |

### Submit a job

```bash
curl -X POST http://localhost:3000/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "type":       "sendEmail",
    "payload":    { "to": "user@example.com", "subject": "Hello" },
    "priority":   5,
    "maxRetries": 3
  }'
```

---

## Rate Limiting

Rate limits are per job type, shared across all API replicas (state in Redis, sliding-window algorithm).

Configure via the `RATE_LIMITS` environment variable:

```json
{
  "sendEmail":      { "max": 30, "windowMs": 60000 },
  "processPayment": { "max": 20, "windowMs": 60000 }
}
```

When exceeded, the API returns `429 Too Many Requests` with a `Retry-After` header and `resetAfterMs` in the body.

---

## Priority Queues

Each job type has its own sorted set (`jq:queue:<type>`).  Workers pop from
their subscribed sets atomically via a Lua script that finds the globally
highest-priority job across all subscribed queues in a single round-trip.

Higher `priority` numbers are processed first (score = `-priority`).

---

## Kubernetes Deployment

```bash
# 1. Build & push the image
docker build -t YOUR_REGISTRY/distributed-job-queue:latest .
docker push YOUR_REGISTRY/distributed-job-queue:latest

# 2. Update image references in k8s/03-api.yaml and k8s/04-workers.yaml

# 3. Deploy
npm run k8s:apply
# or
kubectl apply -f k8s/

# 4. Check status
npm run k8s:status

# 5. Expose dashboard (if no Ingress yet)
kubectl port-forward svc/jq-api 3000:80 -n job-queue

# Scale workers manually
kubectl scale deployment jq-worker-email --replicas=10 -n job-queue

# Tear down
npm run k8s:delete
```

### HPA (auto-scaling)

The `k8s/05-hpa.yaml` manifest automatically scales workers up/down based on CPU.
Requires `metrics-server`:

```bash
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `127.0.0.1` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_PASSWORD` | _(empty)_ | Redis password |
| `PORT` | `3000` | API server port |
| `RATE_LIMITS` | `{}` | JSON rate-limit config |
| `WORKER_TYPES` | _(all)_ | Comma-separated types |
| `WORKER_CONCURRENCY` | `3` | Parallel jobs per worker |
| `WORKER_ID` | `w-<PID>` | Display name in logs |
| `POLL_INTERVAL_MS` | `300` | Idle poll interval |
| `VISIBILITY_TIMEOUT` | `5000` | Stuck-job reclaim window |

---

## File Structure

```
.
├── src/
│   ├── Job.js               Job model + Status enum
│   ├── Queue.js             Redis queue (push, pop, ack, rate-limit, DLQ, stats)
│   ├── Worker.js            Job dispatcher (concurrency, retry, backoff)
│   ├── api/
│   │   ├── server.js        Express app + SSE broadcaster
│   │   └── routes/
│   │       ├── jobs.js      Job CRUD + DLQ routes
│   │       └── stats.js     Stats endpoint
│   └── dashboard/
│       └── index.html       Live dashboard (SSE + fetch)
├── workers/
│   └── worker.js            Standalone worker process (env-configured)
├── k8s/
│   ├── 00-namespace.yaml
│   ├── 01-configmap.yaml
│   ├── 02-redis.yaml        StatefulSet + Service
│   ├── 03-api.yaml          Deployment + Service
│   ├── 04-workers.yaml      General / Email / Payment deployments
│   ├── 05-hpa.yaml          HPA for all deployments
│   └── 06-ingress.yaml      Nginx Ingress (with SSE annotations)
├── Dockerfile
├── docker-compose.yml
├── index.js                 Local demo / smoke-test
└── package.json
```
