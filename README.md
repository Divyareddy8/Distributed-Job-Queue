# Distributed Job Queue (Node.js + Redis)

## Overview

This project implements a distributed job queue system using Node.js and Redis. It demonstrates how to build a reliable background processing system with support for concurrency, retries, delayed execution, crash recovery, and dead-letter handling.

The system is inspired by production-grade tools like Bull and Sidekiq, but implemented from scratch to understand the underlying concepts.

---

## Architecture

The system consists of three main components:

### 1. Job

A Job represents a unit of work. Each job contains:

* Unique ID (UUID)
* Type (used to select a handler)
* Payload (input data)
* Priority
* Retry metadata
* Status tracking

### 2. Queue

The Queue is responsible for:

* Storing jobs in Redis
* Managing job states
* Handling retries and delays
* Recovering stuck jobs
* Maintaining a dead-letter queue

### 3. Worker

The Worker:

* Continuously pulls jobs from the queue
* Executes them using registered handlers
* Manages concurrency
* Handles failures and retries

---

## Redis Data Structures

| Purpose           | Key           | Type       |
| ----------------- | ------------- | ---------- |
| Active Queue      | jq:queue      | Sorted Set |
| Delayed Jobs      | jq:delayed    | Sorted Set |
| Processing Jobs   | jq:processing | Sorted Set |
| Dead Letter Queue | jq:dlq        | List       |
| Job Data          | jq:data:<id>  | String     |
| Job Metadata      | jq:meta:<id>  | Hash       |

---

## Features

* Priority-based job scheduling
* Configurable concurrency
* Retry with exponential backoff
* Delayed job execution
* Crash recovery using visibility timeout
* Dead-letter queue for failed jobs
* Graceful shutdown
* Idempotent job pushing

---

## Installation

### 1. Clone the repository

```bash
git clone <your-repo-url>
cd Distributed-Job-Queue
```

### 2. Install dependencies

```bash
npm install
```

---

## Running Redis (Docker)

You can run Redis without installing it locally using Docker:

```bash
docker-compose up -d
```

This will start Redis on:

```
localhost:6379
```

---

## Running the Project

```bash
npm start
```

---

## Example Flow

1. Jobs are pushed into the queue
2. Worker picks jobs based on priority
3. Jobs are processed concurrently
4. On failure:

   * Job is retried with exponential backoff
   * If retries exceed limit, it moves to the dead-letter queue
5. If worker crashes:

   * Jobs are recovered using visibility timeout

---

## Handlers

Handlers define how each job type is processed.

Example:

```js
const handlers = {
  async sendEmail({ to }) {
    // simulate email sending
  },
  async processPayment({ amount }) {
    // simulate payment processing
  }
};
```

---

## Job Lifecycle

```
PENDING → PROCESSING → COMPLETED
                    ↘ FAILED → RETRY → DELAYED
                                   ↘ DEAD (DLQ)
```

---

## Crash Recovery

If a worker crashes while processing a job:

* The job remains in the processing set
* After the visibility timeout expires
* It is automatically moved back to the queue

---

## Dead Letter Queue (DLQ)

Jobs are moved to DLQ when:

* They exceed maximum retry attempts
* No handler exists for the job type

You can inspect DLQ using:

```js
queue.getDLQJobs()
```

---

## Configuration

Worker options:

```js
const worker = new Worker(queue, handlers, {
  concurrency: 3,
  pollInterval: 300,
  visibilityTimeout: 5000
});
```

---

## Project Structure

```
.
├── src
│   ├── Job.js
│   ├── Queue.js
│   └── Worker.js
├── index.js
├── docker-compose.yml
├── package.json
└── README.md
```

---

## Example Output

```
[Worker] started concurrency=3
[Queue] pushed job ...
[Worker] start job ...
[Worker] done job ...
[Worker] retry job ...
[DLQ] buried job ...
```

---

## Future Improvements

* REST API for job submission
* Web dashboard for monitoring
* Multiple worker instances
* Rate limiting
* Job grouping and batching
* Metrics and logging integration

