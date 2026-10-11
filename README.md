# mini-temporal

Workflow orchestration in the style of [Temporal](https://temporal.io), built with Bun, PostgreSQL, and Apache Kafka.

You define tasks and dependencies. PostgreSQL stores workflow state, task state, an append-only event log, and saga compensation intents. Kafka carries ready tasks to workers. A transactional outbox writes scheduling intent to Postgres before anything hits the bus, so a crash after commit does not lose work.

Workers claim with a lease and a fencing token. A dead owner cannot complete after recovery has already reassigned the task. Retryable failures schedule backoff via `next_retry_at`. Permanent failures cancel open tasks and start reverse-order compensation (for example refund after charge). Charge and refund use stable idempotency keys so a retry after a crash does not create a second payment or double refund.

No hosted orchestration platform required.

## Tech stack

- TypeScript
- Bun
- PostgreSQL
- Apache Kafka
- KafkaJS
- `pg` (node-postgres)
- Pino
- prom-client
- Prometheus / Grafana (optional, via docker compose)

## Architecture

```text
                         PostgreSQL
                      ┌─────────────────────────────┐
                      │ workflows                   │
Client / CLI          │ tasks (+ next_retry_at)     │
  │                   │ task_dependencies           │
  │                   │ task_attempts               │
  │                   │ workflow_events             │
  │                   │ outbox                      │
  │                   │ payment_operations          │
  │                   │ compensation_operations     │
  │                   └─────────────────────────────┘
  │                           ▲
  ▼                           │
startWorkflow() ─── single DB transaction
  │
  │ workflow + tasks + deps + history + outbox
  │
  ▼
PostgreSQL
  │
  │ unpublished outbox rows (TASK_READY)
  ▼
Outbox Relay (engine/src/outbox)
  │  metrics on :9464 (/metrics, /healthz)
  ▼
Kafka: workflow-tasks
  │
  ▼
Worker (engine/src/worker)
  │
  ├── claim task (READY → RUNNING, respect next_retry_at)
  ├── insert task_attempt (worker_id, fencing_token, lease_until)
  ├── heartbeat every 10s (extends 30s lease)
  ├── execute activity (CHARGE_PAYMENT is idempotent)
  ├── complete only if worker + token + RUNNING + lease still valid
  ├── append TASK_COMPLETED
  ├── unblock dependents → READY + outbox
  └── mark workflow COMPLETED when no open tasks remain

On activity failure (fenced):
  ├── retryable → READY + next_retry_at + TASK_RETRY_SCHEDULED
  │                 publishDueRetries() later inserts outbox
  └── permanent  → FAIL task, CANCEL open tasks, startCompensation()
                    workflow → COMPENSATING (or FAILED if nothing to undo)
                    compensationWorker claims reverse-DAG ops (leases + fencing)
                    REFUND_PAYMENT is idempotent
                    all comps done → workflow FAILED
```

The orchestrator never publishes to Kafka directly. It commits workflow state and outbox rows in one Postgres transaction. A relay polls unpublished rows and publishes them. Rows for non-`RUNNING` workflows are discarded instead of published.

If a worker dies after claim, `recoverStaleTasks` waits until `lease_until` is past, marks the attempt `TIMED_OUT`, sets the task back to `READY`, appends `TASK_RECOVERY`, and inserts a new outbox row. Stale compensation claims are requeued by `recoverStaleCompensations`.

## Demo

https://github.com/user-attachments/assets/9916d088-c80b-47bd-a9b0-a7363fc68a8c

The browser demo is a crash / retry / saga lab. It does not talk to Postgres or Kafka. It runs the same claim, lease, heartbeat, recover, dual-consumer race, retry backoff, cancel, and compensation paths as the engine.

```bash
cd engine && bun run demo
```

Open `http://localhost:3456`, then:

**Crash lab** — click **Crash lab**:

1. Wait until Verify Email is running.
2. Click **Crash worker**. The task stays `RUNNING`. Heartbeats stop.
3. **Recover stale** queues until the lease hits 0, then requeues the task.
4. Two consumers race the new claim. Only one wins.

**Saga lab** — click **Saga lab** to fail confirmation after charge and watch `REFUND_PAYMENT` run.

**Fail task** / **Cancel** exercise retryable failure and workflow cancel on a live run.

**Start over** resets the lab. Free play uses the onboarding or order preset.

To crash the real worker after a successful activity, before `completeAttempt`:

```bash
CRASH_AFTER_ACTIVITY=true bun run worker
```

## Project structure

```text
engine/
    src/
        index.ts                   Start workflow CLI (order example)
        db/client.ts               Postgres pool
        kafka/client.ts            Kafka producer
        outbox/
            index.ts               Relay loop + metrics server
            publisher.ts           Poll outbox, publish, set published_at
        worker/
            worker.ts              Consume, claim, heartbeat, fence, complete
            claimTask.ts           READY → RUNNING under workflow lock
            compensationWorker.ts  Claim / run / finish saga compensations
            compensationLease.ts   Compensation lease renewal
        workflow/
            startWorkflow.ts       Create workflow + ready outbox rows
            taskCompletion.ts      Fenced complete + unblock dependents
            startCompensation.ts   RUNNING → COMPENSATING + comp rows
            cancelWorkflow.ts      Cancel open tasks, drop unpublished outbox
            updateWorkflowStatus.ts
            appendWorkflowEvent.ts
        retry/
            retryPolicy.ts         Max attempts, exponential backoff + jitter
            handleAttemptFailure.ts Retry vs permanent fail → compensation
            publishDueRetries.ts   Due READY retries → outbox
        recovery/
            recoverStaleTasks.ts   Expired task leases → READY + outbox
            recoverStaleCompensations.ts  Expired comp leases → READY
        external/
            paymentService.ts      Idempotent charge by operation_id
            refundService.ts       Idempotent refund by compensation key
        observability/
            logger.ts              Pino
            metrics.ts             Prometheus counters / gauges / histograms
            metricsServer.ts       Bun.serve /metrics + /healthz
        monitoring/
            prometheus.yml
            alerts.yml
        migrations/
            001_initial.sql
            002_add_fencing_token.sql
            003_add_payment_operations.sql
            004_add_next_retry_at.sql
            005_saga_compensation.sql
            006_payment_opearations.sql
            007_add_lease_fencing_compensation_operations.sql

    demo/
        serve.ts
        index.html                 Crash / retry / saga lab UI
        app.js                     In-browser engine
        app.test.ts                Lab tests
        styles.css

    docker-compose.yml             Postgres 16 + Kafka + Prometheus + Grafana
    package.json
    tsconfig.json
```

## Starting a workflow

```text
startWorkflow(definition)
  │
  ├── BEGIN transaction
  │
  ├── INSERT workflow (status RUNNING)
  ├── INSERT tasks (PENDING or READY based on dependencies)
  ├── INSERT task_dependencies
  ├── INSERT WORKFLOW_STARTED into workflow_events
  │
  ├── For each task with no dependencies:
  │       ├── INSERT TASK_READY into workflow_events
  │       └── INSERT TASK_READY into outbox
  │
  └── COMMIT → return workflowId
```

Tasks with dependencies start as `PENDING`. Tasks with none start as `READY` and get an outbox row in the same transaction.

### Example

```typescript
await startWorkflow({
  type: "ORDER_PROCESSING",
  name: "Process Order",
  version: 1,
  tasks: [
    { id: "charge", name: "Charge Payment", type: "CHARGE_PAYMENT" },
    { id: "reserve", name: "Reserve Inventory", type: "RESERVE_INVENTORY" },
    {
      id: "email",
      name: "Send Confirmation",
      type: "SEND_EMAIL",
      dependsOn: ["charge", "reserve"],
    },
  ],
});
```

## Task lifecycle

```text
PENDING ──(all dependencies COMPLETED)──► READY
   ▲                                         │
   │                                         │ worker claims (next_retry_at due)
   │                                         ▼
   │                                      RUNNING
   │                                         │
   │              lease expired              │
   │              recoverStaleTasks          │
   │                          ┌──────────────┼──────────────┐
   │                          ▼              ▼              ▼
   │                     COMPLETED    retryable fail   permanent fail
   │                          │         READY +            FAILED
   │                          │         next_retry_at        │
   │                          │              │               ├── CANCEL other open tasks
   │                          │              │               └── startCompensation()
   │                          └── unblock dependents
   │                              last task → WORKFLOW_COMPLETED

cancelWorkflow() on RUNNING:
  workflow CANCELLED, open tasks CANCELLED, unpublished TASK_READY outbox deleted
```

Each task has a stable `operation_id` (`{workflowId}:{taskKey}`). Pass it to external activities as an idempotency key.

Default retry policy: 5 attempts, 1s initial delay, 30s max, 20% jitter. Retryable error codes: `503`, `429`, `ECONNRESET`, `ETIMEDOUT`.

A recovered attempt is `TIMED_OUT`. The task itself goes back to `READY` with a higher `attempt_count`. The next claim gets a new fencing token.

### Compensation lifecycle

On permanent failure, completed tasks with a registered compensator (today: `CHARGE_PAYMENT` → `REFUND_PAYMENT`) get a `compensation_operations` row. Workflow status becomes `COMPENSATING`. The compensation worker claims in reverse dependency order (dependents first), with a 60s lease and fencing token. When every compensation is `COMPLETED`, the workflow is marked `FAILED`. If nothing needs undoing, the workflow fails immediately.

## Leases and fencing

Claim is one transaction (workflow locked first; skip if `next_retry_at` is still in the future):

```sql
UPDATE tasks
SET status = 'RUNNING', attempt_count = attempt_count + 1
WHERE id = $1
  AND status = 'READY'
  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
```

If that updates a row, the worker inserts a `task_attempts` row with `worker_id`, `fencing_token = attempt_count`, and `lease_until = now() + 30s`. Duplicate Kafka messages skip when the task is no longer `READY`.

Heartbeat (every 10 seconds):

```sql
UPDATE task_attempts
SET lease_until = NOW() + INTERVAL '30 seconds',
    last_heartbeat_at = NOW()
WHERE id = $1
  AND worker_id = $2
  AND fencing_token = $3
  AND status = 'RUNNING'
  AND lease_until > NOW()
```

Complete requires the same owner and a live lease:

```sql
UPDATE task_attempts
SET status = 'COMPLETED', result = $1, completed_at = NOW()
WHERE id = $2
  AND task_id = $3
  AND worker_id = $4
  AND fencing_token = $5
  AND status = 'RUNNING'
  AND lease_until > NOW()
```

Zero rows means a late worker. The result is dropped.

`(task_id, fencing_token)` is unique on `task_attempts`. Compensation ops use the same idea with `fencing_token` and `lease_expires_at` on `compensation_operations`.

## Recovery

`bun src/recovery/recoverStaleTasks.ts` (one shot) selects `RUNNING` tasks on `RUNNING` workflows whose current attempt has `lease_until < NOW()`, then in the same transaction:

- marks the attempt `TIMED_OUT`
- sets the task `READY`
- appends `TASK_RECOVERY`
- inserts a `TASK_READY` outbox row

Kafka then delivers the retry. A new worker claims with a new token.

`recoverStaleCompensations()` does the parallel path for compensation ops whose `lease_expires_at` has passed: status back to `READY`, `COMPENSATION_LEASE_EXPIRED` event.

## Idempotent charge

`CHARGE_PAYMENT` calls `chargePayment(operationId, amount)`. Rows live in `payment_operations` with `idempotency_key` as the primary key.

```sql
INSERT INTO payment_operations (idempotency_key, transaction_id, amount, status)
VALUES ($1, $2, $3, 'SUCCEEDED')
ON CONFLICT (idempotency_key) DO NOTHING
```

A crash after charge and before `completeAttempt` can recover and run the activity again. The second insert is a no-op. The existing `transaction_id` is returned.

`REFUND_PAYMENT` updates the same row to `REFUNDED` keyed by `refund_idempotency_key` (the compensation `idempotency_key`). A second refund with the same key is a no-op; a different key is rejected.

Other task types still sleep for 1 second.

## Transactional outbox

Scheduling intent lands in Postgres first.

```text
PostgreSQL transaction
    │
    ├── update task state / workflow_events
    └── insert outbox row (published_at IS NULL)
    │
    ▼
COMMIT
```

The relay polls:

```sql
SELECT id, workflow_id, task_id, event_type, payload
FROM outbox
WHERE published_at IS NULL
ORDER BY created_at
LIMIT 100
```

For each row it locks the workflow. Non-`RUNNING` `TASK_READY` rows are deleted. Otherwise it publishes to Kafka and sets `published_at`. If publish fails, the row stays unpublished and the next poll retries it.

Publication is two-phase: Kafka send, then `published_at`. The relay can crash after Kafka acks but before `published_at` commits. The same row may publish twice. The claim filter above is what keeps that from running the activity twice on the same attempt.

The outbox process also serves Prometheus metrics on `METRICS_PORT` (default `9464`).

## Database schema

Migrations in `engine/src/migrations/`.

| Table | Purpose |
|-------|---------|
| `workflows` | Workflow instance metadata and status (`RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED`, `COMPENSATING`) |
| `tasks` | Work units with status, `operation_id`, and `next_retry_at` |
| `task_dependencies` | DAG edges |
| `task_attempts` | Per-attempt history, `worker_id`, `fencing_token`, `lease_until` |
| `workflow_events` | Append-only log with per-workflow sequence |
| `outbox` | Task-ready events waiting for Kafka |
| `payment_operations` | Charge / refund results keyed by `operation_id` |
| `compensation_operations` | Saga undo intents with lease, fencing, and retry fields |

## How to run

### 1. Clone

```bash
git clone https://github.com/vinaybadgujar102/mini-temporal.git
cd mini-temporal/engine
```

### 2. Install

```bash
bun install
```

### 3. Environment

Create `engine/.env`:

```env
DATABASE_URL=postgresql://admin:admin@localhost:5432/mini_temporal
```

### 4. Postgres, Kafka, and monitoring

```bash
bun run infra
```

That is `docker compose up -d` for Postgres 16 on `5432`, Kafka on `9092`, Prometheus on `9090`, and Grafana on `3000` (admin/admin).

### 5. Migrate

```bash
bun run migrate
```

Runs `001` through `007` (initial schema, fencing, payments, retries, saga compensation, refund columns, compensation leases).

### 6. Outbox relay

```bash
bun run outbox
```

Serves `/metrics` and `/healthz` on port `9464` by default.

### 7. Worker

```bash
bun run worker
```

### 8. Recover stale tasks (optional)

After killing a worker mid-activity, wait for the 30 second lease, then:

```bash
bun src/recovery/recoverStaleTasks.ts
```

Due retries need `publishDueRetries()` (used from tests / demo loop; not a packaged CLI yet). Compensation reclaim uses `recoverStaleCompensations()` and `runCompensationOnce()` the same way.

### 9. Start a workflow

```bash
bun run start-workflow
```

Runs the bundled order-processing example and prints `workflowId`. Charge and reserve run in parallel. Email waits for both. When every task is `COMPLETED`, the workflow is marked `COMPLETED`.

### 10. Browser demo (no infra)

```bash
bun run demo
```

Open `http://localhost:3456`.

## Testing

```bash
cd engine && bun test
```

`demo/app.test.ts` covers the crash lab, retry backoff, cancel, and saga refund.

Integration tests under `src/workflow/` exercise completion, failure propagation, cancel, and compensation against Postgres when `DATABASE_URL` is set:

```bash
bun run test:completion
bun run test:failure
```

## Reliability properties

- **Durable state.** Workflows, tasks, deps, attempts, compensations, and history live in Postgres and survive restarts.
- **Atomic scheduling.** Completion updates task state, events, and outbox rows in one transaction.
- **Transactional outbox.** Ready tasks stay in Postgres if relay or Kafka is down. Obsolete rows for terminal workflows are dropped.
- **Task claiming.** `UPDATE ... WHERE status = 'READY'` skips duplicate messages.
- **Leases.** A running attempt expires if heartbeats stop (tasks and compensations).
- **Fencing.** Complete and fail match `worker_id` and `fencing_token`. A stale owner cannot finish after recovery.
- **Retry with backoff.** Retryable failures stay `READY` until `next_retry_at`; permanent failures start compensation.
- **Saga compensation.** Completed side effects get reverse-order undo with their own leases and idempotency keys.
- **Cancel.** `cancelWorkflow` stops open work and clears unpublished dispatch.
- **Idempotent charge / refund.** `payment_operations` keys charge and refund so retries do not double-apply.
- **Append-only history.** Events include `WORKFLOW_STARTED`, `TASK_READY`, `TASK_COMPLETED`, `TASK_RETRY_SCHEDULED`, `TASK_FAILED`, `TASK_RECOVERY`, `WORKFLOW_COMPENSATION_STARTED`, `COMPENSATION_COMPLETED`, `WORKFLOW_CANCELLED`, `WORKFLOW_FAILED`, and `WORKFLOW_COMPLETED`.
- **Outbox metrics.** Prometheus scrapes the relay; alerts cover backlog age and scrape health.

## Current limitations

- Only `CHARGE_PAYMENT` / `REFUND_PAYMENT` talk to an idempotent store. Other activities are a 1s sleep. Only charge has a registered compensator.
- No HTTP API. Start / cancel workflows via CLI or library calls.
- `publishDueRetries`, compensation worker, and compensation recovery are library entry points, not long-running packaged services.
- Relay publishes one row at a time, no `FOR UPDATE SKIP LOCKED`.
- Kafka offsets use KafkaJS defaults, not production settings.
- OpenTelemetry is not wired; metrics cover the outbox path only.
- Browser demo is separate from Postgres and Kafka.
- Task recovery is a one-shot script, not a loop.

## Planned work

- HTTP API to start, cancel, and query workflows
- Packaged retry publisher and compensation worker loops
- Dead-letter topic for poison tasks
- Concurrent relay with row locking
- Broader Postgres/Kafka CI in GitHub Actions
- OpenTelemetry across orchestrator, relay, and worker
- Background recovery loops for tasks and compensations
- More compensators (for example release inventory)

## Contact

- GitHub: [@vinaybadgujar102](https://github.com/vinaybadgujar102)
- LinkedIn: [Vinay Badgujar](https://www.linkedin.com/in/badgujarvinay/)
- Email: [vinaybadgujar8@gmail.com](mailto:vinaybadgujar8@gmail.com)
