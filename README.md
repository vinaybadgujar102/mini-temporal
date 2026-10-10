# mini-temporal

Workflow orchestration in the style of [Temporal](https://temporal.io), built with Bun, PostgreSQL, and Apache Kafka.

You define tasks and dependencies. PostgreSQL stores workflow state, task state, and an append-only event log. Kafka carries ready tasks to workers. A transactional outbox writes scheduling intent to Postgres before anything hits the bus, so a crash after commit does not lose work.

Workers claim with a lease and a fencing token. A dead owner cannot complete after recovery has already reassigned the task. Charge uses a stable `operation_id` so a retry after a crash does not create a second payment.

No hosted orchestration platform required.

## Tech stack

- TypeScript
- Bun
- PostgreSQL
- Apache Kafka
- KafkaJS
- `pg` (node-postgres)

## Architecture

```text
                         PostgreSQL
                      ┌─────────────────────┐
                      │ workflows           │
Client / CLI          │ tasks               │
  │                   │ task_dependencies   │
  │                   │ task_attempts       │
  │                   │ workflow_events     │
  │                   │ outbox              │
  │                   │ payment_operations  │
  │                   └─────────────────────┘
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
  │
  ▼
Kafka: workflow-tasks
  │
  ▼
Worker (engine/src/worker)
  │
  ├── claim task (READY → RUNNING)
  ├── insert task_attempt (worker_id, fencing_token, lease_until)
  ├── heartbeat every 10s (extends 30s lease)
  ├── execute activity (CHARGE_PAYMENT is idempotent)
  ├── complete only if worker + token + RUNNING + lease still valid
  ├── append TASK_COMPLETED
  ├── unblock dependents → READY + outbox
  └── mark workflow COMPLETED when no open tasks remain
```

The orchestrator never publishes to Kafka directly. It commits workflow state and outbox rows in one Postgres transaction. A relay polls unpublished rows and publishes them.

If a worker dies after claim, `recoverStaleTasks` waits until `lease_until` is past, marks the attempt `TIMED_OUT`, sets the task back to `READY`, appends `TASK_RECOVERY`, and inserts a new outbox row.

## Demo

https://github.com/user-attachments/assets/9916d088-c80b-47bd-a9b0-a7363fc68a8c

The browser demo is a crash lab. It does not talk to Postgres or Kafka. It runs the same claim, lease, heartbeat, recover, and dual-consumer race as the engine.

```bash
cd engine && bun run demo
```

Open `http://localhost:3456`, click **Start lab**, then:

1. Wait until Verify Email is running.
2. Click **Crash worker**. The task stays `RUNNING`. Heartbeats stop.
3. **Recover stale** queues until the lease hits 0, then requeues the task.
4. Two consumers race the new claim. Only one wins.

**Start over** resets the lab. **Start workflow** is free play with the onboarding or order preset.

To crash the real worker after a successful activity, before `completeAttempt`:

```bash
CRASH_AFTER_ACTIVITY=true bun run worker
```

## Project structure

```text
engine/
    src/
        index.ts                   Start workflow, tasks, deps, outbox
        db/client.ts               Postgres pool
        kafka/client.ts            Kafka producer
        outbox/
            index.ts               Relay loop
            publisher.ts           Poll outbox, publish, set published_at
        worker/
            worker.ts              Consume, claim, heartbeat, fence, complete
        recovery/
            recoverStaleTasks.ts   Expired leases → READY + outbox
        external/
            paymentService.ts      Idempotent charge by operation_id
        migrations/
            001_initial.sql
            002_add_fencing_token.sql
            003_add_payment_operations.sql

    demo/
        serve.ts
        index.html                 Crash / recover lab UI
        app.js                     In-browser engine
        app.test.ts                Lab tests
        styles.css

    docker-compose.yml             Postgres 16 + Kafka
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
   │                                         │ worker claims
   │                                         ▼
   │                                      RUNNING
   │                                         │
   │              lease expired              │
   │              recoverStaleTasks          │
   │                          ┌──────────────┴──────────────┐
   │                          ▼                             ▼
   │                     COMPLETED                       FAILED
   │                          │
   │                          └── unblock dependents → READY + outbox
   │                              last task → WORKFLOW_COMPLETED
```

Each task has a stable `operation_id` (`{workflowId}:{taskKey}`). Pass it to external activities as an idempotency key.

A recovered attempt is `TIMED_OUT`. The task itself goes back to `READY` with a higher `attempt_count`. The next claim gets a new fencing token.

## Leases and fencing

Claim is one transaction:

```sql
UPDATE tasks
SET status = 'RUNNING', attempt_count = attempt_count + 1
WHERE id = $1 AND status = 'READY'
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

`(task_id, fencing_token)` is unique on `task_attempts`.

## Recovery

`bun src/recovery/recoverStaleTasks.ts` (one shot) selects `RUNNING` tasks whose current attempt has `lease_until < NOW()`, then in the same transaction:

- marks the attempt `TIMED_OUT`
- sets the task `READY`
- appends `TASK_RECOVERY`
- inserts a `TASK_READY` outbox row

Kafka then delivers the retry. A new worker claims with a new token.

## Idempotent charge

`CHARGE_PAYMENT` calls `chargePayment(operationId, amount)`. Rows live in `payment_operations` with `idempotency_key` as the primary key.

```sql
INSERT INTO payment_operations (idempotency_key, transaction_id, amount, status)
VALUES ($1, $2, $3, 'SUCCEEDED')
ON CONFLICT (idempotency_key) DO NOTHING
```

A crash after charge and before `completeAttempt` can recover and run the activity again. The second insert is a no-op. The existing `transaction_id` is returned.

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

After Kafka accepts the message, the relay sets `published_at`. If publish fails, the row stays unpublished and the next poll retries it.

Publication is at-least-once. The relay can crash after Kafka acks but before `published_at` commits. The same row may publish twice. The claim filter above is what keeps that from running the activity twice on the same attempt.

## Database schema

Migrations in `engine/src/migrations/`.

| Table | Purpose |
|-------|---------|
| `workflows` | Workflow instance metadata and status |
| `tasks` | Work units with status and `operation_id` |
| `task_dependencies` | DAG edges |
| `task_attempts` | Per-attempt history, `worker_id`, `fencing_token`, `lease_until` |
| `workflow_events` | Append-only log with per-workflow sequence |
| `outbox` | Task-ready events waiting for Kafka |
| `payment_operations` | Charge results keyed by `operation_id` |

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

### 4. Postgres and Kafka

```bash
bun run infra
```

That is `docker compose up -d` for Postgres 16 on `5432` and Kafka on `9092`.

### 5. Migrate

```bash
bun run migrate
```

Runs `001_initial.sql`, `002_add_fencing_token.sql`, and `003_add_payment_operations.sql`.

### 6. Outbox relay

```bash
bun run outbox
```

### 7. Worker

```bash
bun run worker
```

### 8. Recover stale tasks (optional)

After killing a worker mid-activity, wait for the 30 second lease, then:

```bash
bun src/recovery/recoverStaleTasks.ts
```

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

`demo/app.test.ts` covers the crash lab: crash then recover, Verify Email waiting for a crash, recover queued until the lease dies, two consumers racing one claim.

There is no integration suite yet against live Postgres and Kafka.

## Reliability properties

- **Durable state.** Workflows, tasks, deps, attempts, and history live in Postgres and survive restarts.
- **Atomic scheduling.** Completion updates task state, events, and outbox rows in one transaction.
- **Transactional outbox.** Ready tasks stay in Postgres if relay or Kafka is down.
- **Task claiming.** `UPDATE ... WHERE status = 'READY'` skips duplicate messages.
- **Leases.** A running attempt expires if heartbeats stop.
- **Fencing.** Complete and fail match `worker_id` and `fencing_token`. A stale owner cannot finish after recovery.
- **Idempotent charge.** `payment_operations.idempotency_key` is the task `operation_id`.
- **Append-only history.** Events include `WORKFLOW_STARTED`, `TASK_READY`, `TASK_COMPLETED`, `TASK_RECOVERY`, and `WORKFLOW_COMPLETED`.

## Current limitations

- Only `CHARGE_PAYMENT` talks to an idempotent store. Other activities are a 1s sleep.
- No HTTP API. Start workflows via CLI or `startWorkflow()` directly.
- Failed tasks stay `FAILED`. No retry policy, backoff, or DLQ.
- Relay publishes one row at a time, no `FOR UPDATE SKIP LOCKED`.
- Kafka offsets use KafkaJS defaults, not production settings.
- No metrics or tracing.
- Browser demo is separate from Postgres and Kafka.
- Recovery is a one-shot script, not a loop.

## Planned work

- HTTP API to start and query workflows
- Retry with exponential backoff for `FAILED` tasks
- Dead-letter topic for poison tasks
- Concurrent relay with row locking
- Postgres/Kafka integration tests
- GitHub Actions CI
- OpenTelemetry across orchestrator, relay, and worker
- Background recovery loop

## Contact

- GitHub: [@vinaybadgujar102](https://github.com/vinaybadgujar102)
- LinkedIn: [Vinay Badgujar](https://www.linkedin.com/in/badgujarvinay/)
- Email: [vinaybadgujar8@gmail.com](mailto:vinaybadgujar8@gmail.com)
