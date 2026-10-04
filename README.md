# mini-temporal

Workflow orchestration in the style of [Temporal](https://temporal.io), built with Bun, PostgreSQL, and Apache Kafka.

You define tasks and dependencies. PostgreSQL stores workflow state, task state, and an append-only event log. Kafka carries ready tasks to workers. A transactional outbox writes scheduling intent to Postgres before anything hits the bus, so a crash after commit does not lose work.

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
  ├── record task_attempt
  ├── execute activity (idempotent operation_id)
  ├── mark task COMPLETED
  ├── append TASK_COMPLETED to workflow_events
  ├── unblock dependent tasks → READY
  └── insert new outbox rows for newly ready tasks
```

The orchestrator never publishes to Kafka directly. It commits workflow state and outbox rows in one Postgres transaction. A relay polls unpublished rows and publishes them.

That closes the gap where a task is scheduled in the database but never reaches a worker.

## Demo

https://github.com/user-attachments/assets/9916d088-c80b-47bd-a9b0-a7363fc68a8c

Local browser demo (no Postgres/Kafka):

```bash
cd engine && bun run demo
```

Open `http://localhost:3456`, pick a preset, click Start workflow. The UI mirrors scheduling in `engine/src/index.ts` and worker logic in `engine/src/worker/worker.ts`.

## Project structure

```text
engine/
    src/
        index.ts              Start workflow, create tasks, deps, outbox
        db/client.ts            Postgres connection pool
        kafka/client.ts         Kafka producer and client config
        outbox/
            index.ts            Outbox relay loop
            publisher.ts        Poll outbox, publish, set published_at
        worker/
            worker.ts           Consume workflow-tasks, claim, execute
        migrations/
            001_initial.sql     Schema

    demo/
        serve.ts                Static file server
        index.html              Task graph and event log UI
        app.js                  In-browser simulation
        styles.css

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
   │                          ┌──────────────┴──────────────┐
   │                          ▼                             ▼
   │                     COMPLETED                       FAILED
   │                          │
   │                          └── unblock dependents → READY + outbox
```

Each task has a stable `operation_id` (`{workflowId}:{taskKey}`). Pass it to external activities as an idempotency key.

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

## Delivery semantics

Publication is at-least-once.

The relay can crash after Kafka acks but before `published_at` commits. The same row may publish twice.

Workers handle duplicates with a conditional claim:

```sql
UPDATE tasks
SET status = 'RUNNING', attempt_count = attempt_count + 1
WHERE id = $1 AND status = 'READY'
```

If status is not `READY`, the worker skips the message.

## Worker processing

1. Consume from `workflow-tasks`.
2. Claim the task (`READY` to `RUNNING`) in a transaction.
3. Insert a `task_attempts` row.
4. Run the activity (today a 1s sleep stub).
5. On success, mark attempt and task `COMPLETED`, append `TASK_COMPLETED`, unblock dependents, insert outbox rows for newly ready tasks.
6. On failure, mark attempt and task `FAILED`.

Kafka auto-commit is still default. Offset handling needs hardening before production.

## Database schema

See `engine/src/migrations/001_initial.sql`.

| Table | Purpose |
|-------|---------|
| `workflows` | Workflow instance metadata and status |
| `tasks` | Work units with status and `operation_id` |
| `task_dependencies` | DAG edges |
| `task_attempts` | Per-attempt execution history |
| `workflow_events` | Append-only log with per-workflow sequence |
| `outbox` | Task-ready events waiting for Kafka |

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

<!-- TODO: add docker-compose.yml -->

### 4. Postgres and Kafka

Manual setup for now.

```bash
docker run -d --name mini-temporal-pg \
  -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=admin \
  -e POSTGRES_DB=mini_temporal \
  -p 5432:5432 \
  postgres:16

docker run -d --name mini-temporal-kafka \
  -p 9092:9092 \
  apache/kafka:latest
```

Add `docker-compose.yml` when you want one command for infra.

### 5. Migrate

```bash
psql "$DATABASE_URL" -f src/migrations/001_initial.sql
```

### 6. Outbox relay

```bash
bun src/outbox/index.ts
```

### 7. Worker

```bash
bun src/worker/worker.ts
```

### 8. Start a workflow

```bash
bun src/index.ts
```

Runs the bundled order-processing example and prints `workflowId`.

### 9. Browser demo (no infra)

```bash
bun run demo
```

Open `http://localhost:3456`, pick a preset, click Start workflow.

## Testing

<!-- TODO: add tests -->

```bash
bun test
```

Planned integration coverage:

- workflow start creates tasks and initial outbox rows
- dependency unblocking after parent completion
- worker skips when task is not READY (duplicate delivery)
- relay retry after Kafka failure
- relay crash after publish (at-least-once)

## Reliability properties

- **Durable state.** Workflows, tasks, deps, and history live in Postgres and survive restarts.
- **Atomic scheduling.** Worker completion updates task state and inserts outbox rows in one transaction.
- **Transactional outbox.** Ready tasks stay in Postgres if relay or Kafka is down.
- **Task claiming.** `UPDATE ... WHERE status = 'READY'` blocks double execution on duplicate messages.
- **Idempotent activities.** Stable `operation_id` per task for external calls.
- **Append-only history.** `workflow_events` stores `WORKFLOW_STARTED`, `TASK_READY`, `TASK_COMPLETED` with monotonic sequence per workflow.

## Current limitations

- Activities are a sleep stub, not real integrations.
- No HTTP API. Start workflows via CLI or `startWorkflow()` directly.
- No `WORKFLOW_COMPLETED` in the DB path yet.
- No retry policy, backoff, or DLQ for failed tasks.
- Relay publishes one row at a time, no `FOR UPDATE SKIP LOCKED`.
- No `docker-compose.yml`.
- Kafka offsets not tuned for production.
- No metrics or tracing.
- Browser demo is separate from Postgres and Kafka.

## Planned work

- HTTP API to start and query workflows
- `docker-compose.yml` for Postgres, Kafka, and app processes
- Workflow completion and terminal failure states
- Retry with exponential backoff
- Dead-letter topic for poison tasks
- Real activities with idempotency keys
- Concurrent relay with row locking
- Recorded demo video linked from README
- `bun test` integration suite
- GitHub Actions CI
- OpenTelemetry across orchestrator, relay, and worker

## Documentation (planned)

```text
docs/adr/001-transactional-outbox.md
docs/adr/002-task-claiming-and-idempotency.md
docs/adr/003-workflow-event-sourcing.md
docs/postmortems/duplicate-kafka-delivery.md
docs/postmortems/outbox-relay-crash.md
```

## Contact

- GitHub: [@vinaybadgujar102](https://github.com/vinaybadgujar102)
- LinkedIn: [Vinay Badgujar](https://www.linkedin.com/in/badgujarvinay/)
- Email: [vinaybadgujar8@gmail.com](mailto:vinaybadgujar8@gmail.com)
