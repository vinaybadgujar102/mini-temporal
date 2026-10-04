# mini-temporal

A durable workflow orchestration engine inspired by [Temporal](https://temporal.io), built with Bun, PostgreSQL, and Apache Kafka.

The project explores how to run multi-step workflows with task dependencies, durable state, append-only history, and asynchronous worker execution — without relying on a hosted orchestration platform.

PostgreSQL holds workflow and task state plus publication intent. Kafka delivers ready tasks to workers. A transactional outbox bridges the two so scheduling survives process crashes.

## Tech Stack

- TypeScript
- Bun
- PostgreSQL
- Apache Kafka
- KafkaJS
- `pg` (node-postgres)

## Current Architecture

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

The orchestrator does not publish to Kafka directly.

Workflow state and outbox rows are written in the same PostgreSQL transaction. A separate relay polls unpublished outbox rows and publishes them to Kafka.

This removes the failure window where a task could be scheduled in the database but never reach a worker.

## Demo

<!-- TODO: Replace placeholders once you record the demo -->

### Live demo

| Resource | Link |
|----------|------|
| Interactive browser demo (local) | `http://localhost:3456` after `bun run demo` |
| Deployed demo | _[Add URL — e.g. Vercel / Fly / Railway]_ |
| Demo repository branch | _[Add branch or tag if you snapshot a stable demo]_ |

### Screenshots and recordings

<!-- Drop assets into ./assets/ and uncomment when ready -->

<!-- ![Order processing workflow — task graph and event log](./assets/demo-order-processing.png) -->

<!-- ![User onboarding workflow — dependency unblocking](./assets/demo-user-onboarding.png) -->

<!-- ![GIF — charge + reserve run in parallel, email waits for both](./assets/demo-workflow.gif) -->

| Asset | Description | Status |
|-------|-------------|--------|
| `assets/demo-order-processing.png` | Order workflow DAG with all tasks completed | _Placeholder_ |
| `assets/demo-user-onboarding.png` | Linear onboarding chain | _Placeholder_ |
| `assets/demo-workflow.gif` | End-to-end run with event log scrolling | _Placeholder_ |
| `assets/demo-architecture.mp4` | Walkthrough: Postgres → outbox → Kafka → worker | _Placeholder_ |

### What the demo shows

The browser demo (`engine/demo/`) simulates the same scheduling and dependency logic as the real engine:

- **Order processing** — `charge` and `reserve` run in parallel; `email` waits for both
- **User onboarding** — linear chain: verify → profile → welcome
- Live **task graph** (pending → ready → running → completed)
- Append-only **event log** mirroring `workflow_events`

It does not connect to Postgres or Kafka; use it to explain the model before running the full stack.

## Project Structure

```text
engine/
    src/
        index.ts              Workflow start: create workflow, tasks, deps, outbox
        db/client.ts            PostgreSQL connection pool
        kafka/client.ts         Kafka producer / client config
        outbox/
            index.ts            Outbox relay loop
            publisher.ts        Poll outbox, publish to Kafka, mark published_at
        worker/
            worker.ts           Consume workflow-tasks, claim & execute tasks
        migrations/
            001_initial.sql     Schema: workflows, tasks, events, outbox

    demo/
        serve.ts                Static file server for browser demo
        index.html              Demo UI — task graph + event log
        app.js                  In-browser engine simulation
        styles.css              Demo styling

    package.json
    tsconfig.json
```

## Workflow Start Flow

Starting a workflow follows this flow:

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

Tasks with dependencies start as `PENDING`. Tasks with no dependencies start as `READY` and get an outbox row immediately.

### Example definition

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

## Task Lifecycle

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

Each task has a stable `operation_id` (`{workflowId}:{taskKey}`) used as an idempotency key when calling external activities.

## Transactional Outbox

The orchestrator writes scheduling intent to PostgreSQL first:

```text
PostgreSQL transaction
    │
    ├── update task state / workflow_events
    └── insert outbox row (published_at IS NULL)
    │
    ▼
COMMIT
```

The outbox relay polls:

```sql
SELECT id, workflow_id, task_id, event_type, payload
FROM outbox
WHERE published_at IS NULL
ORDER BY created_at
LIMIT 100
```

After successful Kafka delivery, the relay sets `published_at`.

If Kafka publication fails, the row stays unpublished and will be retried on the next poll.

## Delivery Semantics

The outbox provides **at-least-once publication** to Kafka.

A relay can crash after Kafka acknowledges delivery but before `published_at` commits. The same outbox row may be published again.

Workers mitigate duplicate delivery by claiming tasks with a conditional update:

```sql
UPDATE tasks
SET status = 'RUNNING', attempt_count = attempt_count + 1
WHERE id = $1 AND status = 'READY'
```

If the task is no longer `READY`, the worker skips execution.

## Worker Processing

The worker:

1. consumes messages from `workflow-tasks`
2. claims the task (`READY` → `RUNNING`) in a transaction
3. inserts a `task_attempts` row
4. executes the activity (currently a simulated 1s delay)
5. on success: marks attempt and task `COMPLETED`, appends `TASK_COMPLETED` event
6. checks dependent tasks — when all dependencies are `COMPLETED`, moves them to `READY` and inserts outbox rows
7. on failure: marks attempt and task `FAILED`

Kafka auto-commit behavior is not yet customized; offset management is an area for future hardening.

## Database Schema

Core tables (see `engine/src/migrations/001_initial.sql`):

| Table | Purpose |
|-------|---------|
| `workflows` | Workflow instance metadata and status |
| `tasks` | Logical units of work with status and `operation_id` |
| `task_dependencies` | DAG edges between tasks |
| `task_attempts` | Per-attempt execution history |
| `workflow_events` | Append-only event log with monotonic sequence |
| `outbox` | Durable task-ready events pending Kafka publish |

## How to Run

### 1. Clone the repository

```bash
git clone https://github.com/vinaybadgujar102/mini-temporal.git
cd mini-temporal/engine
```

### 2. Install dependencies

```bash
bun install
```

### 3. Configure environment

Create `engine/.env`:

```env
DATABASE_URL=postgresql://admin:admin@localhost:5432/mini_temporal
```

<!-- TODO: Add docker-compose.yml for Postgres + Kafka -->

### 4. Start PostgreSQL and Kafka

_Infra setup is manual for now. Suggested local stack:_

```bash
# PostgreSQL — example with Docker
docker run -d --name mini-temporal-pg \
  -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=admin \
  -e POSTGRES_DB=mini_temporal \
  -p 5432:5432 \
  postgres:16

# Kafka — example with Docker (adjust image/version as needed)
docker run -d --name mini-temporal-kafka \
  -p 9092:9092 \
  apache/kafka:latest
```

_Create `docker-compose.yml` here when you wire up a one-command stack._

### 5. Initialize the database

```bash
psql "$DATABASE_URL" -f src/migrations/001_initial.sql
```

### 6. Start the outbox relay

```bash
bun src/outbox/index.ts
```

### 7. Start the worker

```bash
bun src/worker/worker.ts
```

### 8. Start a workflow

```bash
bun src/index.ts
```

This runs the bundled order-processing example and prints the new `workflowId`.

### 9. Run the browser demo (no infra required)

```bash
bun run demo
```

Open `http://localhost:3456`, pick a workflow preset, and click **Start workflow**.

## Testing

<!-- TODO: Add tests -->

```bash
# Planned
bun test
```

Integration coverage to add:

- workflow start creates correct tasks and initial outbox rows
- dependency unblocking after parent task completion
- worker skip when task is not READY (duplicate Kafka delivery)
- outbox relay retry after Kafka failure
- relay crash after publish (at-least-once semantics)

## Key Reliability Properties

### Durable workflow state

Workflows, tasks, dependencies, and history survive process restarts in PostgreSQL.

### Atomic scheduling

Task state changes and outbox inserts for newly ready tasks happen in one transaction inside the worker completion path.

### Transactional outbox

Ready tasks are not lost if the relay or Kafka is temporarily unavailable.

### Task claiming

Workers use conditional `UPDATE ... WHERE status = 'READY'` to avoid double execution under duplicate delivery.

### Idempotent activities

Each task carries a stable `operation_id` intended for idempotent external side effects.

### Append-only history

`workflow_events` records `WORKFLOW_STARTED`, `TASK_READY`, and `TASK_COMPLETED` with per-workflow sequence numbers.

## Current Limitations

This project intentionally leaves several areas open:

- activities are simulated (sleep), not real external integrations
- no HTTP API for starting workflows — CLI / direct function call only
- no workflow completion detection or `WORKFLOW_COMPLETED` event in the DB path
- no retry policy, backoff, or dead-letter queue for failed tasks
- outbox relay publishes rows one-by-one without `FOR UPDATE SKIP LOCKED`
- no `docker-compose.yml` for one-command local setup
- Kafka consumer offset strategy not tuned for production
- no observability (metrics, tracing, dashboards)
- browser demo is disconnected from the real Postgres + Kafka stack

## Future Improvements

- HTTP API to start and query workflows
- `docker-compose.yml` for Postgres + Kafka + app processes
- workflow completion and failure terminal states
- configurable retry with exponential backoff
- dead-letter topic for permanently failed tasks
- real activity implementations with idempotency keys
- outbox relay concurrency with row locking
- record demo assets under `assets/` and link deployed demo URL
- unit and integration tests with `bun test`
- GitHub Actions CI
- OpenTelemetry tracing across orchestrator, relay, and worker

## Documentation

<!-- TODO: Add ADRs and postmortems as the design evolves -->

Architecture decisions (planned):

```text
docs/adr/001-transactional-outbox.md
docs/adr/002-task-claiming-and-idempotency.md
docs/adr/003-workflow-event-sourcing.md
```

Failure analysis (planned):

```text
docs/postmortems/duplicate-kafka-delivery.md
docs/postmortems/outbox-relay-crash.md
```

## Let's Connect

If you'd like to discuss workflow engines, distributed systems, or event-driven architecture:

- GitHub: [@vinaybadgujar102](https://github.com/vinaybadgujar102)
- LinkedIn: _[Add your LinkedIn URL]_
- Email: _[Add your email]_
