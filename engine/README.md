# engine

Core mini-temporal workflow engine: Postgres state, Kafka dispatch, outbox relay, leased workers, and stale-task recovery.

Full project documentation lives in the [root README](../README.md).

## Quick start

```bash
bun install
bun run demo              # crash/recover lab in the browser, no Postgres/Kafka
bun run infra             # docker compose: Postgres + Kafka
bun run migrate
bun run outbox            # terminal 1
bun run worker            # terminal 2
bun run start-workflow    # terminal 3
```

Crash a worker after the activity succeeds:

```bash
CRASH_AFTER_ACTIVITY=true bun run worker
```

Then wait 30s for the lease and run:

```bash
bun src/recovery/recoverStaleTasks.ts
```

See [How to run](../README.md#how-to-run) for the rest.
