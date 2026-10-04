# engine

Core mini-temporal workflow engine — Postgres state, Kafka dispatch, outbox relay, and worker.

Full project documentation lives in the [root README](../README.md).

## Quick start

```bash
bun install
bun run demo          # browser demo — no Postgres/Kafka needed
bun src/index.ts      # start example workflow (requires DB + relay + worker)
```

See [How to Run](../README.md#how-to-run) for the full stack setup.
