import { Counter, Gauge, Histogram, Registry } from "prom-client";

export const metricsRegistry = new Registry();

metricsRegistry.setDefaultLabels({
  service: process.env.SERVICE_NAME ?? "mini-temporal",
  component: "outbox",
});

export const outboxMessagesProcessed = new Counter({
  name: "outbox_messages_processed_total",
  help: "Outbox message processing attempts by outcome.",
  labelNames: ["outcome"] as const,
  registers: [metricsRegistry],
});

export const outboxMessageDuration = new Histogram({
  name: "outbox_message_processing_duration_seconds",
  help: "Time spent processing an individual outbox message.",
  labelNames: ["outcome"] as const,
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
  registers: [metricsRegistry],
});

export const outboxBatchSize = new Histogram({
  name: "outbox_batch_size",
  help: "Number of pending outbox messages selected per batch.",
  buckets: [0, 1, 5, 10, 25, 50, 75, 100],
  registers: [metricsRegistry],
});

export const outboxBatchFailure = new Counter({
  name: "outbox_batch_failures_total",
  help: "Outbox batch failures before individual message processing.",
  registers: [metricsRegistry],
});
export const outboxPendingMessages = new Gauge({
  name: "outbox_pending_messages",
  help: "Current number of unpublished outbox messages.",
  registers: [metricsRegistry],
});

export const outboxOldestPendingAge = new Gauge({
  name: "outbox_oldest_pending_age_seconds",
  help: "Age in seconds of the oldest unpublished outbox message.",
  registers: [metricsRegistry],
});

export const outboxActionablePendingMessages = new Gauge({
  name: "outbox_actionable_pending_messages",
  help: "Unpublished outbox messages eligible for publication.",
  registers: [metricsRegistry],
});

export const outboxOldestActionablePendingAge = new Gauge({
  name: "outbox_oldest_actionable_pending_age_seconds",
  help: "Age of the oldest unpublished message eligible for publication.",
  registers: [metricsRegistry],
});

export const outboxRetainedTerminalMessages = new Gauge({
  name: "outbox_retained_terminal_messages",
  help: "Unpublished outbox messages associated with non-running or missing workflows.",
  registers: [metricsRegistry],
});
