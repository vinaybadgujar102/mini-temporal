import { expect, test } from "bun:test";
import {
  MiniTemporalEngine,
  PRESETS,
  eventSentence,
  labPhase,
  isRetryable,
  calculateRetryDelay,
  defaultRetryPolicy,
} from "./app.js";

test("dual claim: loser skips, winner completes", async () => {
  const engine = new MiniTemporalEngine(PRESETS.onboarding, {
    activityMs: 120,
    outboxMs: 10,
    leaseMs: 2000,
    heartbeatMs: 80,
  });
  engine.start();
  await engine.run();

  expect(engine.workflowStatus).toBe("COMPLETED");
  expect(engine.events.filter((e) => e.eventType === "CLAIM_WON")).toHaveLength(
    3,
  );
  expect(
    engine.events.filter((e) => e.eventType === "CLAIM_SKIPPED"),
  ).toHaveLength(3);
  expect(engine.attempts).toHaveLength(3);
});

test("manual crash then recover: retry races A and B", async () => {
  const engine = new MiniTemporalEngine(PRESETS.onboarding, {
    activityMs: 500,
    outboxMs: 10,
    leaseMs: 350,
    heartbeatMs: 80,
  });

  let crashed = false;
  let recovered = false;
  engine.start();
  await engine.run(() => {
    if (!crashed && engine.tasks.get("verify")?.status === "RUNNING") {
      engine.crashWorker("verify");
      crashed = true;
    }
    if (crashed && !recovered && engine.staleAttempts().length > 0) {
      engine.recover();
      recovered = true;
    }
  });

  expect(crashed).toBe(true);
  expect(recovered).toBe(true);
  const types = engine.events.map((e) => e.eventType);
  expect(types).toContain("WORKER_CRASHED");
  expect(types).toContain("TASK_RECOVERY");
  expect(types).toContain("CLAIM_SKIPPED");
  expect(engine.workflowStatus).toBe("COMPLETED");

  const verifyAttempts = engine.attempts.filter((a) => a.taskKey === "verify");
  expect(verifyAttempts).toHaveLength(2);
  expect(verifyAttempts[0].status).toBe("TIMED_OUT");
  expect(verifyAttempts[1].status).toBe("COMPLETED");
}, 15_000);

test("lab: verify waits for crash, first claim is A only, recover races A and B", async () => {
  const engine = new MiniTemporalEngine(PRESETS.onboarding, {
    activityMs: 80,
    outboxMs: 10,
    leaseMs: 280,
    heartbeatMs: 60,
    dualClaim: false,
    gateCrashKey: "verify",
    unlockDualOnRecover: true,
    runKind: "lab",
  });

  engine.start();
  const done = engine.run();

  const wait = async (pred, ms = 8000) => {
    const start = Date.now();
    while (!pred()) {
      if (Date.now() - start > ms) throw new Error("timeout");
      await new Promise((r) => setTimeout(r, 20));
    }
  };

  await wait(() => engine.tasks.get("verify")?.status === "RUNNING");
  await new Promise((r) => setTimeout(r, 200));
  expect(engine.tasks.get("verify")?.status).toBe("RUNNING");
  expect(engine.tasks.get("verify")?.attemptCount).toBe(1);
  expect(
    engine.events.filter((e) => e.eventType === "CLAIM_SKIPPED"),
  ).toHaveLength(0);

  engine.crashWorker("verify");
  expect(engine.labCrashUsed).toBe(true);
  expect(engine.crashWorker("verify")).toBe(false);

  await wait(() => engine.staleAttempts().length > 0);
  expect(labPhase(engine)).toBe("recover");
  engine.recover();
  expect(engine.dualClaim).toBe(true);

  await done;
  expect(engine.workflowStatus).toBe("COMPLETED");
  expect(labPhase(engine)).toBe("lab-done");

  const verifyAttempts = engine.attempts.filter((a) => a.taskKey === "verify");
  expect(verifyAttempts).toHaveLength(2);
  expect(
    engine.events.filter((e) => e.eventType === "CLAIM_SKIPPED").length,
  ).toBeGreaterThan(0);
}, 15_000);

test("queue recover before lease dies, run() recovers", async () => {
  const engine = new MiniTemporalEngine(PRESETS.onboarding, {
    activityMs: 80,
    outboxMs: 10,
    leaseMs: 250,
    heartbeatMs: 60,
    dualClaim: false,
    gateCrashKey: "verify",
    unlockDualOnRecover: true,
    runKind: "lab",
  });

  engine.start();
  const done = engine.run();
  const wait = async (pred, ms = 8000) => {
    const start = Date.now();
    while (!pred()) {
      if (Date.now() - start > ms) throw new Error("timeout");
      await new Promise((r) => setTimeout(r, 20));
    }
  };

  await wait(() => engine.tasks.get("verify")?.status === "RUNNING");
  engine.crashWorker("verify");
  expect(engine.queueRecover()).toBe("queued");
  await done;
  expect(engine.workflowStatus).toBe("COMPLETED");
}, 15_000);

test("eventSentence names the crash", () => {
  expect(
    eventSentence({
      eventType: "WORKER_CRASHED",
      data: { taskKey: "verify", consumerId: "A" },
    }),
  ).toBe("Worker A crashed on Verify Email. Lease still live.");
});

test("labPhase idle vs finished", () => {
  expect(labPhase(null)).toBe("idle");
  expect(labPhase(null, Date.now(), true)).toBe("lab-done");
});

test("labPhase stays watch after crash while retry is READY", () => {
  const engine = new MiniTemporalEngine(PRESETS.onboarding, {
    dualClaim: false,
    gateCrashKey: "verify",
    unlockDualOnRecover: true,
    runKind: "lab",
  });
  engine.start();
  engine.labCrashUsed = true;
  const verify = engine.tasks.get("verify");
  verify.status = "READY";
  verify.attemptCount = 1;
  expect(labPhase(engine)).toBe("watch");
});

test("retryable error schedules backoff then completes", async () => {
  expect(isRetryable({ code: "503" })).toBe(true);
  expect(calculateRetryDelay(1, { ...defaultRetryPolicy, jitterFactor: 0 })).toBe(
    400,
  );

  const engine = new MiniTemporalEngine(PRESETS.onboarding, {
    activityMs: 80,
    outboxMs: 10,
    leaseMs: 2000,
    heartbeatMs: 60,
    dualClaim: false,
    runKind: "free",
    retryPolicy: { ...defaultRetryPolicy, maxAttempts: 3, initialDelayMs: 50 },
  });

  engine.start();
  const done = engine.run(() => {
    const verify = engine.tasks.get("verify");
    const attempt = engine.attempts.find(
      (a) => a.taskKey === "verify" && a.status === "RUNNING",
    );
    if (
      verify?.status === "RUNNING" &&
      attempt &&
      verify.attemptCount === 1 &&
      !engine.failRequested.has(attempt.id)
    ) {
      engine.failWorker("verify", { code: "503", message: "upstream busy" });
    }
  });

  await done;
  expect(engine.workflowStatus).toBe("COMPLETED");
  expect(
    engine.events.some((e) => e.eventType === "TASK_RETRY_SCHEDULED"),
  ).toBe(true);
  const verifyAttempts = engine.attempts.filter((a) => a.taskKey === "verify");
  expect(verifyAttempts.length).toBeGreaterThanOrEqual(2);
  expect(verifyAttempts[0].status).toBe("FAILED");
}, 15_000);

test("cancel stops open tasks", async () => {
  const engine = new MiniTemporalEngine(PRESETS.onboarding, {
    activityMs: 800,
    outboxMs: 10,
    leaseMs: 2000,
    heartbeatMs: 60,
    dualClaim: false,
    gateCrashKey: "verify",
    runKind: "free",
  });

  engine.start();
  const done = engine.run();
  const wait = async (pred, ms = 8000) => {
    const start = Date.now();
    while (!pred()) {
      if (Date.now() - start > ms) throw new Error("timeout");
      await new Promise((r) => setTimeout(r, 20));
    }
  };

  await wait(() => engine.tasks.get("verify")?.status === "RUNNING");
  expect(engine.cancelWorkflow().outcome).toBe("CANCELLED");
  await done;

  expect(engine.workflowStatus).toBe("CANCELLED");
  expect(engine.tasks.get("verify")?.status).toBe("CANCELLED");
  expect(engine.tasks.get("profile")?.status).toBe("CANCELLED");
  expect(engine.events.map((e) => e.eventType)).toContain("WORKFLOW_CANCELLED");
}, 15_000);

test("saga: permanent fail after charge refunds payment", async () => {
  const engine = new MiniTemporalEngine(PRESETS.checkout, {
    activityMs: 60,
    outboxMs: 10,
    leaseMs: 2000,
    heartbeatMs: 50,
    dualClaim: true,
    runKind: "saga",
  });

  engine.start();
  await engine.run(() => {
    if (
      engine.workflowStatus === "RUNNING" &&
      engine.tasks.get("email")?.status === "RUNNING"
    ) {
      engine.failWorker("email", {
        code: "FATAL",
        message: "confirmation rejected",
      });
    }
  });

  expect(engine.workflowStatus).toBe("FAILED");
  expect(engine.tasks.get("charge")?.status).toBe("COMPLETED");
  expect(engine.tasks.get("email")?.status).toBe("FAILED");
  expect(engine.compensations).toHaveLength(1);
  expect(engine.compensations[0].status).toBe("COMPLETED");
  expect(engine.compensations[0].compensationType).toBe("REFUND_PAYMENT");

  const stored = engine.paymentStore.get(
    engine.tasks.get("charge").operationId,
  );
  expect(stored?.status).toBe("REFUNDED");
  expect(engine.events.map((e) => e.eventType)).toContain(
    "WORKFLOW_COMPENSATION_STARTED",
  );
  expect(engine.events.map((e) => e.eventType)).toContain(
    "COMPENSATION_COMPLETED",
  );
}, 15_000);
