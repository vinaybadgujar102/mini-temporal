import { expect, test } from "bun:test";
import {
  MiniTemporalEngine,
  PRESETS,
  eventSentence,
  labPhase,
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
