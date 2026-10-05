/**
 * In-browser simulation of mini-temporal.
 * Manual crash, manual recoverStaleTasks, dual consumers racing one claim.
 */

export const PRESETS = {
  order: {
    type: "ORDER_FULFILLMENT",
    name: "Fulfill Order",
    version: 1,
    viewBox: "0 0 640 580",
    tasks: [
      { id: "validate", name: "Validate order", type: "VALIDATE_ORDER" },
      {
        id: "fraud",
        name: "Fraud check",
        type: "CHECK_FRAUD",
        dependsOn: ["validate"],
      },
      {
        id: "catalog",
        name: "Catalog lookup",
        type: "CHECK_CATALOG",
        dependsOn: ["validate"],
      },
      {
        id: "tax",
        name: "Calculate tax",
        type: "CALC_TAX",
        dependsOn: ["validate"],
      },
      {
        id: "charge",
        name: "Charge payment",
        type: "CHARGE_PAYMENT",
        dependsOn: ["fraud", "tax"],
      },
      {
        id: "reserve",
        name: "Reserve stock",
        type: "RESERVE_INVENTORY",
        dependsOn: ["catalog"],
      },
      {
        id: "pick",
        name: "Pick items",
        type: "PICK_ITEMS",
        dependsOn: ["charge", "reserve"],
      },
      {
        id: "pack",
        name: "Pack order",
        type: "PACK_ORDER",
        dependsOn: ["pick"],
      },
      {
        id: "ship",
        name: "Dispatch",
        type: "DISPATCH_SHIPMENT",
        dependsOn: ["pack"],
      },
      {
        id: "notify",
        name: "Notify buyer",
        type: "SEND_EMAIL",
        dependsOn: ["pack"],
      },
    ],
    layout: {
      validate: { x: 260, y: 16 },
      fraud: { x: 40, y: 110 },
      catalog: { x: 260, y: 110 },
      tax: { x: 480, y: 110 },
      charge: { x: 80, y: 220 },
      reserve: { x: 440, y: 220 },
      pick: { x: 260, y: 330 },
      pack: { x: 260, y: 420 },
      ship: { x: 150, y: 510 },
      notify: { x: 370, y: 510 },
    },
  },
  onboarding: {
    type: "USER_ONBOARDING",
    name: "Onboard User",
    version: 1,
    viewBox: "0 0 520 280",
    tasks: [
      { id: "verify", name: "Verify Email", type: "VERIFY_EMAIL" },
      {
        id: "profile",
        name: "Create Profile",
        type: "CREATE_PROFILE",
        dependsOn: ["verify"],
      },
      {
        id: "welcome",
        name: "Send Welcome",
        type: "SEND_WELCOME",
        dependsOn: ["profile"],
      },
    ],
    layout: {
      verify: { x: 200, y: 30 },
      profile: { x: 200, y: 120 },
      welcome: { x: 200, y: 210 },
    },
  },
};

const ACTIVITY_MS = 4200;
const OUTBOX_MS = 400;
const LEASE_MS = 5000;
const HEARTBEAT_MS = 1200;
const PAYMENT_AMOUNT = 4999;
const WAIT_SLICE = 80;

function uuid() {
  return crypto.randomUUID();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function shortId(id) {
  return id.slice(0, 8) + "…";
}

/** @typedef {{ id: string, name: string, type: string, dependsOn?: string[] }} WorkflowTask */
/** @typedef {{ type: string, name: string, version: number, viewBox: string, tasks: WorkflowTask[], layout: Record<string, {x:number,y:number}> }} Preset */

export class MiniTemporalEngine {
  /**
   * @param {Preset} definition
   * @param {{ dualClaim?: boolean, leaseMs?: number, heartbeatMs?: number, outboxMs?: number, activityMs?: number, gateCrashKey?: string | null, unlockDualOnRecover?: boolean, runKind?: "lab" | "free" }} [opts]
   */
  constructor(definition, opts = {}) {
    this.definition = definition;
    this.dualClaim = opts.dualClaim !== false;
    this.gateCrashKey = opts.gateCrashKey ?? null;
    this.unlockDualOnRecover = Boolean(opts.unlockDualOnRecover);
    this.runKind = opts.runKind ?? "free";
    this.labCrashUsed = false;
    this.leaseMs = opts.leaseMs ?? LEASE_MS;
    this.heartbeatMs = opts.heartbeatMs ?? HEARTBEAT_MS;
    this.outboxMs = opts.outboxMs ?? OUTBOX_MS;
    this.activityMs = opts.activityMs ?? ACTIVITY_MS;
    this.workflowId = uuid();
    this.workflowStatus = "RUNNING";
    this.sequence = 0;
    /** @type {{ sequence: number, eventType: string, data: object, at: Date }[]} */
    this.events = [];
    /** @type {Map<string, object>} */
    this.tasks = new Map();
    /** @type {Map<string, string[]>} */
    this.deps = new Map();
    /** @type {Map<string, string[]>} */
    this.dependents = new Map();
    /** @type {{ id: string, taskKey: string, payload: object }[]} */
    this.outbox = [];
    /** @type {object[]} */
    this.attempts = [];
    /** @type {Map<string, object>} */
    this.paymentStore = new Map();
    /** @type {object[]} */
    this.payments = [];
    /** @type {object[]} */
    this.zombies = [];
    /** @type {Set<string>} */
    this.crashRequested = new Set();
    /** @type {Map<string, ReturnType<typeof setInterval>>} */
    this.heartbeatTimers = new Map();
    /** @type {Record<string, string>} */
    this.consumers = { A: "idle", B: "idle" };
    this.running = false;
    this.recoverQueued = false;
  }

  now() {
    return Date.now();
  }

  /** @param {string} eventType @param {object} data */
  appendEvent(eventType, data) {
    this.sequence += 1;
    const entry = {
      sequence: this.sequence,
      eventType,
      data,
      at: new Date(),
    };
    this.events.push(entry);
    return entry;
  }

  setConsumer(id, state) {
    this.consumers[id] = state;
  }

  /** @param {WorkflowTask} taskDef */
  initTask(taskDef) {
    const hasDeps = taskDef.dependsOn?.length > 0;
    const status = hasDeps ? "PENDING" : "READY";
    const task = {
      id: uuid(),
      key: taskDef.id,
      name: taskDef.name,
      type: taskDef.type,
      operationId: `${this.workflowId}:${taskDef.id}`,
      status,
      attemptCount: 0,
      result: null,
      error: null,
    };
    this.tasks.set(taskDef.id, task);

    const depKeys = taskDef.dependsOn ?? [];
    this.deps.set(taskDef.id, depKeys);
    for (const dep of depKeys) {
      if (!this.dependents.has(dep)) this.dependents.set(dep, []);
      this.dependents.get(dep).push(taskDef.id);
    }
    return task;
  }

  start() {
    for (const taskDef of this.definition.tasks) {
      this.initTask(taskDef);
    }

    this.appendEvent("WORKFLOW_STARTED", {
      workflowType: this.definition.type,
      workflowVersion: this.definition.version,
      workflowName: this.definition.name,
    });

    for (const taskDef of this.definition.tasks) {
      if (taskDef.dependsOn?.length) continue;
      this.scheduleReady(taskDef.id);
    }
  }

  /** @param {string} taskKey */
  scheduleReady(taskKey) {
    const task = this.tasks.get(taskKey);
    if (!task || task.status !== "READY") return;

    this.appendEvent("TASK_READY", {
      taskKey,
      taskId: task.id,
      taskType: task.type,
    });

    this.outbox.push({
      id: uuid(),
      taskKey,
      payload: {
        workflowId: this.workflowId,
        taskId: task.id,
        taskKey,
        taskType: task.type,
        operationId: task.operationId,
      },
    });
  }

  async publishOutbox(onPublish) {
    while (this.running) {
      if (this.outbox.length === 0) {
        await sleep(80);
        continue;
      }
      const msg = this.outbox.shift();
      await sleep(this.outboxMs);
      if (!this.running) return;
      this.appendEvent("OUTBOX_PUBLISHED", {
        outboxId: msg.id,
        taskKey: msg.taskKey,
        topic: "workflow-tasks",
      });
      onPublish?.(msg);
    }
  }

  runningTaskKeys() {
    return [...this.tasks.values()]
      .filter((t) => {
        if (t.status !== "RUNNING") return false;
        const attempt = this.currentAttemptFor(t);
        return attempt && !this.crashRequested.has(attempt.id);
      })
      .map((t) => t.key);
  }

  currentAttemptFor(task) {
    return this.attempts.find(
      (a) => a.taskId === task.id && a.attemptNumber === task.attemptCount,
    );
  }

  staleAttempts() {
    const now = this.now();
    const out = [];
    for (const task of this.tasks.values()) {
      if (task.status !== "RUNNING") continue;
      const attempt = this.currentAttemptFor(task);
      if (
        attempt &&
        attempt.status === "RUNNING" &&
        attempt.leaseUntil <= now
      ) {
        out.push({ task, attempt });
      }
    }
    return out;
  }

  dyingAttempt(now = this.now()) {
    return (
      this.attempts.find(
        (a) =>
          this.crashRequested.has(a.id) &&
          a.status === "RUNNING" &&
          a.leaseUntil > now,
      ) ?? null
    );
  }

  canCrash(taskKey) {
    if (this.runKind === "lab" && this.labCrashUsed) return false;
    const running = this.runningTaskKeys();
    if (taskKey) {
      if (running.includes(taskKey)) return true;
      const task = this.tasks.get(taskKey);
      if (task?.status !== "RUNNING") return false;
      if (this.runKind === "lab") return !this.labCrashUsed;
      return true;
    }
    if (running.length > 0) return true;
    if (this.runKind === "lab" && this.gateCrashKey && !this.labCrashUsed) {
      return this.tasks.get(this.gateCrashKey)?.status === "RUNNING";
    }
    return false;
  }

  runningAttemptFor(task) {
    const current = this.currentAttemptFor(task);
    if (current?.status === "RUNNING") return current;
    return (
      [...this.attempts]
        .reverse()
        .find((a) => a.taskId === task.id && a.status === "RUNNING") ?? null
    );
  }

  queueRecover() {
    if (this.staleAttempts().length > 0) {
      this.recoverQueued = false;
      this.recover();
      return "recovered";
    }
    if (this.dyingAttempt()) {
      this.recoverQueued = true;
      return "queued";
    }
    return "noop";
  }

  flushQueuedRecover() {
    if (!this.recoverQueued) return false;
    if (this.staleAttempts().length === 0) return false;
    this.recoverQueued = false;
    this.recover();
    return true;
  }

  /**
   * Kill the worker on a RUNNING task. Stops heartbeats. Leaves the task
   * RUNNING until Recover stale.
   * @param {string} [taskKey]
   */
  crashWorker(taskKey) {
    const key = taskKey ?? this.runningTaskKeys()[0] ?? this.gateCrashKey;
    if (!key) return false;
    const task = this.tasks.get(key);
    if (!task || task.status !== "RUNNING") return false;
    const attempt = this.runningAttemptFor(task);
    if (!attempt || attempt.status !== "RUNNING") return false;
    if (this.crashRequested.has(attempt.id)) return false;
    if (this.runKind === "lab" && this.labCrashUsed) return false;

    this.crashRequested.add(attempt.id);
    this.labCrashUsed = true;
    this.stopHeartbeat(attempt.id);
    this.appendEvent("WORKER_CRASHED", {
      taskKey: task.key,
      attemptId: attempt.id,
      fencingToken: attempt.fencingToken,
      workerId: attempt.workerId,
      consumerId: attempt.consumerId,
    });
    if (attempt.consumerId) {
      this.setConsumer(attempt.consumerId, `dead on ${task.key}`);
    }
    return true;
  }

  /** Same as recoverStaleTasks.ts — only expired leases. */
  recover() {
    this.recoverQueued = false;
    if (this.unlockDualOnRecover) this.dualClaim = true;
    this.recoverStaleTasks();
    this.flushZombies();
    return this;
  }

  /** @param {string} taskKey @param {string} [consumerId] */
  claimTask(taskKey, consumerId) {
    const task = this.tasks.get(taskKey);
    if (!task || task.status !== "READY") {
      if (consumerId) {
        this.setConsumer(consumerId, `skip ${taskKey}`);
        this.appendEvent("CLAIM_SKIPPED", {
          taskKey,
          consumerId,
          reason: "NOT_READY",
        });
      }
      return null;
    }

    task.status = "RUNNING";
    task.attemptCount += 1;

    const fencingToken = task.attemptCount;
    const workerId = uuid();
    const now = this.now();
    const attempt = {
      id: uuid(),
      taskKey,
      taskId: task.id,
      attemptNumber: task.attemptCount,
      status: "RUNNING",
      consumerId: consumerId ?? null,
      workerId,
      fencingToken,
      leaseUntil: now + this.leaseMs,
      lastHeartbeatAt: now,
      result: null,
      error: null,
    };
    this.attempts.push(attempt);

    if (consumerId) this.setConsumer(consumerId, `claimed ${taskKey}`);

    this.appendEvent("CLAIM_WON", {
      taskKey,
      taskId: task.id,
      consumerId,
      workerId,
      fencingToken,
      attempt: attempt.attemptNumber,
    });

    this.appendEvent("TASK_RUNNING", {
      taskKey,
      taskId: task.id,
      attempt: attempt.attemptNumber,
      fencingToken,
      workerId,
      consumerId,
      operationId: task.operationId,
    });

    return { task, attempt };
  }

  heartbeat(attempt, workerId, fencingToken) {
    if (this.crashRequested.has(attempt.id)) return false;
    if (attempt.status !== "RUNNING") return false;
    if (attempt.workerId !== workerId) return false;
    if (attempt.fencingToken !== fencingToken) return false;
    if (attempt.leaseUntil <= this.now()) return false;
    const now = this.now();
    attempt.leaseUntil = now + this.leaseMs;
    attempt.lastHeartbeatAt = now;
    return true;
  }

  stopHeartbeat(attemptId) {
    const timer = this.heartbeatTimers.get(attemptId);
    if (timer) {
      clearInterval(timer);
      this.heartbeatTimers.delete(attemptId);
    }
  }

  chargePayment(operationId, amount) {
    const existing = this.paymentStore.get(operationId);
    if (existing) {
      this.payments.push({ ...existing, duplicate: true });
      return existing;
    }
    const payment = {
      operationId,
      transactionId: uuid(),
      amount,
      status: "SUCCEEDED",
    };
    this.paymentStore.set(operationId, payment);
    this.payments.push({ ...payment, duplicate: false });
    return payment;
  }

  async waitOrCrash(attempt, ms) {
    let left = ms;
    while (left > 0) {
      if (!this.running) return "stopped";
      if (this.crashRequested.has(attempt.id)) return "crashed";
      const slice = Math.min(WAIT_SLICE, left);
      await sleep(slice);
      left -= slice;
    }
    if (this.crashRequested.has(attempt.id)) return "crashed";
    return "ok";
  }

  async executeActivity(task, attempt) {
    if (this.gateCrashKey === task.key && attempt.attemptNumber === 1) {
      const aborted = await this.waitOrCrash(attempt, Number.POSITIVE_INFINITY);
      return { aborted: aborted === "ok" ? "stopped" : aborted, result: null };
    }

    const duration = this.activityMs;
    const work = Math.max(WAIT_SLICE, Math.floor(duration * 0.7));
    const tail = Math.max(0, duration - work);

    const first = await this.waitOrCrash(attempt, work);
    if (first !== "ok") return { aborted: first, result: null };

    let result;
    if (task.type === "CHARGE_PAYMENT") {
      result = this.chargePayment(task.operationId, PAYMENT_AMOUNT);
    } else {
      result = {
        transactionId: uuid(),
        operationId: task.operationId,
        success: true,
      };
    }

    const second = await this.waitOrCrash(attempt, tail);
    if (second !== "ok") return { aborted: second, result };

    return { aborted: null, result };
  }

  completeAttempt(task, attempt, workerId, fencingToken, result) {
    if (
      attempt.status !== "RUNNING" ||
      attempt.workerId !== workerId ||
      attempt.fencingToken !== fencingToken ||
      attempt.leaseUntil <= this.now()
    ) {
      return false;
    }

    attempt.status = "COMPLETED";
    attempt.result = result;

    if (task.status === "RUNNING") {
      task.status = "COMPLETED";
      task.result = result;
    }

    this.appendEvent("TASK_COMPLETED", {
      taskKey: task.key,
      taskId: task.id,
      attemptId: attempt.id,
      fencingToken,
      consumerId: attempt.consumerId,
      result,
    });

    if (attempt.consumerId) {
      this.setConsumer(attempt.consumerId, `done ${task.key}`);
    }

    this.unblockDependents(task.key);
    return true;
  }

  recoverStaleTasks() {
    for (const { task, attempt } of this.staleAttempts()) {
      attempt.status = "TIMED_OUT";
      attempt.error = { reason: "LEASE_EXPIRED" };
      task.status = "READY";

      this.appendEvent("TASK_RECOVERY", {
        taskId: task.id,
        taskKey: task.key,
        previousAttemptId: attempt.id,
        previousAttempt: attempt.attemptNumber,
        previousFencingToken: attempt.fencingToken,
      });

      this.scheduleReady(task.key);
    }
  }

  flushZombies() {
    const keep = [];
    for (const z of this.zombies) {
      if (z.attempt.status === "RUNNING" && z.attempt.leaseUntil > this.now()) {
        keep.push(z);
        continue;
      }
      const completed = this.completeAttempt(
        z.task,
        z.attempt,
        z.workerId,
        z.fencingToken,
        z.result,
      );
      if (!completed) {
        this.appendEvent("STALE_RESULT_REJECTED", {
          taskKey: z.task.key,
          attemptId: z.attempt.id,
          fencingToken: z.fencingToken,
          workerId: z.workerId,
          consumerId: z.attempt.consumerId,
        });
      }
    }
    this.zombies = keep;
  }

  async executeTask(taskKey, onTick, consumerId) {
    const claimed = this.claimTask(taskKey, consumerId);
    if (!claimed) {
      onTick?.();
      return;
    }
    const { task, attempt } = claimed;
    onTick?.();

    const hb = setInterval(() => {
      const alive = this.heartbeat(
        attempt,
        attempt.workerId,
        attempt.fencingToken,
      );
      if (!alive) this.stopHeartbeat(attempt.id);
      else onTick?.();
    }, this.heartbeatMs);
    this.heartbeatTimers.set(attempt.id, hb);

    try {
      const { aborted, result } = await this.executeActivity(task, attempt);
      if (!this.running) return;

      if (aborted === "crashed") {
        if (result) {
          this.zombies.push({
            task,
            attempt,
            workerId: attempt.workerId,
            fencingToken: attempt.fencingToken,
            result,
          });
        }
        onTick?.();
        return;
      }

      if (aborted || result == null) return;

      const completed = this.completeAttempt(
        task,
        attempt,
        attempt.workerId,
        attempt.fencingToken,
        result,
      );
      if (!completed) {
        this.appendEvent("STALE_RESULT_REJECTED", {
          taskKey: task.key,
          attemptId: attempt.id,
          fencingToken: attempt.fencingToken,
          workerId: attempt.workerId,
          consumerId: attempt.consumerId,
        });
      }
      onTick?.();
    } finally {
      this.stopHeartbeat(attempt.id);
    }
  }

  async dispatch(taskKey, onTick) {
    if (this.dualClaim) {
      await Promise.all([
        this.executeTask(taskKey, onTick, "A"),
        this.executeTask(taskKey, onTick, "B"),
      ]);
      return;
    }
    await this.executeTask(taskKey, onTick, "A");
  }

  /** @param {string} completedKey */
  unblockDependents(completedKey, onTick) {
    for (const dependentKey of this.dependents.get(completedKey) ?? []) {
      const task = this.tasks.get(dependentKey);
      if (!task || task.status !== "PENDING") continue;

      const deps = this.deps.get(dependentKey) ?? [];
      const allDone = deps.every(
        (d) => this.tasks.get(d)?.status === "COMPLETED",
      );
      if (!allDone) continue;

      task.status = "READY";
      this.scheduleReady(dependentKey);
      onTick?.();
    }
  }

  checkWorkflowComplete(onTick) {
    const allDone = [...this.tasks.values()].every(
      (t) => t.status === "COMPLETED",
    );
    if (allDone && this.workflowStatus === "RUNNING") {
      this.workflowStatus = "COMPLETED";
      this.appendEvent("WORKFLOW_COMPLETED", { workflowId: this.workflowId });
      onTick?.();
    }
  }

  hasOpenWork(inflightSize, workerQueueLength) {
    if (this.outbox.length > 0 || workerQueueLength > 0 || inflightSize > 0) {
      return true;
    }
    if (this.zombies.length > 0) return true;
    return [...this.tasks.values()].some(
      (t) =>
        t.status === "READY" ||
        t.status === "RUNNING" ||
        t.status === "PENDING",
    );
  }

  async run(onTick) {
    this.running = true;

    const workerQueue = [];
    const inflight = new Set();
    const enqueue = (msg) => workerQueue.push(msg);
    const publisher = this.publishOutbox((msg) => enqueue(msg));

    while (this.running) {
      while (workerQueue.length > 0 && this.running) {
        const msg = workerQueue.shift();
        const p = this.dispatch(msg.taskKey, onTick).finally(() =>
          inflight.delete(p),
        );
        inflight.add(p);
      }

      this.checkWorkflowComplete(onTick);
      this.flushQueuedRecover();

      if (!this.hasOpenWork(inflight.size, workerQueue.length)) break;

      onTick?.();
      await sleep(80);
    }

    this.running = false;
    await Promise.allSettled([...inflight]);
    await publisher;
  }

  stop() {
    this.running = false;
    for (const attemptId of [...this.heartbeatTimers.keys()]) {
      this.stopHeartbeat(attemptId);
    }
  }
}

export function taskTitle(key) {
  for (const preset of Object.values(PRESETS)) {
    const task = preset.tasks.find((t) => t.id === key);
    if (task) return task.name;
  }
  return key;
}

export function eventSentence(ev) {
  const d = ev.data ?? {};
  const name = d.taskKey ? taskTitle(d.taskKey) : "";
  switch (ev.eventType) {
    case "WORKFLOW_STARTED":
      return `Workflow started (${d.workflowName}).`;
    case "TASK_READY":
      return `${name} is ready.`;
    case "OUTBOX_PUBLISHED":
      return `${name} was sent to workers.`;
    case "CLAIM_WON":
      return `Consumer ${d.consumerId ?? "?"} claimed ${name}.`;
    case "CLAIM_SKIPPED":
      return `Consumer ${d.consumerId ?? "?"} skipped ${name} — already claimed.`;
    case "TASK_RUNNING":
      return `${name} is running.`;
    case "WORKER_CRASHED":
      return `Worker ${d.consumerId ?? "?"} crashed on ${name}. Lease still live.`;
    case "TASK_RECOVERY":
      return `Lease expired. ${name} is ready again.`;
    case "TASK_COMPLETED":
      return `${name} completed.`;
    case "WORKFLOW_COMPLETED":
      return "Workflow finished.";
    case "STALE_RESULT_REJECTED":
      return `Late result from ${name} was rejected.`;
    default:
      return ev.eventType;
  }
}

/** @param {MiniTemporalEngine | null} engine */
export function labPhase(engine, now = Date.now(), finishedLab = false) {
  if (!engine) return finishedLab ? "lab-done" : "idle";
  if (engine.runKind !== "lab") {
    return engine.workflowStatus === "COMPLETED" ? "free-done" : "free";
  }
  if (engine.workflowStatus === "COMPLETED") return "lab-done";
  if (engine.dyingAttempt(now)) return "wait-lease";
  if (engine.staleAttempts().length) return "recover";
  const verify = engine.tasks.get("verify");
  if (verify?.status === "RUNNING" && verify.attemptCount === 1) return "crash";
  if ((verify?.attemptCount ?? 0) >= 2) return "watch";
  if (engine.tasks.size > 0) return "queued";
  return "start";
}

const NARRATION = {
  idle: "Start the lab. Verify Email will run and wait until you crash its worker.",
  queued: "Verify Email is queued. Worker A will claim it.",
  crash: "Verify Email will not finish by itself. Press Crash worker (or the pulsing node).",
  "wait-lease": "Worker is dead. Recover is armed — it runs when the lease hits 0. Click it now to queue.",
  recover: "Lease expired. Press Recover stale. A and B then race the retry.",
  watch: "Retry is in flight. One consumer wins, the other skips. Rest of the graph runs to done.",
  "lab-done": "Lab done. Run it again, or start a free workflow and crash any node.",
  free: "Free play. Crash a running node, wait out the lease, then Recover stale.",
  "free-done": "Workflow finished. Start another, switch workflow, or run the lab again.",
};

function beatForPhase(phase) {
  if (phase === "idle" || phase === "queued" || phase === "start") return "start";
  if (phase === "crash") return "crash";
  if (phase === "wait-lease") return "wait-lease";
  if (phase === "recover") return "recover";
  if (phase === "watch" || phase === "lab-done") return "watch";
  return null;
}

const BEAT_ORDER = ["start", "crash", "wait-lease", "recover", "watch"];

// --- UI ---

if (typeof document !== "undefined") {
  const $ = (sel) => document.querySelector(sel);

  const presetSelect = $("#preset");
  const presetWrap = $("#preset-wrap");
  const startLabBtn = $("#start-lab-btn");
  const startFreeBtn = $("#start-free-btn");
  const crashBtn = $("#crash-btn");
  const recoverBtn = $("#recover-btn");
  const resetBtn = $("#reset-btn");
  const wfStatus = $("#wf-status");
  const wfId = $("#wf-id");
  const dagEmpty = $("#dag-empty");
  const logEmpty = $("#log-empty");
  const eventLog = $("#event-log");
  const dagSvg = $("#dag-svg");
  const dagNodes = $("#dag-nodes");
  const dagEdges = $("#dag-edges");
  const attemptTable = $("#attempt-table");
  const attemptRows = $("#attempt-rows");
  const attemptEmpty = $("#attempt-empty");
  const payPanel = $("#pay-panel");
  const payTable = $("#pay-table");
  const payRows = $("#pay-rows");
  const payEmpty = $("#pay-empty");
  const narration = $("#narration");
  const beats = $("#beats");
  const engineDetail = $("#engine-detail");
  const engineJson = $("#engine-json");
  const consumerA = $("#consumer-A");
  const consumerB = $("#consumer-B");

  /** @type {MiniTemporalEngine | null} */
  let engine = null;
  let labDone = false;

  function renderMeta() {
    if (!engine) {
      wfStatus.textContent = "—";
      wfStatus.removeAttribute("data-status");
      wfId.textContent = "—";
      return;
    }
    wfStatus.textContent = engine.workflowStatus;
    wfStatus.setAttribute("data-status", engine.workflowStatus);
    wfId.textContent = shortId(engine.workflowId);
    wfId.title = engine.workflowId;
  }

  function renderConsumers() {
    const states = engine?.consumers ?? { A: "idle", B: "idle" };
    const phase = labPhase(engine, Date.now(), labDone);
    let stateB = states.B;
    if (engine?.runKind === "lab" && (phase === "crash" || phase === "queued" || phase === "wait-lease")) {
      stateB = "waiting for retry";
    }
    consumerA.querySelector(".consumer-state").textContent = states.A;
    consumerB.querySelector(".consumer-state").textContent = stateB;
    consumerA.classList.toggle("live", states.A.startsWith("claimed"));
    consumerB.classList.toggle("live", states.B.startsWith("claimed"));
    consumerA.classList.toggle("dead", states.A.startsWith("dead"));
    consumerB.classList.toggle("dead", states.B.startsWith("dead"));
    consumerA.classList.toggle("skip", states.A.startsWith("skip"));
    consumerB.classList.toggle("skip", states.B.startsWith("skip"));
  }

  function renderGuide() {
    const phase = labPhase(engine, Date.now(), labDone);
    narration.textContent = NARRATION[phase] ?? NARRATION.idle;
    const currentBeat = beatForPhase(phase);
    const inLabChrome =
      (!labDone && (!engine || engine.runKind === "lab")) ||
      (engine?.runKind === "lab" && engine.workflowStatus === "RUNNING");
    beats.hidden =
      !inLabChrome ||
      phase === "free" ||
      phase === "free-done" ||
      phase === "lab-done";
    if (!beats.hidden) {
      const currentIdx = BEAT_ORDER.indexOf(currentBeat ?? "start");
      for (const li of beats.querySelectorAll("[data-beat]")) {
        const idx = BEAT_ORDER.indexOf(li.dataset.beat);
        li.classList.toggle("current", li.dataset.beat === currentBeat);
        li.classList.toggle("done", idx >= 0 && idx < currentIdx);
      }
    }
  }

  function renderEvents() {
    if (!engine || engine.events.length === 0) {
      eventLog.hidden = true;
      logEmpty.hidden = false;
      return;
    }
    logEmpty.hidden = true;
    eventLog.hidden = false;
    eventLog.innerHTML = "";

    for (const ev of [...engine.events].reverse()) {
      const li = document.createElement("li");
      li.className = `event-item type-${ev.eventType}`;
      li.innerHTML = `
      <span class="event-seq">${String(ev.sequence).padStart(2, "0")}</span>
      <div>
        <div class="event-type">${eventSentence(ev)}</div>
      </div>
    `;
      eventLog.appendChild(li);
    }
  }

  function renderEngineDetail() {
    const show = labDone;
    engineDetail.hidden = !show;
    if (!show || !engine) {
      engineJson.textContent = "";
      return;
    }
    engineJson.textContent = engine.events
      .map((ev) => `${ev.sequence} ${ev.eventType} ${JSON.stringify(ev.data)}`)
      .join("\n");
  }

  function leaseLabel(attempt) {
    if (attempt.status !== "RUNNING") return "—";
    const ms = Math.max(0, attempt.leaseUntil - Date.now());
    return (ms / 1000).toFixed(1) + "s";
  }

  function renderAttempts() {
    if (!engine || engine.attempts.length === 0) {
      attemptTable.hidden = true;
      attemptEmpty.hidden = false;
      return;
    }
    attemptEmpty.hidden = true;
    attemptTable.hidden = false;
    attemptRows.innerHTML = "";

    for (const attempt of engine.attempts) {
      const tr = document.createElement("tr");
      const statusClass = attempt.status.toLowerCase();
      tr.innerHTML = `
      <td>${attempt.attemptNumber}</td>
      <td>${taskTitle(attempt.taskKey)}</td>
      <td>${attempt.consumerId ?? "—"}</td>
      <td title="${attempt.workerId}">${shortId(attempt.workerId)}</td>
      <td>${leaseLabel(attempt)}</td>
      <td><span class="status-chip ${statusClass}">${attempt.status}</span></td>
    `;
      attemptRows.appendChild(tr);
    }
  }

  function renderPayments() {
    const showPanel =
      Boolean(engine) &&
      (engine.definition.tasks.some((t) => t.type === "CHARGE_PAYMENT") ||
        engine.payments.length > 0);

    payPanel.hidden = !showPanel;
    if (!showPanel) return;

    if (!engine || engine.payments.length === 0) {
      payTable.hidden = true;
      payEmpty.hidden = false;
      return;
    }

    payEmpty.hidden = true;
    payTable.hidden = false;
    payRows.innerHTML = "";

    for (const pay of engine.payments) {
      const tr = document.createElement("tr");
      const hit = pay.duplicate ? "duplicate" : "first";
      tr.innerHTML = `
      <td title="${pay.operationId}">${shortId(pay.operationId)}</td>
      <td title="${pay.transactionId}">${shortId(pay.transactionId)}</td>
      <td>${pay.amount}</td>
      <td><span class="${pay.duplicate ? "hit-dup" : "hit-first"}">${hit}</span></td>
    `;
      payRows.appendChild(tr);
    }
  }

  function renderDag() {
    const preset = engine?.definition ?? PRESETS[presetSelect.value];
    dagSvg.setAttribute("viewBox", preset.viewBox);

    if (!engine) {
      dagEmpty.hidden = false;
      dagNodes.innerHTML = "";
      dagEdges.innerHTML = "";
      return;
    }
    dagEmpty.hidden = true;

    const layout = preset.layout;
    const nodeW = 120;
    const nodeH = 52;

    dagEdges.innerHTML = "";
    for (const taskDef of preset.tasks) {
      for (const dep of taskDef.dependsOn ?? []) {
        const from = layout[dep];
        const to = layout[taskDef.id];
        const depTask = engine.tasks.get(dep);
        const line = document.createElementNS("http://www.w3.org/2000/svg", "path");
        line.setAttribute("class", "dag-edge");
        if (depTask?.status === "COMPLETED") line.classList.add("active");
        else line.classList.add("waiting");

        const x1 = from.x + nodeW / 2;
        const y1 = from.y + nodeH;
        const x2 = to.x + nodeW / 2;
        const y2 = to.y;
        const midY = (y1 + y2) / 2;
        line.setAttribute(
          "d",
          `M ${x1} ${y1} C ${x1} ${midY}, ${x2} ${midY}, ${x2} ${y2}`,
        );
        dagEdges.appendChild(line);
      }
    }

    dagNodes.innerHTML = "";
    for (const taskDef of preset.tasks) {
      const task = engine.tasks.get(taskDef.id);
      const pos = layout[taskDef.id];
      const g = document.createElementNS("http://www.w3.org/2000/svg", "g");
      g.setAttribute("class", `dag-node ${task.status.toLowerCase()}`);
      g.setAttribute("transform", `translate(${pos.x}, ${pos.y})`);
      g.dataset.key = task.key;
      if (engine.canCrash(task.key)) g.classList.add("crashable");

      const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
      rect.setAttribute("width", String(nodeW));
      rect.setAttribute("height", String(nodeH));
      rect.setAttribute("rx", "3");
      if (task.status === "RUNNING") rect.classList.add("pulse");

      const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
      label.setAttribute("class", "dag-node-label");
      label.setAttribute("x", "8");
      label.setAttribute("y", "18");
      label.textContent = task.name;

      const type = document.createElementNS("http://www.w3.org/2000/svg", "text");
      type.setAttribute("class", "dag-node-type");
      type.setAttribute("x", "8");
      type.setAttribute("y", "32");
      type.textContent = task.type;

      const attempt = engine.currentAttemptFor(task);
      const status = document.createElementNS("http://www.w3.org/2000/svg", "text");
      status.setAttribute("class", "dag-node-status");
      status.setAttribute("x", "8");
      status.setAttribute("y", "46");
      if (task.status === "RUNNING" && attempt) {
        if (engine.canCrash(task.key)) {
          status.textContent = "click to crash";
        } else if (engine.crashRequested.has(attempt.id)) {
          status.textContent = `dead ${leaseLabel(attempt)}`;
        } else {
          const who = attempt.consumerId ? `${attempt.consumerId} ` : "";
          status.textContent = `${who}${leaseLabel(attempt)}`;
        }
      } else {
        status.textContent = task.status;
      }

      g.append(rect, label, type, status);

      if (task.status === "RUNNING" && attempt) {
        const frac = Math.max(
          0,
          Math.min(1, (attempt.leaseUntil - Date.now()) / engine.leaseMs),
        );
        const bar = document.createElementNS("http://www.w3.org/2000/svg", "rect");
        bar.setAttribute("class", "lease-bar");
        bar.setAttribute("x", "1");
        bar.setAttribute("y", String(nodeH - 4));
        bar.setAttribute("width", String((nodeW - 2) * frac));
        bar.setAttribute("height", "3");
        g.append(bar);
      }

      dagNodes.appendChild(g);
    }
  }

  function syncButtons() {
    const live = Boolean(engine) && engine.workflowStatus === "RUNNING";
    const idle = !live;

    presetWrap.hidden = !labDone;
    presetSelect.disabled = live;

    startLabBtn.hidden = !idle;
    startLabBtn.textContent = labDone ? "Run lab again" : "Start lab";
    startLabBtn.disabled = live;

    startFreeBtn.hidden = !labDone || !idle;
    startFreeBtn.disabled = live;

    crashBtn.disabled = !engine || !engine.canCrash();
    crashBtn.classList.toggle(
      "need-action",
      Boolean(engine && engine.canCrash()),
    );
    resetBtn.hidden = !engine;

    const dying = engine?.dyingAttempt();
    const stale = engine ? engine.staleAttempts().length > 0 : false;
    recoverBtn.disabled = !engine || !(stale || dying || engine.recoverQueued);
    recoverBtn.classList.toggle("need-action", stale || Boolean(engine?.recoverQueued));
    if (dying) {
      const ms = Math.max(0, dying.leaseUntil - Date.now());
      recoverBtn.textContent = engine.recoverQueued
        ? `Recover queued (${(ms / 1000).toFixed(1)}s)`
        : `Recover stale (${(ms / 1000).toFixed(1)}s)`;
    } else {
      recoverBtn.textContent = "Recover stale";
    }

    if (engine?.runKind === "lab" && engine.workflowStatus === "COMPLETED") {
      labDone = true;
    }
  }

  function renderAll() {
    if (engine?.runKind === "lab" && engine.workflowStatus === "COMPLETED") {
      labDone = true;
    }
    renderMeta();
    renderDag();
    renderEvents();
    renderAttempts();
    renderPayments();
    renderConsumers();
    renderGuide();
    renderEngineDetail();
    syncButtons();
  }

  function labOptions() {
    return {
      dualClaim: false,
      gateCrashKey: "verify",
      unlockDualOnRecover: true,
      runKind: "lab",
    };
  }

  async function runEngine(next) {
    engine?.stop();
    engine = next;
    engine.start();
    renderAll();
    await engine.run(renderAll);
    if (engine.runKind === "lab" && engine.workflowStatus === "COMPLETED") {
      labDone = true;
    }
    renderAll();
  }

  async function startLab() {
    presetSelect.value = "onboarding";
    await runEngine(new MiniTemporalEngine(PRESETS.onboarding, labOptions()));
  }

  async function startFree() {
    await runEngine(
      new MiniTemporalEngine(PRESETS[presetSelect.value], {
        dualClaim: true,
        runKind: "free",
      }),
    );
  }

  function resetDemo() {
    engine?.stop();
    engine = null;
    renderAll();
  }

  startLabBtn.addEventListener("click", startLab);
  startFreeBtn.addEventListener("click", startFree);
  resetBtn.addEventListener("click", resetDemo);
  crashBtn.addEventListener("click", () => {
    engine?.crashWorker();
    renderAll();
  });
  recoverBtn.addEventListener("click", () => {
    engine?.queueRecover();
    renderAll();
  });
  dagNodes.addEventListener("click", (e) => {
    const g = e.target.closest(".dag-node");
    const key = g?.dataset.key;
    if (!key || !engine || !engine.canCrash(key)) return;
    engine.crashWorker(key);
    renderAll();
  });
  presetSelect.addEventListener("change", () => {
    if (!engine) renderAll();
  });

  renderAll();
}
