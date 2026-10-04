/**
 * In-browser simulation of mini-temporal workflow engine.
 * Mirrors engine/src/index.ts scheduling + worker execution with dependency unblocking.
 */

const PRESETS = {
  order: {
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
    layout: {
      charge: { x: 80, y: 40 },
      reserve: { x: 320, y: 40 },
      email: { x: 200, y: 180 },
    },
  },
  onboarding: {
    type: "USER_ONBOARDING",
    name: "Onboard User",
    version: 1,
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

const ACTIVITY_MS = 1400;
const OUTBOX_MS = 350;

function uuid() {
  return crypto.randomUUID();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/** @typedef {{ id: string, name: string, type: string, dependsOn?: string[] }} WorkflowTask */
/** @typedef {{ type: string, name: string, version: number, tasks: WorkflowTask[], layout: Record<string, {x:number,y:number}> }} Preset */

class MiniTemporalEngine {
  /** @param {Preset} definition */
  constructor(definition) {
    this.definition = definition;
    this.workflowId = uuid();
    this.workflowStatus = "RUNNING";
    this.sequence = 0;
    /** @type {{ sequence: number, eventType: string, data: object, at: Date }[]} */
    this.events = [];
    /** @type {Map<string, object>} */
    this.tasks = new Map();
    /** @type {Map<string, string[]>} taskKey -> dependsOn keys */
    this.deps = new Map();
    /** @type {Map<string, string[]>} taskKey -> dependents */
    this.dependents = new Map();
    /** @type {{ id: string, taskKey: string, payload: object }[]} */
    this.outbox = [];
    this.running = false;
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

    this.appendEvent("TASK_READY", { taskKey, taskId: task.id, taskType: task.type });

    const outboxId = uuid();
    this.outbox.push({
      id: outboxId,
      taskKey,
      payload: {
        workflowId: this.workflowId,
        taskId: task.id,
        taskKey,
        taskType: task.type,
      },
    });
  }

  /** Simulate outbox publisher + Kafka delivery */
  async publishOutbox(onPublish) {
    while (this.running) {
      if (this.outbox.length === 0) {
        await sleep(80);
        continue;
      }
      const msg = this.outbox.shift();
      await sleep(OUTBOX_MS);
      if (!this.running) return;
      this.appendEvent("OUTBOX_PUBLISHED", {
        outboxId: msg.id,
        taskKey: msg.taskKey,
        topic: "workflow-tasks",
      });
      onPublish?.(msg);
    }
  }

  /** Worker claims and executes a task */
  async executeTask(taskKey, onTick) {
    const task = this.tasks.get(taskKey);
    if (!task || task.status !== "READY") return;

    task.status = "RUNNING";
    task.attemptCount += 1;
    onTick?.();

    this.appendEvent("TASK_RUNNING", {
      taskKey,
      taskId: task.id,
      attempt: task.attemptCount,
      operationId: task.operationId,
    });
    onTick?.();

    await sleep(ACTIVITY_MS);
    if (!this.running) return;

    task.result = {
      transactionId: uuid(),
      operationId: task.operationId,
      success: true,
    };
    task.status = "COMPLETED";

    this.appendEvent("TASK_COMPLETED", {
      taskKey,
      taskId: task.id,
      result: task.result,
    });
    onTick?.();

    this.unblockDependents(taskKey, onTick);
  }

  /** @param {string} completedKey @param {() => void} [onTick] */
  unblockDependents(completedKey, onTick) {
    for (const dependentKey of this.dependents.get(completedKey) ?? []) {
      const task = this.tasks.get(dependentKey);
      if (!task || task.status !== "PENDING") continue;

      const deps = this.deps.get(dependentKey) ?? [];
      const allDone = deps.every((d) => this.tasks.get(d)?.status === "COMPLETED");
      if (!allDone) continue;

      task.status = "READY";
      this.scheduleReady(dependentKey);
      onTick?.();
    }
  }

  checkWorkflowComplete(onTick) {
    const allDone = [...this.tasks.values()].every((t) => t.status === "COMPLETED");
    if (allDone && this.workflowStatus === "RUNNING") {
      this.workflowStatus = "COMPLETED";
      this.appendEvent("WORKFLOW_COMPLETED", { workflowId: this.workflowId });
      onTick?.();
    }
  }

  async run(onTick) {
    this.running = true;

    const workerQueue = [];
    const enqueue = (msg) => workerQueue.push(msg);

    const publisher = this.publishOutbox((msg) => enqueue(msg));

    while (this.running) {
      if (workerQueue.length > 0) {
        const msg = workerQueue.shift();
        await this.executeTask(msg.taskKey, onTick);
        this.checkWorkflowComplete(onTick);
      } else if (this.outbox.length === 0 && workerQueue.length === 0) {
        const pending = [...this.tasks.values()].some(
          (t) => t.status === "READY" || t.status === "RUNNING" || t.status === "PENDING",
        );
        if (!pending) break;
      }
      await sleep(80);
    }

    this.running = false;
    await publisher;
  }

  stop() {
    this.running = false;
  }
}

// --- UI ---

const $ = (sel) => document.querySelector(sel);

const presetSelect = $("#preset");
const startBtn = $("#start-btn");
const resetBtn = $("#reset-btn");
const wfStatus = $("#wf-status");
const wfId = $("#wf-id");
const dagEmpty = $("#dag-empty");
const logEmpty = $("#log-empty");
const eventLog = $("#event-log");
const dagNodes = $("#dag-nodes");
const dagEdges = $("#dag-edges");

/** @type {MiniTemporalEngine | null} */
let engine = null;

function shortId(id) {
  return id.slice(0, 8) + "…";
}

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
        <div class="event-type">${ev.eventType}</div>
        <div class="event-data">${JSON.stringify(ev.data, null, 0)}</div>
      </div>
    `;
    eventLog.appendChild(li);
  }
}

function renderDag() {
  if (!engine) {
    dagEmpty.hidden = false;
    dagNodes.innerHTML = "";
    dagEdges.innerHTML = "";
    return;
  }
  dagEmpty.hidden = true;

  const preset = PRESETS[presetSelect.value];
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

    const status = document.createElementNS("http://www.w3.org/2000/svg", "text");
    status.setAttribute("class", "dag-node-status");
    status.setAttribute("x", "8");
    status.setAttribute("y", "46");
    status.textContent = task.status;

    g.append(rect, label, type, status);
    dagNodes.appendChild(g);
  }
}

function renderAll() {
  renderMeta();
  renderDag();
  renderEvents();
}

function setControlsRunning(running) {
  startBtn.disabled = running;
  presetSelect.disabled = running;
  resetBtn.hidden = !running && !engine;
}

async function startWorkflow() {
  engine?.stop();
  engine = new MiniTemporalEngine(PRESETS[presetSelect.value]);
  engine.start();
  setControlsRunning(true);
  renderAll();

  await engine.run(renderAll);
  setControlsRunning(false);
  resetBtn.hidden = false;
}

function resetDemo() {
  engine?.stop();
  engine = null;
  setControlsRunning(false);
  resetBtn.hidden = true;
  renderAll();
}

startBtn.addEventListener("click", startWorkflow);
resetBtn.addEventListener("click", resetDemo);
presetSelect.addEventListener("change", () => {
  if (!engine) resetDemo();
});

renderAll();
