import { describe, expect, test } from "bun:test";
import { pool } from "../db/client";
import { recoverStaleTasks } from "../recovery/recoverStaleTasks";
import { claimTask } from "../worker/claimTask";
import { cancelWorkflow } from "./cancelWorkflow";
import { getTaskIdsByKey, startWorkflow } from "./startWorkflow";
import {
  claimTaskForTest,
  completeAttempt,
  failAttemptForTest,
} from "./taskCompletion";

async function dbReady(): Promise<boolean> {
  try {
    await pool.query("SELECT 1");
    return true;
  } catch {
    return false;
  }
}

const hasDb = await dbReady();

const TERMINAL_WORKFLOW = new Set(["CANCELLED", "FAILED", "COMPLETED"]);

async function workflowStatus(workflowId: string): Promise<string> {
  const result = await pool.query(
    `SELECT status FROM workflows WHERE id = $1`,
    [workflowId],
  );
  return result.rows[0]?.status;
}

async function taskStatus(taskId: string): Promise<string> {
  const result = await pool.query(`SELECT status FROM tasks WHERE id = $1`, [
    taskId,
  ]);
  return result.rows[0]?.status;
}

async function taskResult(taskId: string): Promise<unknown> {
  const result = await pool.query(`SELECT result FROM tasks WHERE id = $1`, [
    taskId,
  ]);
  return result.rows[0]?.result ?? null;
}

async function outboxTaskReadyCount(workflowId: string): Promise<number> {
  const result = await pool.query(
    `
    SELECT COUNT(*)::int AS count
    FROM outbox
    WHERE workflow_id = $1
      AND event_type = 'TASK_READY'
    `,
    [workflowId],
  );
  return result.rows[0].count;
}

async function eventSequences(workflowId: string): Promise<number[]> {
  const result = await pool.query(
    `
    SELECT sequence
    FROM workflow_events
    WHERE workflow_id = $1
    ORDER BY sequence
    `,
    [workflowId],
  );
  return result.rows.map((row) => Number(row.sequence));
}

function expectUniqueSequences(sequences: number[]) {
  expect(new Set(sequences).size).toBe(sequences.length);
}

describe.skipIf(!hasDb)("workflow cancellation (integration)", () => {
  test("cancel RUNNING workflow: READY, PENDING, and RUNNING tasks become CANCELLED", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_CANCEL_MIX",
      name: "Mixed task states",
      version: 1,
      tasks: [
        { id: "A", name: "Task A", type: "NOOP" },
        { id: "B", name: "Task B", type: "NOOP" },
        { id: "C", name: "Task C", type: "NOOP", dependsOn: ["A"] },
      ],
    });

    const keys = await getTaskIdsByKey(workflowId);
    const taskA = keys.get("A")!;
    const taskB = keys.get("B")!;
    const taskC = keys.get("C")!;

    expect(await taskStatus(taskB)).toBe("READY");
    expect(await taskStatus(taskC)).toBe("PENDING");

    const workerId = crypto.randomUUID();
    const claim = await claimTaskForTest(pool, taskA, workerId);
    expect(claim).not.toBeNull();
    expect(await taskStatus(taskA)).toBe("RUNNING");

    const result = await cancelWorkflow(pool, workflowId);
    expect(result.outcome).toBe("CANCELLED");
    expect(await workflowStatus(workflowId)).toBe("CANCELLED");
    expect(await taskStatus(taskA)).toBe("CANCELLED");
    expect(await taskStatus(taskB)).toBe("CANCELLED");
    expect(await taskStatus(taskC)).toBe("CANCELLED");

    const cancelledEvent = await pool.query(
      `
      SELECT 1
      FROM workflow_events
      WHERE workflow_id = $1
        AND event_type = 'WORKFLOW_CANCELLED'
      `,
      [workflowId],
    );
    expect(cancelledEvent.rowCount).toBe(1);
  });

  test("late Kafka delivery after cancel: claimTask returns null", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_CANCEL_KAFKA",
      name: "Late message",
      version: 1,
      tasks: [
        { id: "A", name: "Task A", type: "NOOP" },
        { id: "B", name: "Task B", type: "NOOP" },
      ],
    });

    const keys = await getTaskIdsByKey(workflowId);
    const taskA = keys.get("A")!;
    const taskB = keys.get("B")!;

    await claimTaskForTest(pool, taskA, crypto.randomUUID());

    await pool.query(
      `
      UPDATE outbox
      SET published_at = NOW()
      WHERE workflow_id = $1
        AND task_id = $2
        AND event_type = 'TASK_READY'
      `,
      [workflowId, taskB],
    );

    await cancelWorkflow(pool, workflowId);
    expect(await taskStatus(taskB)).toBe("CANCELLED");

    const claimed = await claimTask(taskB, crypto.randomUUID(), pool);
    expect(claimed).toBeNull();
    expect(await taskStatus(taskB)).toBe("CANCELLED");
  });

  test("invalidated attempt completion commits no task result", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_CANCEL_COMPLETE",
      name: "Stale completion",
      version: 1,
      tasks: [{ id: "A", name: "Task A", type: "NOOP" }],
    });

    const keys = await getTaskIdsByKey(workflowId);
    const taskA = keys.get("A")!;
    const workerId = crypto.randomUUID();
    const claim = await claimTaskForTest(pool, taskA, workerId);
    expect(claim).not.toBeNull();

    await cancelWorkflow(pool, workflowId);

    const ok = await completeAttempt(
      pool,
      taskA,
      claim!.attemptId,
      workerId,
      claim!.fencingToken,
      { shouldNotPersist: true },
    );

    expect(ok).toBe(false);
    expect(await taskStatus(taskA)).toBe("CANCELLED");
    expect(await taskResult(taskA)).toBeNull();
    expect(await workflowStatus(workflowId)).toBe("CANCELLED");

    const completedEvents = await pool.query(
      `
      SELECT 1
      FROM workflow_events
      WHERE workflow_id = $1
        AND event_type = 'TASK_COMPLETED'
      `,
      [workflowId],
    );
    expect(completedEvents.rowCount).toBe(0);
  });

  test("recovery after cancellation does not requeue tasks", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_CANCEL_RECOVERY",
      name: "No recovery after cancel",
      version: 1,
      tasks: [
        { id: "A", name: "Task A", type: "NOOP" },
        { id: "B", name: "Task B", type: "NOOP" },
      ],
    });

    const keys = await getTaskIdsByKey(workflowId);
    const taskA = keys.get("A")!;
    const taskB = keys.get("B")!;

    const workerA = crypto.randomUUID();
    const workerB = crypto.randomUUID();
    const claimA = await claimTaskForTest(pool, taskA, workerA);
    const claimB = await claimTaskForTest(pool, taskB, workerB);
    expect(claimA).not.toBeNull();
    expect(claimB).not.toBeNull();

    await pool.query(
      `
      UPDATE task_attempts
      SET lease_until = NOW() - INTERVAL '1 minute'
      WHERE id = $1
      `,
      [claimB!.attemptId],
    );

    const outboxBefore = await outboxTaskReadyCount(workflowId);
    await cancelWorkflow(pool, workflowId);

    await recoverStaleTasks();

    expect(await taskStatus(taskA)).toBe("CANCELLED");
    expect(await taskStatus(taskB)).toBe("CANCELLED");
    expect(await outboxTaskReadyCount(workflowId)).toBe(outboxBefore);

    const recoveryEvents = await pool.query(
      `
      SELECT 1
      FROM workflow_events
      WHERE workflow_id = $1
        AND event_type = 'TASK_RECOVERY'
      `,
      [workflowId],
    );
    expect(recoveryEvents.rowCount).toBe(0);
  });

  test("second cancel returns ALREADY_TERMINAL", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_CANCEL_TWICE",
      name: "Idempotent cancel",
      version: 1,
      tasks: [{ id: "A", name: "Task A", type: "NOOP" }],
    });

    const first = await cancelWorkflow(pool, workflowId);
    expect(first.outcome).toBe("CANCELLED");

    const second = await cancelWorkflow(pool, workflowId);
    expect(second.outcome).toBe("ALREADY_TERMINAL");
    if (second.outcome === "ALREADY_TERMINAL") {
      expect(second.status).toBe("CANCELLED");
    }
  });

  test("race cancel vs completion: terminal workflow and unique event sequences", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_CANCEL_RACE_COMPLETE",
      name: "Cancel vs complete",
      version: 1,
      tasks: [{ id: "A", name: "Task A", type: "NOOP" }],
    });

    const keys = await getTaskIdsByKey(workflowId);
    const taskA = keys.get("A")!;
    const workerId = crypto.randomUUID();
    const claim = await claimTaskForTest(pool, taskA, workerId);
    expect(claim).not.toBeNull();

    await Promise.all([
      cancelWorkflow(pool, workflowId),
      completeAttempt(
        pool,
        taskA,
        claim!.attemptId,
        workerId,
        claim!.fencingToken,
        { race: true },
      ),
    ]);

    const status = await workflowStatus(workflowId);
    expect(TERMINAL_WORKFLOW.has(status)).toBe(true);

    const sequences = await eventSequences(workflowId);
    expect(sequences.length).toBeGreaterThan(0);
    expectUniqueSequences(sequences);
  });

  test("race cancel vs permanent failure: terminal workflow and unique event sequences", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_CANCEL_RACE_FAIL",
      name: "Cancel vs fail",
      version: 1,
      tasks: [{ id: "A", name: "Task A", type: "NOOP" }],
    });

    const keys = await getTaskIdsByKey(workflowId);
    const taskA = keys.get("A")!;
    const workerId = crypto.randomUUID();
    const claim = await claimTaskForTest(pool, taskA, workerId);
    expect(claim).not.toBeNull();

    await Promise.all([
      cancelWorkflow(pool, workflowId),
      failAttemptForTest(
        pool,
        taskA,
        claim!.attemptId,
        workerId,
        claim!.fencingToken,
        workflowId,
        { message: "permanent" },
      ),
    ]);

    const status = await workflowStatus(workflowId);
    expect(TERMINAL_WORKFLOW.has(status)).toBe(true);

    const sequences = await eventSequences(workflowId);
    expect(sequences.length).toBeGreaterThan(0);
    expectUniqueSequences(sequences);
  });
});

if (!hasDb) {
  test("workflow cancellation integration requires Postgres", () => {
    console.warn(
      "Start engine infra: cd engine && docker compose up -d && bun run migrate",
    );
    expect(true).toBe(true);
  });
}
