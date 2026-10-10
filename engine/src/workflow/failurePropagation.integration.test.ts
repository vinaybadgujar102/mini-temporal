import { describe, expect, test } from "bun:test";
import { pool } from "../db/client";
import { recoverStaleTasks } from "../recovery/recoverStaleTasks";
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

async function failureEvents(workflowId: string) {
  const result = await pool.query(
    `
    SELECT sequence, event_type
    FROM workflow_events
    WHERE workflow_id = $1
      AND event_type IN ('TASK_FAILED', 'WORKFLOW_FAILED')
    ORDER BY sequence
    `,
    [workflowId],
  );
  return result.rows;
}

describe.skipIf(!hasDb)("failure propagation (integration)", () => {
  test("permanent task failure marks task and workflow FAILED", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_FAIL_PERM",
      name: "Permanent fail",
      version: 1,
      tasks: [{ id: "A", name: "Task A", type: "NOOP" }],
    });

    const keys = await getTaskIdsByKey(workflowId);
    const taskA = keys.get("A")!;
    const workerId = crypto.randomUUID();
    const claim = await claimTaskForTest(pool, taskA, workerId);
    expect(claim).not.toBeNull();

    const result = await failAttemptForTest(
      pool,
      taskA,
      claim!.attemptId,
      workerId,
      claim!.fencingToken,
      workflowId,
      { message: "permanent" },
    );

    expect(result.outcome).toBe("FAILED");
    expect(await taskStatus(taskA)).toBe("FAILED");
    expect(await workflowStatus(workflowId)).toBe("FAILED");
  });

  test("TASK_FAILED and WORKFLOW_FAILED in order with unique sequences", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_FAIL_EVENTS",
      name: "Fail events",
      version: 1,
      tasks: [{ id: "A", name: "Task A", type: "NOOP" }],
    });

    const keys = await getTaskIdsByKey(workflowId);
    const taskA = keys.get("A")!;
    const workerId = crypto.randomUUID();
    const claim = await claimTaskForTest(pool, taskA, workerId);
    expect(claim).not.toBeNull();

    await failAttemptForTest(
      pool,
      taskA,
      claim!.attemptId,
      workerId,
      claim!.fencingToken,
      workflowId,
      { message: "permanent" },
    );

    const events = await failureEvents(workflowId);
    expect(events).toHaveLength(2);
    expect(events[0].event_type).toBe("TASK_FAILED");
    expect(events[1].event_type).toBe("WORKFLOW_FAILED");
    const seqTaskFailed = Number(events[0].sequence);
    const seqWorkflowFailed = Number(events[1].sequence);
    expect(seqTaskFailed).toBeLessThan(seqWorkflowFailed);
    expect(seqTaskFailed).not.toBe(seqWorkflowFailed);
  });

  test("READY and PENDING tasks CANCELLED, no new TASK_READY outbox", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_FAIL_CANCEL",
      name: "Cancel siblings",
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

    const outboxBefore = await outboxTaskReadyCount(workflowId);

    const workerId = crypto.randomUUID();
    const claim = await claimTaskForTest(pool, taskA, workerId);
    expect(claim).not.toBeNull();

    await failAttemptForTest(
      pool,
      taskA,
      claim!.attemptId,
      workerId,
      claim!.fencingToken,
      workflowId,
      { message: "permanent" },
    );

    expect(await taskStatus(taskB)).toBe("CANCELLED");
    expect(await taskStatus(taskC)).toBe("CANCELLED");
    expect(await outboxTaskReadyCount(workflowId)).toBe(outboxBefore);
  });

  test("retryable failure schedules retry and workflow stays RUNNING", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_FAIL_RETRY",
      name: "Retryable",
      version: 1,
      tasks: [{ id: "A", name: "Task A", type: "NOOP" }],
    });

    const keys = await getTaskIdsByKey(workflowId);
    const taskA = keys.get("A")!;
    const workerId = crypto.randomUUID();
    const claim = await claimTaskForTest(pool, taskA, workerId);
    expect(claim).not.toBeNull();

    const result = await failAttemptForTest(
      pool,
      taskA,
      claim!.attemptId,
      workerId,
      claim!.fencingToken,
      workflowId,
      { code: "503", message: "upstream unavailable" },
    );

    expect(result.outcome).toBe("RETRY_SCHEDULED");
    expect(await workflowStatus(workflowId)).toBe("RUNNING");
    expect(await taskStatus(taskA)).toBe("READY");

    const row = await pool.query(
      `SELECT next_retry_at FROM tasks WHERE id = $1`,
      [taskA],
    );
    expect(row.rows[0].next_retry_at).not.toBeNull();
  });

  test("stale attempt cannot fail task or workflow", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_FAIL_STALE",
      name: "Stale fail",
      version: 1,
      tasks: [{ id: "A", name: "Task A", type: "NOOP" }],
    });

    const keys = await getTaskIdsByKey(workflowId);
    const taskA = keys.get("A")!;
    const workerId = crypto.randomUUID();
    const claim = await claimTaskForTest(pool, taskA, workerId);
    expect(claim).not.toBeNull();

    await pool.query(
      `
      UPDATE task_attempts
      SET lease_until = NOW() - INTERVAL '1 minute'
      WHERE id = $1
      `,
      [claim!.attemptId],
    );

    const result = await failAttemptForTest(
      pool,
      taskA,
      claim!.attemptId,
      workerId,
      claim!.fencingToken,
      workflowId,
      { message: "permanent" },
    );

    expect(result.outcome).toBe("STALE_ATTEMPT");
    expect(await taskStatus(taskA)).toBe("RUNNING");
    expect(await workflowStatus(workflowId)).toBe("RUNNING");
    expect(await failureEvents(workflowId)).toHaveLength(0);
  });

  test("concurrent completion cannot overwrite FAILED workflow", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_FAIL_RACE",
      name: "Fail race",
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

    await failAttemptForTest(
      pool,
      taskA,
      claimA!.attemptId,
      workerA,
      claimA!.fencingToken,
      workflowId,
      { message: "permanent" },
    );

    expect(await workflowStatus(workflowId)).toBe("FAILED");

    const ok = await completeAttempt(
      pool,
      taskB,
      claimB!.attemptId,
      workerB,
      claimB!.fencingToken,
      { ok: true },
    );

    expect(ok).toBe(false);
    expect(await workflowStatus(workflowId)).toBe("FAILED");
    expect(await taskStatus(taskB)).toBe("CANCELLED");
  });

  test("stale-task recovery does not requeue tasks from failed workflows", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_FAIL_RECOVERY",
      name: "Recovery skip failed wf",
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

    await failAttemptForTest(
      pool,
      taskA,
      claimA!.attemptId,
      workerA,
      claimA!.fencingToken,
      workflowId,
      { message: "permanent" },
    );

    expect(await workflowStatus(workflowId)).toBe("FAILED");
    expect(await taskStatus(taskB)).toBe("CANCELLED");

    const outboxBefore = await outboxTaskReadyCount(workflowId);
    await recoverStaleTasks();

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
});

if (!hasDb) {
  test("failure propagation integration requires Postgres", () => {
    console.warn(
      "Start engine infra: cd engine && docker compose up -d && bun run migrate",
    );
    expect(true).toBe(true);
  });
}
