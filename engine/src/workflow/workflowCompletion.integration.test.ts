import { afterAll, describe, expect, test } from "bun:test";
import type { PoolClient } from "pg";
import { pool } from "../db/client";
import { getTaskIdsByKey, startWorkflow } from "./startWorkflow";
import { claimTaskForTest, completeAttempt } from "./taskCompletion";
import { updateWorkflowStatus } from "./updateWorkflowStatus";

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

async function completionEventCount(workflowId: string): Promise<number> {
  const result = await pool.query(
    `
    SELECT COUNT(*)::int AS count
    FROM workflow_events
    WHERE workflow_id = $1
      AND event_type = 'WORKFLOW_COMPLETED'
    `,
    [workflowId],
  );
  return result.rows[0].count;
}

async function taskStatus(taskId: string): Promise<string> {
  const result = await pool.query(`SELECT status FROM tasks WHERE id = $1`, [
    taskId,
  ]);
  return result.rows[0]?.status;
}

describe.skipIf(!hasDb)("workflow completion (integration)", () => {
  afterAll(async () => {
    await pool.end();
  });

  test("A then B: RUNNING until both done, one WORKFLOW_COMPLETED", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_PARALLEL",
      name: "Parallel A and B",
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
    const claimA = await claimTaskForTest(pool, taskA, workerA);
    expect(claimA).not.toBeNull();

    const okA = await completeAttempt(
      pool,
      taskA,
      claimA!.attemptId,
      workerA,
      claimA!.fencingToken,
      { task: "A" },
    );
    expect(okA).toBe(true);
    expect(await workflowStatus(workflowId)).toBe("RUNNING");
    expect(await completionEventCount(workflowId)).toBe(0);

    const workerB = crypto.randomUUID();
    const claimB = await claimTaskForTest(pool, taskB, workerB);
    expect(claimB).not.toBeNull();

    const okB = await completeAttempt(
      pool,
      taskB,
      claimB!.attemptId,
      workerB,
      claimB!.fencingToken,
      { task: "B" },
    );
    expect(okB).toBe(true);
    expect(await workflowStatus(workflowId)).toBe("COMPLETED");
    expect(await completionEventCount(workflowId)).toBe(1);

    const events = await pool.query(
      `
      SELECT sequence, event_type, data
      FROM workflow_events
      WHERE workflow_id = $1
      ORDER BY sequence
      `,
      [workflowId],
    );
    const completedRows = events.rows.filter(
      (row) => row.event_type === "WORKFLOW_COMPLETED",
    );
    expect(completedRows).toHaveLength(1);
    expect(completedRows[0].data).toEqual({ workflowId });
  });

  test("stale or invalid attempt completion commits nothing", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_STALE",
      name: "Stale fence",
      version: 1,
      tasks: [{ id: "A", name: "Task A", type: "NOOP" }],
    });

    const keys = await getTaskIdsByKey(workflowId);
    const taskA = keys.get("A")!;
    const workerId = crypto.randomUUID();
    const claim = await claimTaskForTest(pool, taskA, workerId);
    expect(claim).not.toBeNull();

    const wrongWorker = crypto.randomUUID();
    const rejected = await completeAttempt(
      pool,
      taskA,
      claim!.attemptId,
      wrongWorker,
      claim!.fencingToken,
      { ok: true },
    );
    expect(rejected).toBe(false);
    expect(await taskStatus(taskA)).toBe("RUNNING");
    expect(await workflowStatus(workflowId)).toBe("RUNNING");
    expect(await completionEventCount(workflowId)).toBe(0);

    await pool.query(
      `
      UPDATE task_attempts
      SET lease_until = NOW() - INTERVAL '1 minute'
      WHERE id = $1
      `,
      [claim!.attemptId],
    );

    const expired = await completeAttempt(
      pool,
      taskA,
      claim!.attemptId,
      workerId,
      claim!.fencingToken,
      { ok: true },
    );
    expect(expired).toBe(false);
    expect(await taskStatus(taskA)).toBe("RUNNING");
    expect(await workflowStatus(workflowId)).toBe("RUNNING");
    expect(await completionEventCount(workflowId)).toBe(0);
  });

  test("empty workflow: updateWorkflowStatus does not complete", async () => {
    const workflowId = crypto.randomUUID();
    const client = await pool.connect();

    try {
      await client.query("BEGIN");

      await client.query(
        `
        INSERT INTO workflows (id, type, name, version, status)
        VALUES ($1, 'EMPTY', 'No tasks', 1, 'RUNNING')
        `,
        [workflowId],
      );

      await client.query(
        `
        INSERT INTO workflow_events (
          id, workflow_id, sequence, event_type, data
        )
        VALUES ($1, $2, 1, 'WORKFLOW_STARTED', $3)
        `,
        [
          crypto.randomUUID(),
          workflowId,
          JSON.stringify({ workflowType: "EMPTY" }),
        ],
      );

      const completed = await updateWorkflowStatus(
        client as PoolClient,
        workflowId,
      );
      expect(completed).toBe(false);

      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }

    expect(await workflowStatus(workflowId)).toBe("RUNNING");
    expect(await completionEventCount(workflowId)).toBe(0);

    const row = await pool.query(
      `SELECT id, status FROM workflows WHERE id = $1`,
      [workflowId],
    );
    expect(row.rows[0]).toEqual({ id: workflowId, status: "RUNNING" });
  });
});

if (!hasDb) {
  test("workflow completion integration requires Postgres", () => {
    console.warn(
      "Start engine infra: cd engine && docker compose up -d && bun run migrate",
    );
    expect(true).toBe(true);
  });
}
