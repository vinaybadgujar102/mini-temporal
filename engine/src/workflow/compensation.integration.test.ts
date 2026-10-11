import { describe, expect, test } from "bun:test";
import { pool } from "../db/client";
import { recoverStaleCompensations } from "../recovery/recoverStaleCompensations";
import { recoverStaleTasks } from "../recovery/recoverStaleTasks";
import { runCompensationOnce } from "../worker/compensationWorker";
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

async function seedSucceededPayment(operationId: string, amount = 100) {
  await pool.query(
    `
    INSERT INTO payment_operations (
      idempotency_key,
      transaction_id,
      amount,
      status
    )
    VALUES ($1, $2, $3, 'SUCCEEDED')
    ON CONFLICT (idempotency_key) DO NOTHING
    `,
    [operationId, crypto.randomUUID(), amount],
  );
}

async function compensationOps(workflowId: string) {
  const result = await pool.query(
    `
    SELECT id, status, attempt_count, fencing_token, error, lease_expires_at
    FROM compensation_operations
    WHERE workflow_id = $1
    ORDER BY created_at
    `,
    [workflowId],
  );
  return result.rows;
}

async function eventTypes(workflowId: string): Promise<string[]> {
  const result = await pool.query(
    `
    SELECT event_type
    FROM workflow_events
    WHERE workflow_id = $1
    ORDER BY sequence
    `,
    [workflowId],
  );
  return result.rows.map((row) => String(row.event_type));
}

async function runCompensationForWorkflow(
  workflowId: string,
  maxAttempts = 20,
): Promise<void> {
  for (let i = 0; i < maxAttempts; i++) {
    const ops = await compensationOps(workflowId);
    if (ops.length === 0) return;
    if (ops.every((op) => op.status === "COMPLETED" || op.status === "FAILED")) {
      return;
    }

    const ran = await runCompensationOnce();
    if (!ran) {
      // Another worker may have finished; re-check our rows.
      continue;
    }
  }

  throw new Error(
    `Compensation for workflow ${workflowId} did not finish in ${maxAttempts} attempts`,
  );
}

async function startChargeThenFailWorkflow() {
  const workflowId = await startWorkflow({
    type: "TEST_SAGA",
    name: "Charge then fail",
    version: 1,
    tasks: [
      { id: "charge", name: "Charge", type: "CHARGE_PAYMENT" },
      { id: "ship", name: "Ship", type: "NOOP", dependsOn: ["charge"] },
    ],
  });

  const keys = await getTaskIdsByKey(workflowId);
  const chargeId = keys.get("charge")!;
  const shipId = keys.get("ship")!;
  const operationId = `${workflowId}:charge`;

  await seedSucceededPayment(operationId);

  const chargeWorker = crypto.randomUUID();
  const chargeClaim = await claimTaskForTest(pool, chargeId, chargeWorker);
  expect(chargeClaim).not.toBeNull();

  const charged = await completeAttempt(
    pool,
    chargeId,
    chargeClaim!.attemptId,
    chargeWorker,
    chargeClaim!.fencingToken,
    { charged: true },
  );
  expect(charged).toBe(true);
  expect(await taskStatus(shipId)).toBe("READY");

  const shipWorker = crypto.randomUUID();
  const shipClaim = await claimTaskForTest(pool, shipId, shipWorker);
  expect(shipClaim).not.toBeNull();

  const failed = await failAttemptForTest(
    pool,
    shipId,
    shipClaim!.attemptId,
    shipWorker,
    shipClaim!.fencingToken,
    workflowId,
    { message: "ship permanently failed" },
  );

  return { workflowId, chargeId, shipId, operationId, failed };
}

describe.skipIf(!hasDb)("saga compensation (integration)", () => {
  test("permanent failure with completed charge starts compensation", async () => {
    const { workflowId, failed } = await startChargeThenFailWorkflow();

    expect(failed.outcome).toBe("FAILED");
    expect(await workflowStatus(workflowId)).toBe("COMPENSATING");

    const ops = await compensationOps(workflowId);
    expect(ops).toHaveLength(1);
    expect(ops[0].status).toBe("PENDING");

    const types = await eventTypes(workflowId);
    expect(types).toContain("TASK_FAILED");
    expect(types).toContain("WORKFLOW_COMPENSATION_STARTED");
    expectUniqueSequences(await eventSequences(workflowId));
  });

  test("compensation success refunds payment and fails workflow", async () => {
    const { workflowId, operationId } = await startChargeThenFailWorkflow();

    await runCompensationForWorkflow(workflowId);

    const ops = await compensationOps(workflowId);
    expect(ops).toHaveLength(1);
    expect(ops[0].status).toBe("COMPLETED");
    expect(await workflowStatus(workflowId)).toBe("FAILED");

    const payment = await pool.query(
      `SELECT status FROM payment_operations WHERE idempotency_key = $1`,
      [operationId],
    );
    expect(payment.rows[0].status).toBe("REFUNDED");

    const types = await eventTypes(workflowId);
    expect(types).toContain("COMPENSATION_COMPLETED");
    expect(types).toContain("WORKFLOW_FAILED");
    expectUniqueSequences(await eventSequences(workflowId));
  });

  test("compensation failure guard records FAILED (status column, not missing)", async () => {
    const { workflowId, operationId } = await startChargeThenFailWorkflow();

    // Force refund to fail so recordCompensationFailure runs.
    await pool.query(
      `DELETE FROM payment_operations WHERE idempotency_key = $1`,
      [operationId],
    );

    await runCompensationForWorkflow(workflowId);

    const ops = await compensationOps(workflowId);
    expect(ops).toHaveLength(1);
    // Bug was SELECT id then check .status → always undefined → always ROLLBACK.
    expect(ops[0].status).toBe("FAILED");
    expect(ops[0].error).not.toBeNull();
    expect(await workflowStatus(workflowId)).toBe("COMPENSATING");
  });

  test("retryable task failure schedules retry under workflow lock", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_RETRY",
      name: "Retry once",
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
      { code: "503", message: "transient" },
    );

    expect(result.outcome).toBe("RETRY_SCHEDULED");
    expect(await taskStatus(taskA)).toBe("READY");
    expect(await workflowStatus(workflowId)).toBe("RUNNING");

    const types = await eventTypes(workflowId);
    expect(types).toContain("TASK_RETRY_SCHEDULED");
    expectUniqueSequences(await eventSequences(workflowId));
  });

  test("concurrent completions: one WORKFLOW_COMPLETED and unique sequences", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_CONCURRENT_COMPLETE",
      name: "Concurrent A and B",
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

    const [okA, okB] = await Promise.all([
      completeAttempt(
        pool,
        taskA,
        claimA!.attemptId,
        workerA,
        claimA!.fencingToken,
        { task: "A" },
      ),
      completeAttempt(
        pool,
        taskB,
        claimB!.attemptId,
        workerB,
        claimB!.fencingToken,
        { task: "B" },
      ),
    ]);

    expect(okA).toBe(true);
    expect(okB).toBe(true);
    expect(await workflowStatus(workflowId)).toBe("COMPLETED");

    const completed = await pool.query(
      `
      SELECT COUNT(*)::int AS count
      FROM workflow_events
      WHERE workflow_id = $1
        AND event_type = 'WORKFLOW_COMPLETED'
      `,
      [workflowId],
    );
    expect(completed.rows[0].count).toBe(1);
    expectUniqueSequences(await eventSequences(workflowId));
  });

  test("cancel while compensating is ALREADY_TERMINAL", async () => {
    const { workflowId } = await startChargeThenFailWorkflow();
    expect(await workflowStatus(workflowId)).toBe("COMPENSATING");

    const cancel = await cancelWorkflow(pool, workflowId);
    expect(cancel.outcome).toBe("ALREADY_TERMINAL");
    if (cancel.outcome === "ALREADY_TERMINAL") {
      expect(cancel.status).toBe("COMPENSATING");
    }
    expect(await workflowStatus(workflowId)).toBe("COMPENSATING");
  });

  test("stale compensation worker recovery requeues with unique sequences", async () => {
    const { workflowId } = await startChargeThenFailWorkflow();

    const claimed = await pool.query(
      `
      UPDATE compensation_operations
      SET status = 'RUNNING',
          attempt_count = attempt_count + 1,
          fencing_token = fencing_token + 1,
          lease_expires_at = NOW() - INTERVAL '1 minute',
          updated_at = NOW()
      WHERE workflow_id = $1
        AND status = 'PENDING'
      RETURNING id
      `,
      [workflowId],
    );
    expect(claimed.rowCount).toBe(1);

    const recovered = await recoverStaleCompensations();
    expect(recovered).toBeGreaterThanOrEqual(1);

    const ops = await compensationOps(workflowId);
    expect(ops[0].status).toBe("READY");
    expect(ops[0].lease_expires_at).toBeNull();

    const types = await eventTypes(workflowId);
    expect(types).toContain("COMPENSATION_LEASE_EXPIRED");
    expectUniqueSequences(await eventSequences(workflowId));
  });

  test("stale task recovery skips COMPENSATING workflows", async () => {
    const { workflowId, shipId } = await startChargeThenFailWorkflow();
    expect(await taskStatus(shipId)).toBe("FAILED");

    const outboxBefore = await pool.query(
      `
      SELECT COUNT(*)::int AS count
      FROM outbox
      WHERE workflow_id = $1 AND event_type = 'TASK_READY'
      `,
      [workflowId],
    );

    await recoverStaleTasks();

    expect(await workflowStatus(workflowId)).toBe("COMPENSATING");
    expect(await taskStatus(shipId)).toBe("FAILED");
    const outboxAfter = await pool.query(
      `
      SELECT COUNT(*)::int AS count
      FROM outbox
      WHERE workflow_id = $1 AND event_type = 'TASK_READY'
      `,
      [workflowId],
    );
    expect(outboxAfter.rows[0].count).toBe(outboxBefore.rows[0].count);

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

  test("race cancel vs compensation start: terminal or compensating, unique sequences", async () => {
    const workflowId = await startWorkflow({
      type: "TEST_CANCEL_VS_SAGA",
      name: "Cancel vs saga",
      version: 1,
      tasks: [
        { id: "charge", name: "Charge", type: "CHARGE_PAYMENT" },
        { id: "ship", name: "Ship", type: "NOOP", dependsOn: ["charge"] },
      ],
    });

    const keys = await getTaskIdsByKey(workflowId);
    const chargeId = keys.get("charge")!;
    const shipId = keys.get("ship")!;
    const operationId = `${workflowId}:charge`;
    await seedSucceededPayment(operationId);

    const chargeWorker = crypto.randomUUID();
    const chargeClaim = await claimTaskForTest(pool, chargeId, chargeWorker);
    expect(chargeClaim).not.toBeNull();
    await completeAttempt(
      pool,
      chargeId,
      chargeClaim!.attemptId,
      chargeWorker,
      chargeClaim!.fencingToken,
      { charged: true },
    );

    const shipWorker = crypto.randomUUID();
    const shipClaim = await claimTaskForTest(pool, shipId, shipWorker);
    expect(shipClaim).not.toBeNull();

    await Promise.all([
      cancelWorkflow(pool, workflowId),
      failAttemptForTest(
        pool,
        shipId,
        shipClaim!.attemptId,
        shipWorker,
        shipClaim!.fencingToken,
        workflowId,
        { message: "permanent" },
      ),
    ]);

    const status = await workflowStatus(workflowId);
    expect(["CANCELLED", "COMPENSATING", "FAILED"].includes(status)).toBe(true);
    expectUniqueSequences(await eventSequences(workflowId));
  });
});

if (!hasDb) {
  test("saga compensation integration requires Postgres", () => {
    console.warn(
      "Start engine infra: cd engine && docker compose up -d && bun run migrate",
    );
    expect(true).toBe(true);
  });
}
