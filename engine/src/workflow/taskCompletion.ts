import crypto from "node:crypto";
import type { Pool, PoolClient } from "pg";
import { updateWorkflowStatus } from "./updateWorkflowStatus";

function taskKeyFrom(workflowId: string, operationId: string) {
  const prefix = `${workflowId}:`;
  return operationId.startsWith(prefix)
    ? operationId.slice(prefix.length)
    : operationId;
}

export async function scheduleUnblockedDependents(
  client: PoolClient,
  workflowId: string,
  completedTaskId: string,
  sequenceStart: number,
) {
  let sequence = sequenceStart;

  const dependents = await client.query(
    `
    SELECT
      child.id,
      child.type,
      child.operation_id
    FROM task_dependencies d
    JOIN tasks child ON child.id = d.task_id
    WHERE d.depends_on_task_id = $1
      AND child.status = 'PENDING'
    `,
    [completedTaskId],
  );

  for (const child of dependents.rows) {
    const blocked = await client.query(
      `
      SELECT 1
      FROM task_dependencies d
      JOIN tasks dep ON dep.id = d.depends_on_task_id
      WHERE d.task_id = $1
        AND dep.status <> 'COMPLETED'
      LIMIT 1
      `,
      [child.id],
    );

    if ((blocked.rowCount ?? 0) > 0) continue;

    await client.query(
      `
      UPDATE tasks
      SET
        status = 'READY',
        updated_at = NOW()
      WHERE id = $1
        AND status = 'PENDING'
      `,
      [child.id],
    );

    const taskKey = taskKeyFrom(workflowId, String(child.operation_id));

    await client.query(
      `
      INSERT INTO workflow_events (
        id,
        workflow_id,
        sequence,
        event_type,
        data
      )
      VALUES ($1, $2, $3, $4, $5)
      `,
      [
        crypto.randomUUID(),
        workflowId,
        sequence++,
        "TASK_READY",
        JSON.stringify({
          taskId: child.id,
          taskKey,
          taskType: child.type,
        }),
      ],
    );

    await client.query(
      `
      INSERT INTO outbox (
        id,
        workflow_id,
        task_id,
        event_type,
        payload
      )
      VALUES ($1, $2, $3, $4, $5)
      `,
      [
        crypto.randomUUID(),
        workflowId,
        child.id,
        "TASK_READY",
        JSON.stringify({
          workflowId,
          taskId: child.id,
          taskKey,
          taskType: child.type,
          operationId: child.operation_id,
        }),
      ],
    );
  }

  const open = await client.query(
    `
    SELECT 1
    FROM tasks
    WHERE workflow_id = $1
      AND status <> 'COMPLETED'
    LIMIT 1
    `,
    [workflowId],
  );

  if ((open.rowCount ?? 0) > 0) return;

  await client.query(
    `
    UPDATE workflows
    SET
      status = 'COMPLETED',
      updated_at = NOW()
    WHERE id = $1
      AND status = 'RUNNING'
    `,
    [workflowId],
  );

  await client.query(
    `
    INSERT INTO workflow_events (
      id,
      workflow_id,
      sequence,
      event_type,
      data
    )
    VALUES ($1, $2, $3, $4, $5)
    `,
    [
      crypto.randomUUID(),
      workflowId,
      sequence++,
      "WORKFLOW_COMPLETED",
      JSON.stringify({ workflowId }),
    ],
  );
}

export async function completeAttempt(
  pool: Pool,
  taskId: string,
  attemptId: string,
  workerId: string,
  fencingToken: number,
  result: unknown,
): Promise<boolean> {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const attemptResult = await client.query(
      `
      UPDATE task_attempts
      SET
        status = 'COMPLETED',
        result = $1,
        completed_at = NOW()
      WHERE id = $2
        AND task_id = $3
        AND worker_id = $4
        AND fencing_token = $5
        AND status = 'RUNNING'
        AND lease_until > NOW()
      RETURNING id;
      `,
      [JSON.stringify(result), attemptId, taskId, workerId, fencingToken],
    );

    if (attemptResult.rowCount === 0) {
      await client.query("ROLLBACK");

      return false;
    }

    await client.query(
      `
      UPDATE tasks
      SET
        status = 'COMPLETED',
        result = $1,
        updated_at = NOW()
      WHERE id = $2
        AND status = 'RUNNING'
      `,
      [JSON.stringify(result), taskId],
    );

    const workflowResult = await client.query(
      `
      SELECT workflow_id
      FROM tasks
      WHERE id = $1
      `,
      [taskId],
    );

    if (workflowResult.rowCount === 0) {
      throw new Error(`Task not found: ${taskId}`);
    }

    const workflowId = workflowResult.rows[0].workflow_id;

    await client.query(
      `
      SELECT id
      FROM workflows
      WHERE id = $1
      FOR UPDATE
      `,
      [workflowId],
    );

    const sequenceResult = await client.query(
      `
      SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence
      FROM workflow_events
      WHERE workflow_id = $1
      `,
      [workflowId],
    );

    let sequence = Number(sequenceResult.rows[0].sequence);

    await client.query(
      `
      INSERT INTO workflow_events (
        id,
        workflow_id,
        sequence,
        event_type,
        data
      )
      VALUES ($1, $2, $3, $4, $5)
      `,
      [
        crypto.randomUUID(),
        workflowId,
        sequence++,
        "TASK_COMPLETED",
        JSON.stringify({
          taskId,
          attemptId,
          fencingToken,
          result,
        }),
      ],
    );

    await scheduleUnblockedDependents(client, workflowId, taskId, sequence);

    await updateWorkflowStatus(client, workflowId);

    await client.query("COMMIT");

    return true;
  } catch (error) {
    await client.query("ROLLBACK");

    throw error;
  } finally {
    client.release();
  }
}

export async function claimTaskForTest(
  pool: Pool,
  taskId: string,
  workerId: string,
  leaseSeconds = 30,
): Promise<{
  taskId: string;
  workflowId: string;
  attemptId: string;
  fencingToken: number;
} | null> {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const taskResult = await client.query(
      `
      UPDATE tasks
      SET
        status = 'RUNNING',
        attempt_count = attempt_count + 1,
        updated_at = NOW()
      WHERE id = $1
        AND status = 'READY'
        AND (next_retry_at IS NULL OR next_retry_at <= NOW())
      RETURNING id, workflow_id, attempt_count;
      `,
      [taskId],
    );

    if (taskResult.rowCount === 0) {
      await client.query("ROLLBACK");

      return null;
    }

    const row = taskResult.rows[0];
    const fencingToken = Number(row.attempt_count);
    const attemptId = crypto.randomUUID();

    await client.query(
      `
      INSERT INTO task_attempts (
        id,
        task_id,
        attempt_number,
        status,
        worker_id,
        fencing_token,
        lease_until,
        last_heartbeat_at,
        started_at
      )
      VALUES (
        $1,
        $2,
        $3,
        'RUNNING',
        $4,
        $5,
        NOW() + ($6 * INTERVAL '1 second'),
        NOW(),
        NOW()
      )
      `,
      [
        attemptId,
        row.id,
        fencingToken,
        workerId,
        fencingToken,
        leaseSeconds,
      ],
    );

    await client.query("COMMIT");

    return {
      taskId: row.id,
      workflowId: row.workflow_id,
      attemptId,
      fencingToken,
    };
  } catch (error) {
    await client.query("ROLLBACK");

    throw error;
  } finally {
    client.release();
  }
}
