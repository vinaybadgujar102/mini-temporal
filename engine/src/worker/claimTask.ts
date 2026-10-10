import crypto from "node:crypto";
import type { Pool } from "pg";
import { pool as defaultPool } from "../db/client";

const DEFAULT_LEASE_SECONDS = 30;

export type ClaimedTask = {
  id: string;
  workflow_id: string;
  type: string;
  operation_id: string;
  attempt_count: number;
};

export type ClaimedAttempt = {
  id: string;
  attempt_number: number;
  fencing_token: number;
  worker_id: string;
};

export async function claimTask(
  taskId: string,
  workerId: string,
  pool: Pool = defaultPool,
  leaseSeconds = DEFAULT_LEASE_SECONDS,
): Promise<{
  task: ClaimedTask;
  attempt: ClaimedAttempt;
} | null> {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const parentResult = await client.query(
      `SELECT workflow_id FROM tasks WHERE id = $1`,
      [taskId],
    );

    if (parentResult.rowCount !== 1) {
      await client.query("ROLLBACK");
      return null;
    }

    const workflowId: string = parentResult.rows[0].workflow_id;

    const workflowResult = await client.query(
      `
      SELECT status
      FROM workflows
      WHERE id = $1
      FOR UPDATE
      `,
      [workflowId],
    );

    if (
      workflowResult.rowCount !== 1 ||
      workflowResult.rows[0].status !== "RUNNING"
    ) {
      await client.query("ROLLBACK");
      return null;
    }

    const taskResult = await client.query(
      `
      UPDATE tasks
      SET
        status = 'RUNNING',
        attempt_count = attempt_count + 1,
        updated_at = NOW()
      WHERE id = $1
        AND status = 'READY'
        AND workflow_id = $2
        AND (next_retry_at IS NULL OR next_retry_at <= NOW())
      RETURNING
        id,
        workflow_id,
        type,
        operation_id,
        attempt_count;
      `,
      [taskId, workflowId],
    );

    if (taskResult.rowCount === 0) {
      await client.query("ROLLBACK");

      return null;
    }

    const task = taskResult.rows[0] as ClaimedTask;

    const fencingToken = task.attempt_count;

    const attemptId = crypto.randomUUID();

    const attemptResult = await client.query(
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
      RETURNING
        id,
        attempt_number,
        fencing_token,
        worker_id;
      `,
      [
        attemptId,
        task.id,
        task.attempt_count,
        workerId,
        fencingToken,
        leaseSeconds,
      ],
    );

    await client.query("COMMIT");

    return {
      task,
      attempt: attemptResult.rows[0],
    };
  } catch (error) {
    await client.query("ROLLBACK");

    throw error;
  } finally {
    client.release();
  }
}
