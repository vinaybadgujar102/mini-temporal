import crypto from "node:crypto";
import { pool } from "../db/client";

export async function publishDueRetries() {
  const client = await pool.connect();

  try {
    const due = await client.query(
      `
      SELECT id, workflow_id
      FROM tasks
      WHERE status = 'READY'
        AND next_retry_at IS NOT NULL
        AND next_retry_at <= NOW()
      ORDER BY next_retry_at
      LIMIT 100
      `,
    );

    await client.query("BEGIN");

    let published = 0;

    for (const candidate of due.rows) {
      const workflowResult = await client.query(
        `
        SELECT status
        FROM workflows
        WHERE id = $1
        FOR UPDATE
        `,
        [candidate.workflow_id],
      );

      if (
        workflowResult.rowCount !== 1 ||
        workflowResult.rows[0].status !== "RUNNING"
      ) {
        continue;
      }

      const taskResult = await client.query(
        `
        SELECT id, workflow_id, type, operation_id
        FROM tasks
        WHERE id = $1
          AND workflow_id = $2
          AND status = 'READY'
          AND next_retry_at IS NOT NULL
          AND next_retry_at <= NOW()
        FOR UPDATE
        `,
        [candidate.id, candidate.workflow_id],
      );

      if (taskResult.rowCount !== 1) {
        continue;
      }

      const task = taskResult.rows[0];

      await client.query(
        `
        INSERT INTO outbox (
          id, workflow_id, task_id, event_type, payload
        )
        VALUES ($1, $2, $3, 'TASK_READY', $4::jsonb)
        `,
        [
          crypto.randomUUID(),
          task.workflow_id,
          task.id,
          JSON.stringify({
            workflowId: task.workflow_id,
            taskId: task.id,
            taskType: task.type,
            operationId: task.operation_id,
          }),
        ],
      );

      await client.query(
        `
        UPDATE tasks
        SET next_retry_at = NULL,
            updated_at = NOW()
        WHERE id = $1
        `,
        [task.id],
      );

      published++;
    }

    await client.query("COMMIT");
    return published;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
