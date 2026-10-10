import crypto from "node:crypto";
import type { PoolClient } from "pg";

export async function cancelWorkflow(
  pool: { connect: () => Promise<PoolClient> },
  workflowId: string,
) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const workflowResult = await client.query(
      `
      SELECT status
      FROM workflows
      WHERE id = $1
      FOR UPDATE
      `,
      [workflowId],
    );

    if (workflowResult.rowCount !== 1) {
      await client.query("ROLLBACK");
      return { outcome: "NOT_FOUND" as const };
    }

    if (workflowResult.rows[0].status !== "RUNNING") {
      await client.query("ROLLBACK");
      return {
        outcome: "ALREADY_TERMINAL" as const,
        status: workflowResult.rows[0].status as string,
      };
    }

    await client.query(
      `
      UPDATE workflows
      SET status = 'CANCELLED',
          updated_at = NOW()
      WHERE id = $1
        AND status = 'RUNNING'
      `,
      [workflowId],
    );

    // Invalidate running attempts so their heartbeats and
    // fenced completions cannot succeed.
    await client.query(
      `
      UPDATE task_attempts a
      SET status = 'TIMED_OUT',
          error = $2::jsonb,
          completed_at = NOW()
      FROM tasks t
      WHERE t.id = a.task_id
        AND t.workflow_id = $1
        AND t.status = 'RUNNING'
        AND a.status = 'RUNNING'
      `,
      [workflowId, JSON.stringify({ reason: "WORKFLOW_CANCELLED" })],
    );

    // No task in a cancelled workflow may remain executable.
    await client.query(
      `
      UPDATE tasks
      SET status = 'CANCELLED',
          next_retry_at = NULL,
          updated_at = NOW()
      WHERE workflow_id = $1
        AND status IN ('PENDING', 'READY', 'RUNNING')
      `,
      [workflowId],
    );

    // Remove queued, not-yet-published task dispatches.
    // Already-published Kafka messages are rejected by claimTask().
    await client.query(
      `
      DELETE FROM outbox
      WHERE workflow_id = $1
        AND event_type = 'TASK_READY'
        AND published_at IS NULL
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

    await client.query(
      `
      INSERT INTO workflow_events (
        id, workflow_id, sequence, event_type, data
      )
      VALUES ($1, $2, $3, $4, $5::jsonb)
      `,
      [
        crypto.randomUUID(),
        workflowId,
        Number(sequenceResult.rows[0].sequence),
        "WORKFLOW_CANCELLED",
        JSON.stringify({ workflowId }),
      ],
    );

    await client.query("COMMIT");

    return { outcome: "CANCELLED" as const };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
