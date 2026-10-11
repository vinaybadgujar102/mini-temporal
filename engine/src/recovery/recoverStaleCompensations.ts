import crypto from "node:crypto";
import { pool } from "../db/client";

export async function recoverStaleCompensations(): Promise<number> {
  const client = await pool.connect();
  let recovered = 0;

  try {
    await client.query("BEGIN");

    // Discovery only: do not lock compensation rows before workflow rows.
    const candidates = await client.query(
      `
      SELECT id, workflow_id
      FROM compensation_operations
      WHERE status = 'RUNNING'
        AND lease_expires_at <= NOW()
      ORDER BY lease_expires_at
      LIMIT 100
      `,
    );

    for (const candidate of candidates.rows) {
      const workflow = await client.query(
        `
        SELECT status
        FROM workflows
        WHERE id = $1
        FOR UPDATE
        `,
        [candidate.workflow_id],
      );

      if (
        workflow.rowCount !== 1 ||
        workflow.rows[0].status !== "COMPENSATING"
      ) {
        continue;
      }

      const operation = await client.query(
        `
        SELECT id, task_id, compensation_type
        FROM compensation_operations
        WHERE id = $1
          AND workflow_id = $2
          AND status = 'RUNNING'
          AND lease_expires_at <= NOW()
        FOR UPDATE
        `,
        [candidate.id, candidate.workflow_id],
      );

      // The lease may have been renewed or the operation may have finished
      // since candidate discovery.
      if (operation.rowCount !== 1) continue;

      await client.query(
        `
        UPDATE compensation_operations
        SET status = 'READY',
            lease_expires_at = NULL,
            next_retry_at = NOW(),
            error = $2::jsonb,
            updated_at = NOW()
        WHERE id = $1
          AND status = 'RUNNING'
          AND lease_expires_at <= NOW()
        `,
        [
          candidate.id,
          JSON.stringify({
            message: "Compensation worker lease expired",
            code: "COMPENSATION_LEASE_EXPIRED",
          }),
        ],
      );

      const sequenceResult = await client.query(
        `
        SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence
        FROM workflow_events
        WHERE workflow_id = $1
        `,
        [candidate.workflow_id],
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
          candidate.workflow_id,
          Number(sequenceResult.rows[0].sequence),
          "COMPENSATION_LEASE_EXPIRED",
          JSON.stringify({
            compensationId: operation.rows[0].id,
            taskId: operation.rows[0].task_id,
            type: operation.rows[0].compensation_type,
          }),
        ],
      );

      recovered++;
    }

    await client.query("COMMIT");
    return recovered;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
