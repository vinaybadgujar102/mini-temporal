import { pool } from "../db/client";

export async function publishDueRetries() {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const due = await client.query(
      `
      SELECT id, workflow_id
      FROM tasks
      WHERE status = 'READY'
        AND next_retry_at IS NOT NULL
        AND next_retry_at <= NOW()
      ORDER BY next_retry_at
      FOR UPDATE SKIP LOCKED
      LIMIT 100
      `,
    );

    for (const task of due.rows) {
      await client.query(
        `
        INSERT INTO outbox (
          id, workflow_id, task_id, event_type, payload
        )
        VALUES (
          gen_random_uuid(),
          $1,
          $2,
          'TASK_READY',
          jsonb_build_object('taskId', $2)
        )
        `,
        [task.workflow_id, task.id],
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
    }

    await client.query("COMMIT");
    return due.rowCount ?? 0;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
