import { pool } from "../db/client";
import { producer } from "../kafka/client";

const TOPIC = "workflow-tasks";

export async function publishOutbox() {
  const client = await pool.connect();

  try {
    const result = await client.query(`
      SELECT id, workflow_id, task_id, event_type, payload
      FROM outbox
      WHERE published_at IS NULL
      ORDER BY created_at
      LIMIT 100
    `);

    for (const row of result.rows) {
      await client.query("BEGIN");

      try {
        const workflowResult = await client.query(
          `
          SELECT status
          FROM workflows
          WHERE id = $1
          FOR UPDATE
          `,
          [row.workflow_id],
        );

        if (
          workflowResult.rowCount !== 1 ||
          workflowResult.rows[0].status !== "RUNNING"
        ) {
          if (row.event_type === "TASK_READY") {
            await client.query(
              `
              DELETE FROM outbox
              WHERE id = $1
                AND published_at IS NULL
              `,
              [row.id],
            );
          }

          await client.query("COMMIT");
          continue;
        }

        await producer.send({
          topic: TOPIC,
          messages: [
            {
              key: row.task_id,
              value: JSON.stringify({
                outboxId: row.id,
                workflowId: row.workflow_id,
                taskId: row.task_id,
                eventType: row.event_type,
                payload: row.payload,
              }),
            },
          ],
        });

        await client.query(
          `
          UPDATE outbox
          SET published_at = NOW()
          WHERE id = $1
            AND published_at IS NULL
          `,
          [row.id],
        );

        await client.query("COMMIT");
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      }
    }
  } finally {
    client.release();
  }
}
