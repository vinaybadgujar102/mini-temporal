import crypto from "node:crypto";
import { pool } from "../db/client";

export async function recoverStaleTasks() {
  const client = await pool.connect();

  try {
    const candidates = await client.query(`
      SELECT
        t.id AS task_id,
        t.workflow_id
      FROM tasks t
      JOIN workflows w ON w.id = t.workflow_id
      JOIN task_attempts a
        ON a.task_id = t.id
       AND a.attempt_number = t.attempt_count
      WHERE w.status = 'RUNNING'
        AND t.status = 'RUNNING'
        AND a.status = 'RUNNING'
        AND a.lease_until < NOW()
    `);

    await client.query("BEGIN");

    let recovered = 0;

    for (const candidate of candidates.rows) {
      // Lock order: workflow → task → attempt (same as cancel, claim, completion, failure).
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

      const taskLock = await client.query(
        `
        SELECT
          id AS task_id,
          workflow_id,
          operation_id,
          type,
          attempt_count
        FROM tasks
        WHERE id = $1
          AND workflow_id = $2
          AND status = 'RUNNING'
        FOR UPDATE
        `,
        [candidate.task_id, candidate.workflow_id],
      );

      if (taskLock.rowCount !== 1) {
        continue;
      }

      const lockedTask = taskLock.rows[0];

      const attemptLock = await client.query(
        `
        SELECT
          id AS attempt_id,
          attempt_number,
          fencing_token
        FROM task_attempts
        WHERE task_id = $1
          AND attempt_number = $2
          AND status = 'RUNNING'
          AND lease_until < NOW()
        FOR UPDATE
        `,
        [lockedTask.task_id, lockedTask.attempt_count],
      );

      if (attemptLock.rowCount !== 1) {
        continue;
      }

      const task = {
        ...lockedTask,
        ...attemptLock.rows[0],
      };

      console.log(`[recovery] recovering ${task.task_id}`);

      await client.query(
        `
        UPDATE task_attempts
        SET
          status = 'TIMED_OUT',
          error = $1,
          completed_at = NOW()
        WHERE id = $2
          AND status = 'RUNNING'
        `,
        [
          JSON.stringify({
            reason: "LEASE_EXPIRED",
          }),
          task.attempt_id,
        ],
      );

      await client.query(
        `
        UPDATE tasks
        SET
          status = 'READY',
          updated_at = NOW()
        WHERE id = $1
          AND status = 'RUNNING'
        `,
        [task.task_id],
      );

      const sequenceResult = await client.query(
        `
        SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence
        FROM workflow_events
        WHERE workflow_id = $1
        `,
        [task.workflow_id],
      );

      const sequence = Number(sequenceResult.rows[0].sequence);

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
          task.workflow_id,
          sequence,
          "TASK_RECOVERY",
          JSON.stringify({
            taskId: task.task_id,
            previousAttemptId: task.attempt_id,
            previousAttempt: task.attempt_number,
            previousFencingToken: task.fencing_token,
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
          task.workflow_id,
          task.task_id,
          "TASK_READY",
          JSON.stringify({
            workflowId: task.workflow_id,
            taskId: task.task_id,
            taskType: task.type,
            operationId: task.operation_id,
          }),
        ],
      );

      recovered++;
    }

    await client.query("COMMIT");

    console.log(`[recovery] recovered ${recovered} tasks`);
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function main() {
  await recoverStaleTasks();
  await pool.end();
}

if (import.meta.main) {
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
