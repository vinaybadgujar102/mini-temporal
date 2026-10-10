import type { PoolClient } from "pg";
import {
  calculateRetryDelay,
  defaultRetryPolicy,
  isRetryable,
} from "./retryPolicy";

type FailureInput = {
  client: PoolClient;
  taskId: string;
  attemptId: string;
  workflowId: string;
  workerId: string;
  fencingToken: number;
  error: {
    code?: string;
    message: string;
  };
};

export async function handleAttemptFailure({
  client,
  taskId,
  attemptId,
  workflowId,
  workerId,
  fencingToken,
  error,
}: FailureInput) {
  const policy = defaultRetryPolicy;

  await client.query("BEGIN");

  try {
    const attemptResult = await client.query(
      `
      UPDATE task_attempts
      SET status = 'FAILED',
          error = $5::jsonb,
          completed_at = NOW()
      WHERE id = $1
        AND task_id = $2
        AND worker_id = $3
        AND fencing_token = $4
        AND status = 'RUNNING'
        AND lease_until > NOW()
      RETURNING attempt_number
      `,
      [attemptId, taskId, workerId, fencingToken, JSON.stringify(error)],
    );

    if (attemptResult.rowCount !== 1) {
      await client.query("ROLLBACK");
      return { outcome: "STALE_ATTEMPT" as const };
    }

    const attemptNumber: number = attemptResult.rows[0].attempt_number;

    const shouldRetry =
      isRetryable(error) && attemptNumber < policy.maxAttempts;

    if (!shouldRetry) {
      await client.query(
        `
        UPDATE tasks
        SET status = 'FAILED',
            error = $2::jsonb,
            next_retry_at = NULL,
            updated_at = NOW()
        WHERE id = $1
          AND status = 'RUNNING'
        `,
        [taskId, JSON.stringify(error)],
      );

      await client.query(
        `
        INSERT INTO workflow_events (
          id, workflow_id, sequence, event_type, data
        )
        SELECT
          gen_random_uuid(),
          $1,
          COALESCE(MAX(sequence), 0) + 1,
          'TASK_FAILED',
          $2::jsonb
        FROM workflow_events
        WHERE workflow_id = $1
        `,
        [
          workflowId,
          JSON.stringify({
            taskId,
            attemptNumber,
            error,
          }),
        ],
      );

      await client.query("COMMIT");
      return { outcome: "FAILED" as const };
    }

    const delayMs = calculateRetryDelay(attemptNumber, policy);

    await client.query(
      `
      UPDATE tasks
      SET status = 'READY',
          error = $2::jsonb,
          next_retry_at =
            NOW() + ($3::double precision * INTERVAL '1 millisecond'),
          updated_at = NOW()
      WHERE id = $1
        AND status = 'RUNNING'
      `,
      [taskId, JSON.stringify(error), delayMs],
    );

    await client.query(
      `
      INSERT INTO workflow_events (
        id, workflow_id, sequence, event_type, data
      )
      SELECT
        gen_random_uuid(),
        $1,
        COALESCE(MAX(sequence), 0) + 1,
        'TASK_RETRY_SCHEDULED',
        $2::jsonb
      FROM workflow_events
      WHERE workflow_id = $1
      `,
      [
        workflowId,
        JSON.stringify({
          taskId,
          attemptNumber,
          delayMs,
          error,
        }),
      ],
    );

    await client.query("COMMIT");

    return {
      outcome: "RETRY_SCHEDULED" as const,
      delayMs,
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}
