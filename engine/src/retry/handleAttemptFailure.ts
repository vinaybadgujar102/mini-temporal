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
    const taskRow = await client.query(
      `
      SELECT workflow_id
      FROM tasks
      WHERE id = $1
      `,
      [taskId],
    );

    if (taskRow.rowCount !== 1) {
      await client.query("ROLLBACK");
      return { outcome: "STALE_ATTEMPT" as const };
    }

    const resolvedWorkflowId = taskRow.rows[0].workflow_id as string;

    if (resolvedWorkflowId !== workflowId) {
      await client.query("ROLLBACK");
      return { outcome: "STALE_ATTEMPT" as const };
    }

    const workflowResult = await client.query(
      `
      SELECT status
      FROM workflows
      WHERE id = $1
      FOR UPDATE
      `,
      [resolvedWorkflowId],
    );

    if (
      workflowResult.rowCount !== 1 ||
      workflowResult.rows[0].status !== "RUNNING"
    ) {
      await client.query("ROLLBACK");
      return { outcome: "WORKFLOW_TERMINAL" as const };
    }

    const taskStatusResult = await client.query(
      `
      SELECT id
      FROM tasks
      WHERE id = $1
        AND workflow_id = $2
        AND status = 'RUNNING'
      FOR UPDATE
      `,
      [taskId, resolvedWorkflowId],
    );

    if (taskStatusResult.rowCount !== 1) {
      await client.query("ROLLBACK");
      return { outcome: "STALE_ATTEMPT" as const };
    }

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

    if (shouldRetry) {
      // 6a. Retryable failure (only while workflow lock shows RUNNING).
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

      const sequenceResult = await client.query(
        `
        SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence
        FROM workflow_events
        WHERE workflow_id = $1
        `,
        [resolvedWorkflowId],
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
          resolvedWorkflowId,
          Number(sequenceResult.rows[0].sequence),
          "TASK_RETRY_SCHEDULED",
          JSON.stringify({ taskId, attemptNumber, delayMs, error }),
        ],
      );

      await client.query("COMMIT");

      return {
        outcome: "RETRY_SCHEDULED" as const,
        delayMs,
      };
    }

    // 6b. Permanent failure: fail the task and workflow atomically.
    await client.query(
      `
      UPDATE tasks
      SET status = 'FAILED',
          error = $2::jsonb,
          next_retry_at = NULL,
          updated_at = NOW()
      WHERE id = $1
        AND workflow_id = $3
        AND status = 'RUNNING'
      `,
      [taskId, JSON.stringify(error), resolvedWorkflowId],
    );

    const workflowUpdate = await client.query(
      `
      UPDATE workflows
      SET status = 'FAILED',
          updated_at = NOW()
      WHERE id = $1
        AND status = 'RUNNING'
      RETURNING id
      `,
      [resolvedWorkflowId],
    );

    if (workflowUpdate.rowCount !== 1) {
      throw new Error(
        `Could not transition workflow ${resolvedWorkflowId} to FAILED`,
      );
    }

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
      [
        resolvedWorkflowId,
        JSON.stringify({ reason: "WORKFLOW_FAILED" }),
      ],
    );

    await client.query(
      `
      UPDATE tasks
      SET status = 'CANCELLED',
          next_retry_at = NULL,
          updated_at = NOW()
      WHERE workflow_id = $1
        AND status = 'RUNNING'
        AND id <> $2
      `,
      [resolvedWorkflowId, taskId],
    );

    await client.query(
      `
      UPDATE tasks
      SET status = 'CANCELLED',
          next_retry_at = NULL,
          updated_at = NOW()
      WHERE workflow_id = $1
        AND status IN ('PENDING', 'READY')
      `,
      [resolvedWorkflowId],
    );

    const sequenceResult = await client.query(
      `
      SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence
      FROM workflow_events
      WHERE workflow_id = $1
      `,
      [resolvedWorkflowId],
    );

    let sequence = Number(sequenceResult.rows[0].sequence);

    await client.query(
      `
      INSERT INTO workflow_events (
        id, workflow_id, sequence, event_type, data
      )
      VALUES ($1, $2, $3, $4, $5::jsonb)
      `,
      [
        crypto.randomUUID(),
        resolvedWorkflowId,
        sequence++,
        "TASK_FAILED",
        JSON.stringify({ taskId, attemptNumber, error }),
      ],
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
        resolvedWorkflowId,
        sequence,
        "WORKFLOW_FAILED",
        JSON.stringify({ workflowId: resolvedWorkflowId, taskId, error }),
      ],
    );

    await client.query("COMMIT");

    return { outcome: "FAILED" as const };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}
