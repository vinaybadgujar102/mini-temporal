import crypto from "node:crypto";
import type { PoolClient } from "pg";
import {
  calculateRetryDelay,
  defaultRetryPolicy,
  isRetryable,
} from "./retryPolicy";
import { startCompensation } from "../workflow/startCompensation";

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

    // 6b. Permanent failure: fail the task and begin compensation.
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

    // Invalidate other running attempts.
    await client.query(
      `
      UPDATE task_attempts a
      SET status = 'TIMED_OUT',
          error = $2::jsonb,
          completed_at = NOW()
      FROM tasks t
      WHERE t.id = a.task_id
        AND t.workflow_id = $1
        AND t.id <> $3
        AND t.status = 'RUNNING'
        AND a.status = 'RUNNING'
      `,
      [
        resolvedWorkflowId,
        JSON.stringify({ reason: "WORKFLOW_COMPENSATING" }),
        taskId,
      ],
    );

    // Cancel all other executable tasks.
    await client.query(
      `
      UPDATE tasks
      SET status = 'CANCELLED',
          next_retry_at = NULL,
          updated_at = NOW()
      WHERE workflow_id = $1
        AND id <> $2
        AND status IN ('PENDING', 'READY', 'RUNNING')
      `,
      [resolvedWorkflowId, taskId],
    );

    // Record the failed task event.
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
        "TASK_FAILED",
        JSON.stringify({ taskId, attemptNumber, error }),
      ],
    );

    // Transitions RUNNING -> COMPENSATING and creates durable
    // compensation intents in this same transaction.
    const compensation = await startCompensation(
      client,
      resolvedWorkflowId,
      taskId,
      error,
    );

    await client.query("COMMIT");

    return {
      outcome: "FAILED" as const,
      compensation,
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}
