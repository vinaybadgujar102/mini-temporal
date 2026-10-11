import crypto from "node:crypto";
import type { PoolClient } from "pg";

type CompensationDefinition = {
  type: string;
  getInput: (task: {
    operation_id: string;
    result: unknown;
  }) => Record<string, unknown>;
};

const compensationRegistry: Record<string, CompensationDefinition> = {
  CHARGE_PAYMENT: {
    type: "REFUND_PAYMENT",
    getInput: (task) => ({
      paymentOperationId: task.operation_id,
    }),
  },
};

export async function startCompensation(
  client: PoolClient,
  workflowId: string,
  failedTaskId: string,
  error: { message: string; code?: string },
) {
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
    throw new Error(`Workflow not found: ${workflowId}`);
  }

  const workflowStatus = workflowResult.rows[0].status;

  if (workflowStatus === "COMPENSATING") {
    return { outcome: "ALREADY_COMPENSATING" as const };
  }

  if (workflowStatus !== "RUNNING") {
    return {
      outcome: "WORKFLOW_TERMINAL" as const,
      status: workflowStatus as string,
    };
  }

  const tasksResult = await client.query(
    `
    SELECT id, type, operation_id, result
    FROM tasks
    WHERE workflow_id = $1
      AND status = 'COMPLETED'
    FOR UPDATE
    `,
    [workflowId],
  );

  const toCompensate = tasksResult.rows.filter(
    (task) => compensationRegistry[task.type] != null,
  );

  const sequenceResult = await client.query(
    `
    SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence
    FROM workflow_events
    WHERE workflow_id = $1
    `,
    [workflowId],
  );

  let sequence = Number(sequenceResult.rows[0].sequence);

  // Nothing to undo: terminal failure, same as pre-saga behavior.
  if (toCompensate.length === 0) {
    await client.query(
      `
      UPDATE workflows
      SET status = 'FAILED',
          updated_at = NOW()
      WHERE id = $1
        AND status = 'RUNNING'
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
        sequence,
        "WORKFLOW_FAILED",
        JSON.stringify({ workflowId, failedTaskId, error }),
      ],
    );

    return {
      outcome: "FAILED_NO_COMPENSATION" as const,
      compensationRecordsConsidered: 0,
    };
  }

  await client.query(
    `
    UPDATE workflows
    SET status = 'COMPENSATING',
        updated_at = NOW()
    WHERE id = $1
      AND status = 'RUNNING'
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
      sequence++,
      "WORKFLOW_COMPENSATION_STARTED",
      JSON.stringify({ workflowId, failedTaskId, error }),
    ],
  );

  for (const task of toCompensate) {
    const definition = compensationRegistry[task.type]!;
    const idempotencyKey = `compensate:${workflowId}:${task.id}:${definition.type}`;

    await client.query(
      `
      INSERT INTO compensation_operations (
        workflow_id,
        task_id,
        idempotency_key,
        compensation_type,
        original_operation_id,
        input,
        status
      )
      VALUES ($1, $2, $3, $4, $5, $6::jsonb, 'PENDING')
      ON CONFLICT (idempotency_key) DO NOTHING
      `,
      [
        workflowId,
        task.id,
        idempotencyKey,
        definition.type,
        task.operation_id,
        JSON.stringify(
          definition.getInput({
            operation_id: task.operation_id,
            result: task.result,
          }),
        ),
      ],
    );
  }

  return {
    outcome: "COMPENSATION_STARTED" as const,
    compensationRecordsConsidered: toCompensate.length,
  };
}
