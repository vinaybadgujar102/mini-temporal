import type { PoolClient } from "pg";

export async function updateWorkflowStatus(
  client: PoolClient,
  workflowId: string,
): Promise<boolean> {
  const completionResult = await client.query(
    `
    SELECT
      COUNT(*) AS total_tasks,
      COUNT(*) FILTER (
        WHERE status <> 'COMPLETED'
      ) AS incomplete_tasks
    FROM tasks
    WHERE workflow_id = $1
    `,
    [workflowId],
  );

  const { total_tasks, incomplete_tasks } = completionResult.rows[0];

  if (Number(total_tasks) === 0 || Number(incomplete_tasks) !== 0) {
    return false;
  }

  const updateResult = await client.query(
    `
    UPDATE workflows
    SET status = 'COMPLETED'
    WHERE id = $1
      AND status = 'RUNNING'
    RETURNING id
    `,
    [workflowId],
  );

  if (updateResult.rowCount === 0) {
    return false;
  }

  const sequenceResult = await client.query(
    `
    SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence
    FROM workflow_events
    WHERE workflow_id = $1
    `,
    [workflowId],
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
      workflowId,
      sequence,
      "WORKFLOW_COMPLETED",
      JSON.stringify({ workflowId }),
    ],
  );

  return true;
}
