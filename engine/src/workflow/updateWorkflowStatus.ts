import type { PoolClient } from "pg";
import { appendWorkflowEvent } from "./appendWorkflowEvent";

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

  await appendWorkflowEvent(client, {
    workflowId,
    eventType: "WORKFLOW_COMPLETED",
    data: { workflowId },
  });
  return true;
}
