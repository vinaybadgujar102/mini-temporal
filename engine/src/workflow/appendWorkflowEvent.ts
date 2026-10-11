import crypto from "node:crypto";
import type { PoolClient } from "pg";

type AppendWorkflowEventInput = {
  workflowId: string;
  eventType: string;
  data?: Record<string, unknown>;
};

export async function appendWorkflowEvent(
  client: PoolClient,
  { workflowId, eventType, data = {} }: AppendWorkflowEventInput,
): Promise<number> {
  // Caller must already hold the workflow row lock (or this acquires it).
  await client.query(
    `
    SELECT id
    FROM workflows
    WHERE id = $1
    FOR UPDATE
    `,
    [workflowId],
  );

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
    VALUES ($1, $2, $3, $4, $5::jsonb)
    `,
    [
      crypto.randomUUID(),
      workflowId,
      sequence,
      eventType,
      JSON.stringify(data),
    ],
  );

  return sequence;
}
