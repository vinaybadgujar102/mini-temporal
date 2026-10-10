import { pool } from "../db/client";

export type WorkflowTask = {
  id: string;
  name: string;
  type: string;
  dependsOn?: string[];
};

export type WorkflowDefinition = {
  type: string;
  name: string;
  version: number;
  tasks: WorkflowTask[];
};

export async function startWorkflow(
  definition: WorkflowDefinition,
): Promise<string> {
  const client = await pool.connect();

  const workflowId = crypto.randomUUID();
  const taskIds = new Map<string, string>();

  try {
    await client.query("BEGIN");

    await client.query(
      `
      INSERT INTO workflows (
        id,
        type,
        name,
        version,
        status
      )
      VALUES ($1, $2, $3, $4, 'RUNNING')
      `,
      [workflowId, definition.type, definition.name, definition.version],
    );

    for (const task of definition.tasks) {
      taskIds.set(task.id, crypto.randomUUID());
    }

    for (const task of definition.tasks) {
      const taskId = taskIds.get(task.id)!;
      const operationId = `${workflowId}:${task.id}`;
      const status =
        task.dependsOn && task.dependsOn.length > 0 ? "PENDING" : "READY";

      await client.query(
        `
        INSERT INTO tasks (
          id,
          workflow_id,
          name,
          type,
          operation_id,
          status
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        `,
        [taskId, workflowId, task.name, task.type, operationId, status],
      );
    }

    for (const task of definition.tasks) {
      const taskId = taskIds.get(task.id)!;

      for (const dependency of task.dependsOn ?? []) {
        const dependencyId = taskIds.get(dependency);

        if (!dependencyId) {
          throw new Error(
            `Unknown dependency "${dependency}" for task "${task.id}"`,
          );
        }

        await client.query(
          `
          INSERT INTO task_dependencies (
            task_id,
            depends_on_task_id
          )
          VALUES ($1, $2)
          `,
          [taskId, dependencyId],
        );
      }
    }

    let sequence = 1;

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
        sequence++,
        "WORKFLOW_STARTED",
        JSON.stringify({
          workflowType: definition.type,
          workflowVersion: definition.version,
        }),
      ],
    );

    for (const task of definition.tasks) {
      if (task.dependsOn && task.dependsOn.length > 0) {
        continue;
      }

      const taskId = taskIds.get(task.id)!;

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
          sequence++,
          "TASK_READY",
          JSON.stringify({
            taskId,
            taskKey: task.id,
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
          workflowId,
          taskId,
          "TASK_READY",
          JSON.stringify({
            workflowId,
            taskId,
            taskKey: task.id,
            taskType: task.type,
          }),
        ],
      );
    }

    await client.query("COMMIT");

    return workflowId;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function getTaskIdsByKey(
  workflowId: string,
): Promise<Map<string, string>> {
  const result = await pool.query(
    `
    SELECT id, operation_id
    FROM tasks
    WHERE workflow_id = $1
    `,
    [workflowId],
  );

  const prefix = `${workflowId}:`;
  const map = new Map<string, string>();

  for (const row of result.rows) {
    const operationId = String(row.operation_id);
    const key = operationId.startsWith(prefix)
      ? operationId.slice(prefix.length)
      : operationId;
    map.set(key, row.id);
  }

  return map;
}
