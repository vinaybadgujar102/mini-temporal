import { pool } from "./db/client";

async function main() {
  const result = await pool.query("SELECT NOW()");
  console.log(result.rows[0]);
  await pool.end();
}

main();

// {
//   type: "order-processing",
//   version: 1,
//   tasks: [
//     { id: "charge", type: "CHARGE_PAYMENT" },
//     { id: "reserve", type: "RESERVE_INVENTORY" },
//     { id: "email", type: "SEND_EMAIL" }
//   ]
// }

type WorkflowTask = {
  id: string; // logical task key
  name: string;
  type: string;
  dependsOn?: string[]; // logical task keys
};

type WorkflowDefinition = {
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

  // logical task key -> database UUID
  const taskIds = new Map<string, string>();

  try {
    await client.query("BEGIN");

    // --------------------------------------------------
    // 1. Create workflow
    // --------------------------------------------------

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

    // --------------------------------------------------
    // 2. Generate DB IDs for tasks
    // --------------------------------------------------

    for (const task of definition.tasks) {
      const taskId = crypto.randomUUID();

      taskIds.set(task.id, taskId);
    }

    // --------------------------------------------------
    // 3. Create tasks
    // --------------------------------------------------

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

    // --------------------------------------------------
    // 4. Create dependencies
    // --------------------------------------------------

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

    // --------------------------------------------------
    // 5. Write workflow history
    // --------------------------------------------------

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

    // --------------------------------------------------
    // 6. Schedule initially READY tasks
    // --------------------------------------------------

    for (const task of definition.tasks) {
      if (task.dependsOn && task.dependsOn.length > 0) {
        continue;
      }

      const taskId = taskIds.get(task.id)!;

      // History
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

      // Outbox
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

const workflowId = await startWorkflow({
  type: "ORDER_PROCESSING",
  name: "Process Order",
  version: 1,

  tasks: [
    {
      id: "charge",
      name: "Charge Payment",
      type: "CHARGE_PAYMENT",
    },
    {
      id: "reserve",
      name: "Reserve Inventory",
      type: "RESERVE_INVENTORY",
    },
    {
      id: "email",
      name: "Send Confirmation",
      type: "SEND_EMAIL",
      dependsOn: ["charge", "reserve"],
    },
  ],
});

console.log("Started:", workflowId);
