import { Kafka } from "kafkajs";
import { pool } from "../db/client";

const kafka = new Kafka({
  clientId: "mini-temporal-worker",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "workflow-workers",
});

const TOPIC = "workflow-tasks";

type TaskMesasge = {
  outboxId: string;
  workflowId: string;
  taskId: string;
  eventType: string;
  payload: {
    taskId: string;
    taskKey: string;
    taskTypes: string;
  };
};

export async function startWorker() {
  await consumer.connect();

  await consumer.subscribe({
    topic: TOPIC,
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) {
        return;
      }

      const taskMessage: TaskMesasge = JSON.parse(message.value.toString());

      console.log("Recieved", taskMessage);

      await executeTask(taskMessage.taskId);
    },
  });
}

async function executeTask(taskId: string) {
  const client = await pool.connect();

  let task: any;

  try {
    await client.query("BEGIN");

    const result = await client.query(
      `
  UPDATE tasks
  SET
    status = 'RUNNING',
    attempt_count = attempt_count + 1,
    updated_at = NOW()
  WHERE id = $1
    AND status = 'READY'
  RETURNING *;
  `,
      [taskId],
    );

    if (result.rowCount === 0) {
      await client.query("ROLLBACK");
      console.log("Task does not exist:", taskId);
      return;
    }

    task = result.rows[0];

    await client.query(
      `INSERT INTO task_attempts (
  id, task_id, attempt_number, status, started_at
) VALUES ($1, $2, $3, 'RUNNING', NOW())
`,
      [crypto.randomUUID(), task.id, task.attempt_count],
    );

    await client.query("COMMIT");

    console.log(`Task ${task.id} claimed. Attempt ${task.attempt_count}`);
  } catch (error) {
    await client.query(`ROLLBACK`);
    throw error;
  } finally {
    client.release();
  }

  let activityResult;

  try {
    activityResult = await executeActivity({
      operationId: task.operation_id,
      type: task.type,
    });
  } catch (error) {
    await markAttemptFailed(task.id, task.attempt_count, error);

    return;
  }

  await markTaskCompleted(task.id, task.attempt_count, activityResult);

  /*
   * Fake external service for now.
   *
   * Later we'll turn this into a real
   * idempotent external service.
   */
  async function executeActivity({
    operationId,
    type,
  }: {
    operationId: string;
    type: string;
  }) {
    console.log(`Executing ${type} with idempotency key ${operationId}`);

    await new Promise((resolve) => setTimeout(resolve, 1000));

    return {
      transactionId: crypto.randomUUID(),
      operationId,
      success: true,
    };
  }

  async function markAttemptFailed(
    taskId: string,
    attemptNumber: number,
    error: unknown,
  ) {
    const client = await pool.connect();

    try {
      await client.query("BEGIN");

      await client.query(
        `
      UPDATE task_attempts
      SET
        status = 'FAILED',
        error = $1,
        completed_at = NOW()
      WHERE task_id = $2
        AND attempt_number = $3
      `,
        [
          JSON.stringify({
            message: error instanceof Error ? error.message : String(error),
          }),
          taskId,
          attemptNumber,
        ],
      );

      await client.query(
        `
      UPDATE tasks
      SET
        status = 'FAILED',
        error = $1,
        updated_at = NOW()
      WHERE id = $2
      `,
        [
          JSON.stringify({
            message: error instanceof Error ? error.message : String(error),
          }),
          taskId,
        ],
      );

      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async function markTaskCompleted(
    taskId: string,
    attemptNumber: number,
    result: unknown,
  ) {
    const client = await pool.connect();

    try {
      await client.query("BEGIN");

      /*
       * Mark attempt completed.
       */
      await client.query(
        `
      UPDATE task_attempts
      SET
        status = 'COMPLETED',
        result = $1,
        completed_at = NOW()
      WHERE task_id = $2
        AND attempt_number = $3
      `,
        [JSON.stringify(result), taskId, attemptNumber],
      );

      /*
       * Mark logical task completed.
       */
      await client.query(
        `
      UPDATE tasks
      SET
        status = 'COMPLETED',
        result = $1,
        updated_at = NOW()
      WHERE id = $2
      `,
        [JSON.stringify(result), taskId],
      );

      /*
       * TODO:
       *
       * 1. Append TASK_COMPLETED event
       * 2. Find dependent tasks
       * 3. Determine which dependencies are now satisfied
       * 4. Move newly-unblocked tasks to READY
       * 5. Insert outbox messages
       *
       * We'll implement this next.
       */

      const sequenceResult = await client.query(
        `
      SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence
      FROM workflow_events
      WHERE workflow_id = (
        SELECT workflow_id
        FROM tasks
        WHERE id = $1
      )
      `,
        [taskId],
      );

      let sequence = Number(sequenceResult.rows[0].sequence);

      const workflowResult = await client.query(
        `
      SELECT workflow_id
      FROM tasks
      WHERE id = $1
      `,
        [taskId],
      );

      const workflowId = workflowResult.rows[0].workflow_id;

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
          "TASK_COMPLETED",
          JSON.stringify({
            taskId,
            result,
          }),
        ],
      );

      const dependentTasks = await client.query(
        `
      SELECT task_id
      FROM task_dependencies
      WHERE depends_on_task_id = $1
      `,
        [taskId],
      );

      for (const row of dependentTasks.rows) {
        const dependentTaskId = row.task_id;

        const dependencyCheck = await client.query(
          `
        SELECT COUNT(*) AS remaining
        FROM task_dependencies td
        JOIN tasks dependency
          ON dependency.id = td.depends_on_task_id
        WHERE td.task_id = $1
          AND dependency.status != 'COMPLETED'
        `,
          [dependentTaskId],
        );
        const remaining = Number(dependencyCheck.rows[0].remaining);

        if (remaining !== 0) {
          continue;
        }

        const readyResult = await client.query(
          `
        UPDATE tasks
        SET
          status = 'READY',
          updated_at = NOW()
        WHERE id = $1
          AND status = 'PENDING'
        RETURNING id, workflow_id, type, name
        `,
          [dependentTaskId],
        );

        if (readyResult.rowCount === 0) {
          continue;
        }

        const readyTask = readyResult.rows[0];

        // 8. Record TASK_READY
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
              taskId: readyTask.id,
            }),
          ],
        );

        // 9. Create outbox message
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
            readyTask.id,
            "TASK_READY",
            JSON.stringify({
              workflowId,
              taskId: readyTask.id,
              taskType: readyTask.type,
            }),
          ],
        );
      }

      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }
}
