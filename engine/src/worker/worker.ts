import crypto from "node:crypto";
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

const LEASE_SECONDS = 30;

type TaskMessage = {
  outboxId: string;
  workflowId: string;
  taskId: string;
  eventType: string;
  payload: {
    taskId: string;
    taskKey: string;
    taskType: string;
  };
};

type ClaimedTask = {
  id: string;
  workflow_id: string;
  type: string;
  operation_id: string;
  attempt_count: number;
};

type ClaimedAttempt = {
  id: string;
  attempt_number: number;
  fencing_token: number;
  worker_id: string;
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

      const taskMessage: TaskMessage = JSON.parse(message.value.toString());

      console.log(`[worker] received task ${taskMessage.taskId}`);

      await executeTask(taskMessage.taskId);
    },
  });
}

/**
 * Main task execution lifecycle:
 *
 * Kafka message
 *      ↓
 * Claim task + create attempt
 *      ↓
 * Heartbeat while executing
 *      ↓
 * External activity
 *      ↓
 * Fenced completion
 */
async function executeTask(taskId: string) {
  const workerId = crypto.randomUUID();

  const claimed = await claimTask(taskId, workerId);

  if (!claimed) {
    console.log(`[worker] task ${taskId} was already claimed`);

    return;
  }

  const { task, attempt } = claimed;

  console.log(
    `[worker] claimed task=${task.id} ` +
      `attempt=${attempt.attempt_number} ` +
      `token=${attempt.fencing_token}`,
  );

  // Heartbeat every 10 seconds.
  const heartbeatTimer = setInterval(async () => {
    try {
      const alive = await heartbeat(
        attempt.id,
        workerId,
        attempt.fencing_token,
      );

      if (!alive) {
        console.log(`[worker] lost ownership of attempt ${attempt.id}`);

        clearInterval(heartbeatTimer);
      }
    } catch (error) {
      console.error("[worker] heartbeat failed", error);
    }
  }, 10_000);

  try {
    const result = await executeActivity({
      operationId: task.operation_id,
      type: task.type,
    });

    console.log(`[worker] activity succeeded for ${task.id}`);

    if (process.env.CRASH_AFTER_ACTIVITY === "true") {
      console.log("[worker] 💥 crashing after activity");

      process.exit(1);
    }

    const completed = await completeAttempt(
      task.id,
      attempt.id,
      workerId,
      attempt.fencing_token,
      result,
    );

    if (!completed) {
      console.log(`[worker] stale result rejected for ${task.id}`);
    }
  } catch (error) {
    await failAttempt(
      task.id,
      attempt.id,
      workerId,
      attempt.fencing_token,
      error,
    );
  } finally {
    clearInterval(heartbeatTimer);
  }
}

async function claimTask(
  taskId: string,
  workerId: string,
): Promise<{
  task: ClaimedTask;
  attempt: ClaimedAttempt;
} | null> {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const taskResult = await client.query(
      `
      UPDATE tasks
      SET
        status = 'RUNNING',
        attempt_count = attempt_count + 1,
        updated_at = NOW()
      WHERE id = $1
        AND status = 'READY'
      RETURNING
        id,
        workflow_id,
        type,
        operation_id,
        attempt_count;
      `,
      [taskId],
    );

    if (taskResult.rowCount === 0) {
      await client.query("ROLLBACK");

      return null;
    }

    const task = taskResult.rows[0] as ClaimedTask;

    const fencingToken = task.attempt_count;

    const attemptId = crypto.randomUUID();

    const attemptResult = await client.query(
      `
      INSERT INTO task_attempts (
        id,
        task_id,
        attempt_number,
        status,
        worker_id,
        fencing_token,
        lease_until,
        last_heartbeat_at,
        started_at
      )
      VALUES (
        $1,
        $2,
        $3,
        'RUNNING',
        $4,
        $5,
        NOW() + ($6 * INTERVAL '1 second'),
        NOW(),
        NOW()
      )
      RETURNING
        id,
        attempt_number,
        fencing_token,
        worker_id;
      `,
      [
        attemptId,
        task.id,
        task.attempt_count,
        workerId,
        fencingToken,
        LEASE_SECONDS,
      ],
    );

    await client.query("COMMIT");

    return {
      task,
      attempt: attemptResult.rows[0],
    };
  } catch (error) {
    await client.query("ROLLBACK");

    throw error;
  } finally {
    client.release();
  }
}

async function heartbeat(
  attemptId: string,
  workerId: string,
  fencingToken: number,
): Promise<boolean> {
  const result = await pool.query(
    `
    UPDATE task_attempts
    SET
      lease_until =
        NOW() + ($4 * INTERVAL '1 second'),
      last_heartbeat_at = NOW()
    WHERE id = $1
      AND worker_id = $2
      AND fencing_token = $3
      AND status = 'RUNNING'
      AND lease_until > NOW()
    `,
    [attemptId, workerId, fencingToken, LEASE_SECONDS],
  );

  return result.rowCount === 1;
}

async function completeAttempt(
  taskId: string,
  attemptId: string,
  workerId: string,
  fencingToken: number,
  result: unknown,
): Promise<boolean> {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const attemptResult = await client.query(
      `
      UPDATE task_attempts
      SET
        status = 'COMPLETED',
        result = $1,
        completed_at = NOW()
      WHERE id = $2
        AND task_id = $3
        AND worker_id = $4
        AND fencing_token = $5
        AND status = 'RUNNING'
        AND lease_until > NOW()
      RETURNING id;
      `,
      [JSON.stringify(result), attemptId, taskId, workerId, fencingToken],
    );

    if (attemptResult.rowCount === 0) {
      await client.query("ROLLBACK");

      return false;
    }

    await client.query(
      `
      UPDATE tasks
      SET
        status = 'COMPLETED',
        result = $1,
        updated_at = NOW()
      WHERE id = $2
        AND status = 'RUNNING'
      `,
      [JSON.stringify(result), taskId],
    );

    const workflowResult = await client.query(
      `
      SELECT workflow_id
      FROM tasks
      WHERE id = $1
      `,
      [taskId],
    );

    const workflowId = workflowResult.rows[0].workflow_id;

    const sequenceResult = await client.query(
      `
      SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence
      FROM workflow_events
      WHERE workflow_id = $1
      `,
      [workflowId],
    );

    let sequence = Number(sequenceResult.rows[0].sequence);

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
          attemptId,
          fencingToken,
          result,
        }),
      ],
    );

    await client.query("COMMIT");

    return true;
  } catch (error) {
    await client.query("ROLLBACK");

    throw error;
  } finally {
    client.release();
  }
}

async function failAttempt(
  taskId: string,
  attemptId: string,
  workerId: string,
  fencingToken: number,
  error: unknown,
) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const errorData = {
      message: error instanceof Error ? error.message : String(error),
    };

    const attemptResult = await client.query(
      `
      UPDATE task_attempts
      SET
        status = 'FAILED',
        error = $1,
        completed_at = NOW()
      WHERE id = $2
        AND task_id = $3
        AND worker_id = $4
        AND fencing_token = $5
        AND status = 'RUNNING'
      RETURNING id;
      `,
      [JSON.stringify(errorData), attemptId, taskId, workerId, fencingToken],
    );

    if (attemptResult.rowCount === 0) {
      await client.query("ROLLBACK");

      return;
    }
    await client.query(
      `
      UPDATE tasks
      SET
        status = 'FAILED',
        error = $1,
        updated_at = NOW()
      WHERE id = $2
        AND status = 'RUNNING'
      `,
      [JSON.stringify(errorData), taskId],
    );

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");

    throw error;
  } finally {
    client.release();
  }
}

async function executeActivity({
  operationId,
  type,
}: {
  operationId: string;
  type: string;
}) {
  console.log(`[activity] executing ${type}`);

  console.log(`[activity] idempotency key: ${operationId}`);

  await new Promise((resolve) => setTimeout(resolve, 1_000));

  return {
    transactionId: crypto.randomUUID(),
    operationId,
    success: true,
  };
}

startWorker().catch((error) => {
  console.error(error);
  process.exit(1);
});
