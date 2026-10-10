import crypto from "node:crypto";
import { Kafka } from "kafkajs";
import { pool } from "../db/client";
import { chargePayment } from "../external/paymentService";
import { handleAttemptFailure } from "../retry/handleAttemptFailure";
import { claimTask } from "./claimTask";
import { completeAttempt as completeTaskAttempt } from "../workflow/taskCompletion";

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

    const completed = await completeTaskAttempt(
      pool,
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
      task.workflow_id,
      error,
    );
  } finally {
    clearInterval(heartbeatTimer);
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

async function failAttempt(
  taskId: string,
  attemptId: string,
  workerId: string,
  fencingToken: number,
  workflowId: string,
  error: unknown,
) {
  const client = await pool.connect();

  try {
    const errorPayload =
      error instanceof Error
        ? {
            message: error.message,
            ...("code" in error && typeof error.code === "string"
              ? { code: error.code }
              : {}),
          }
        : { message: String(error) };

    await handleAttemptFailure({
      client,
      taskId,
      attemptId,
      workflowId,
      workerId,
      fencingToken,
      error: errorPayload,
    });
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
  if (type === "CHARGE_PAYMENT") {
    return chargePayment(operationId, 1000);
  }

  await new Promise((resolve) => setTimeout(resolve, 1_000));

  return {
    operationId,
    success: true,
  };
}

startWorker().catch((error) => {
  console.error(error);
  process.exit(1);
});
