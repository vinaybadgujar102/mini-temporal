import crypto from "node:crypto";
import { Kafka } from "kafkajs";
import { pool } from "../db/client";
import { chargePayment } from "../external/paymentService";
import { handleAttemptFailure } from "../retry/handleAttemptFailure";
import { claimTask } from "./claimTask";
import { completeAttempt as completeTaskAttempt } from "../workflow/taskCompletion";
import { logger } from "../observability/logger";

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

      logger.info(
        {
          event: "worker.task.received",
          workflowId: taskMessage.workflowId,
          taskId: taskMessage.taskId,
          outboxId: taskMessage.outboxId,
        },
        "Task received from Kafka",
      );

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
    logger.info(
      {
        event: "worker.task.claim_skipped",
        taskId,
        reason: "not_claimable",
      },
      "Task was not claimed",
    );

    return;
  }

  const { task, attempt } = claimed;

  const workflowId = task.workflow_id;

  logger.info(
    {
      event: "worker.task.claimed",
      workflowId,
      taskId,
      attemptId: attempt.id,
      attempt: attempt.attempt_number,
      fencingToken: attempt.fencing_token,
    },
    "Task claimed",
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
        logger.warn(
          {
            event: "worker.heartbeat.ownership_lost",
            workflowId,
            taskId,
            attemptId: attempt.id,
            fencingToken: attempt.fencing_token,
          },
          "Worker no longer owns the task attempt",
        );

        clearInterval(heartbeatTimer);
      }
    } catch (error) {
      logger.error(
        {
          event: "worker.heartbeat.failed",
          workflowId,
          taskId,
          attemptId: attempt.id,
          err: error,
        },
        "Heartbeat update failed",
      );
    }
  }, 10_000);

  try {
    const activityStartedAt = performance.now();

    const result = await executeActivity({
      operationId: task.operation_id,
      type: task.type,
    });

    const durationMs = Math.round(performance.now() - activityStartedAt);

    logger.info(
      {
        event: "worker.activity.succeeded",
        workflowId,
        taskId,
        attemptId: attempt.id,
        durationMs,
      },
      "Activity execution succeeded",
    );

    if (process.env.CRASH_AFTER_ACTIVITY === "true") {
      logger.fatal(
        {
          event: "worker.crash_injection.triggered",
          workflowId,
          taskId,
          attemptId: attempt.id,
        },
        "Crash injection after activity execution",
      );

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
      logger.warn(
        {
          event: "worker.task.stale_result_rejected",
          workflowId,
          taskId,
          attemptId: attempt.id,
          fencingToken: attempt.fencing_token,
        },
        "Stale task result rejected",
      );
    } else {
      logger.info(
        {
          event: "worker.task.completed",
          workflowId,
          taskId,
          attemptId: attempt.id,
          fencingToken: attempt.fencing_token,
        },
        "Task completed durably",
      );
    }
  } catch (error) {
    await failAttempt(
      task.id,
      attempt.id,
      workerId,
      attempt.fencing_token,
      workflowId,
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
  logger.fatal(
    {
      event: "worker.startup.failed",
      err: error,
    },
    "Worker failed to start",
  );

  process.exit(1);
});
