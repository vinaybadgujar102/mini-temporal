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

      const taskMessage = JSON.parse(message.value.toString());

      console.log("Recieved", taskMessage);

      await executeTask(taskMessage.taskId);
    },
  });
}

async function executeTask(taskId: string) {
  const client = await pool.connect();

  try {
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
      console.log("Task does not exist:", taskId);
      return;
    }

    const task = result.rows[0];
    if (task.status !== "READY") {
      console.log(`Ignoring task ${taskId}. Current status: ${task.status}`);

      return;
    }

    console.log("Executing task:", taskId);

    console.log("🔥 ACTIVITY EXECUTED");
  } finally {
    client.release();
  }
}
