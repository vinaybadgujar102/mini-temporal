import { pool } from "../db/client";
import { producer } from "../kafka/client";
import { logger } from "../observability/logger";
import {
  outboxActionablePendingMessages,
  outboxBatchFailure,
  outboxBatchSize,
  outboxMessageDuration,
  outboxMessagesProcessed,
  outboxOldestActionablePendingAge,
  outboxOldestPendingAge,
  outboxPendingMessages,
  outboxRetainedTerminalMessages,
} from "../observability/metrics";

const TOPIC = "workflow-tasks";

export async function publishOutbox() {
  const client = await pool.connect();
  const batchStartedAt = performance.now();

  let currentOutboxId: string | undefined;
  let currentStage = "select_pending";

  try {
    const result = await client.query(`
      SELECT id, workflow_id, task_id, event_type, payload
      FROM outbox
      WHERE published_at IS NULL
      ORDER BY created_at
      LIMIT 100
    `);

    outboxBatchSize.observe(result.rows.length);

    logger.debug(
      {
        event: "outbox.batch.selected",
        batchSize: result.rows.length,
      },
      "Selected pending outbox messages",
    );

    for (const row of result.rows) {
      currentOutboxId = row.id;
      currentStage = "begin_transaction";

      const messageStartedAt = performance.now();
      let transactionStarted = false;

      try {
        await client.query("BEGIN");
        transactionStarted = true;

        currentStage = "check_workflow";

        const workflowResult = await client.query(
          `
          SELECT status
          FROM workflows
          WHERE id = $1
          FOR UPDATE
          `,
          [row.workflow_id],
        );

        const workflowMissing = workflowResult.rowCount !== 1;
        const workflowNotRunning =
          !workflowMissing && workflowResult.rows[0].status !== "RUNNING";

        if (workflowMissing || workflowNotRunning) {
          if (row.event_type === "TASK_READY") {
            currentStage = "delete_obsolete_message";

            await client.query(
              `
              DELETE FROM outbox
              WHERE id = $1
                AND published_at IS NULL
              `,
              [row.id],
            );

            currentStage = "commit_discard";
            await client.query("COMMIT");
            transactionStarted = false;

            const durationSeconds =
              (performance.now() - messageStartedAt) / 1000;

            outboxMessagesProcessed.inc({
              outcome: "discarded",
            });
            outboxMessageDuration.observe(
              { outcome: "discarded" },
              durationSeconds,
            );

            logger.info(
              {
                event: "outbox.message.discarded",
                outboxId: row.id,
                workflowId: row.workflow_id,
                taskId: row.task_id,
                reason: workflowMissing
                  ? "workflow_missing"
                  : "workflow_not_running",
              },
              "Obsolete task-ready message discarded",
            );
          } else {
            currentStage = "commit_skip";
            await client.query("COMMIT");
            transactionStarted = false;

            outboxMessagesProcessed.inc({ outcome: "skipped" });
            outboxMessageDuration.observe(
              { outcome: "skipped" },
              (performance.now() - messageStartedAt) / 1000,
            );

            logger.debug(
              {
                event: "outbox.message.skipped",
                outboxId: row.id,
                workflowId: row.workflow_id,
                taskId: row.task_id,
                reason: workflowMissing
                  ? "workflow_missing"
                  : "workflow_not_running",
              },
              "Message retained because its workflow is not running",
            );
          }

          currentOutboxId = undefined;
          continue;
        }

        currentStage = "kafka_send";

        await producer.send({
          topic: TOPIC,
          messages: [
            {
              key: row.task_id,
              value: JSON.stringify({
                outboxId: row.id,
                workflowId: row.workflow_id,
                taskId: row.task_id,
                eventType: row.event_type,
                payload: row.payload,
              }),
            },
          ],
        });

        currentStage = "mark_published";

        await client.query(
          `
          UPDATE outbox
          SET published_at = NOW()
          WHERE id = $1
            AND published_at IS NULL
          `,
          [row.id],
        );

        currentStage = "commit_publish";

        await client.query("COMMIT");
        transactionStarted = false;

        const durationMs = Math.round(performance.now() - messageStartedAt);

        outboxMessagesProcessed.inc({ outcome: "published" });
        outboxMessageDuration.observe(
          { outcome: "published" },
          durationMs / 1000,
        );

        logger.info(
          {
            event: "outbox.publish.succeeded",
            outboxId: row.id,
            workflowId: row.workflow_id,
            taskId: row.task_id,
            eventType: row.event_type,
            durationMs,
          },
          "Outbox message published and transaction committed",
        );

        currentOutboxId = undefined;
      } catch (error) {
        outboxMessagesProcessed.inc({ outcome: "failed" });
        outboxMessageDuration.observe(
          { outcome: "failed" },
          (performance.now() - messageStartedAt) / 1000,
        );

        logger.error(
          {
            event: "outbox.publish.failed",
            outboxId: row.id,
            workflowId: row.workflow_id,
            taskId: row.task_id,
            eventType: row.event_type,
            stage: currentStage,
            err: error,
          },
          "Outbox message processing failed",
        );

        if (transactionStarted) {
          try {
            await client.query("ROLLBACK");
          } catch (rollbackError) {
            logger.error(
              {
                event: "outbox.transaction.rollback_failed",
                outboxId: row.id,
                err: rollbackError,
              },
              "Failed to roll back outbox transaction",
            );
          }
        }

        throw error;
      }
    }

    logger.debug(
      {
        event: "outbox.batch.completed",
        batchSize: result.rows.length,
        durationMs: Math.round(performance.now() - batchStartedAt),
      },
      "Outbox batch completed",
    );
  } catch (error) {
    if (!currentOutboxId) {
      outboxBatchFailure.inc();

      logger.error(
        {
          event: "outbox.batch.failed",
          stage: currentStage,
          err: error,
        },
        "Outbox batch failed",
      );
    }

    throw error;
  } finally {
    try {
      const backlog = await client.query(`
        SELECT
          COUNT(*)::int AS pending_count,
          COALESCE(
            EXTRACT(EPOCH FROM (NOW() - MIN(o.created_at))),
            0
          )::float AS oldest_pending_age_seconds,

          COUNT(*) FILTER (
            WHERE w.status = 'RUNNING'
          )::int AS actionable_count,

          COALESCE(
            EXTRACT(
              EPOCH FROM (
                NOW() - MIN(o.created_at) FILTER (
                  WHERE w.status = 'RUNNING'
                )
              )
            ),
            0
          )::float AS oldest_actionable_age_seconds,

          COUNT(*) FILTER (
            WHERE w.status IS DISTINCT FROM 'RUNNING'
          )::int AS retained_terminal_count

        FROM outbox o
        LEFT JOIN workflows w ON w.id = o.workflow_id
        WHERE o.published_at IS NULL
      `);

      outboxPendingMessages.set(Number(backlog.rows[0].pending_count));

      outboxOldestPendingAge.set(
        Number(backlog.rows[0].oldest_pending_age_seconds),
      );

      outboxActionablePendingMessages.set(
        Number(backlog.rows[0].actionable_count),
      );

      outboxOldestActionablePendingAge.set(
        Number(backlog.rows[0].oldest_actionable_age_seconds),
      );

      outboxRetainedTerminalMessages.set(
        Number(backlog.rows[0].retained_terminal_count),
      );
    } catch (error) {
      logger.warn(
        {
          event: "outbox.backlog_metrics.refresh_failed",
          err: error,
        },
        "Failed to refresh outbox backlog metrics",
      );
    } finally {
      client.release();
    }
  }
}
