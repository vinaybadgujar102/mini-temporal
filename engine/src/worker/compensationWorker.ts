import crypto from "node:crypto";
import { pool } from "../db/client";
import { refundPayment } from "../external/refundService";
import {
  calculateRetryDelay,
  defaultRetryPolicy,
  isRetryable,
} from "../retry/retryPolicy";
import { renewCompensationLease } from "./compensationLease";

type Compensation = {
  id: string;
  workflow_id: string;
  task_id: string;
  idempotency_key: string;
  compensation_type: string;
  original_operation_id: string;
  input: { paymentOperationId: string };
  attempt_count: number;
  fencing_token: string;
};

async function claimCompensation(): Promise<Compensation | null> {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // Discover candidates without taking locks in the wrong order.
    const candidates = await client.query(
      `
      SELECT c.id, c.workflow_id
      FROM compensation_operations c
      JOIN workflows w ON w.id = c.workflow_id
      WHERE w.status = 'COMPENSATING'
        AND c.status IN ('PENDING', 'READY')
        AND (
          c.next_retry_at IS NULL
          OR c.next_retry_at <= NOW()
        )
        AND NOT EXISTS (
          SELECT 1
          FROM task_dependencies d
          JOIN compensation_operations child_c
            ON child_c.task_id = d.task_id
          WHERE d.depends_on_task_id = c.task_id
            AND child_c.workflow_id = c.workflow_id
            AND child_c.status <> 'COMPLETED'
        )
      ORDER BY c.created_at
      LIMIT 20
      `,
    );

    for (const candidate of candidates.rows) {
      // Match the workflow -> operation lock order.
      const workflow = await client.query(
        `
        SELECT status
        FROM workflows
        WHERE id = $1
        FOR UPDATE
        `,
        [candidate.workflow_id],
      );

      if (
        workflow.rowCount !== 1 ||
        workflow.rows[0].status !== "COMPENSATING"
      ) {
        continue;
      }

      const operationResult = await client.query(
        `
        SELECT *
        FROM compensation_operations
        WHERE id = $1
          AND workflow_id = $2
          AND status IN ('PENDING', 'READY')
          AND (
            next_retry_at IS NULL
            OR next_retry_at <= NOW()
          )
        FOR UPDATE
        `,
        [candidate.id, candidate.workflow_id],
      );

      if (operationResult.rowCount !== 1) continue;

      // Recheck dependencies after obtaining the workflow lock.
      const blocked = await client.query(
        `
        SELECT 1
        FROM task_dependencies d
        JOIN compensation_operations child_c
          ON child_c.task_id = d.task_id
        WHERE d.depends_on_task_id = $1
          AND child_c.workflow_id = $2
          AND child_c.status <> 'COMPLETED'
        LIMIT 1
        `,
        [operationResult.rows[0].task_id, candidate.workflow_id],
      );

      if (blocked.rowCount !== 0) continue;

      const claimed = await client.query(
        `
  UPDATE compensation_operations
  SET status = 'RUNNING',
      attempt_count = attempt_count + 1,
      fencing_token = fencing_token + 1,
      lease_expires_at = NOW() + INTERVAL '60 seconds',
      updated_at = NOW()
  WHERE id = $1
    AND status IN ('PENDING', 'READY')
  RETURNING *
  `,
        [candidate.id],
      );
      await client.query("COMMIT");

      return claimed.rows[0] as Compensation;
    }

    await client.query("COMMIT");
    return null;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function finishCompensation(compensation: Compensation, result: unknown) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const workflow = await client.query(
      `
      SELECT status
      FROM workflows
      WHERE id = $1
      FOR UPDATE
      `,
      [compensation.workflow_id],
    );

    if (workflow.rowCount !== 1 || workflow.rows[0].status !== "COMPENSATING") {
      await client.query("ROLLBACK");
      return;
    }

    const updated = await client.query(
      `
      UPDATE compensation_operations
      SET status = 'COMPLETED',
          result = $2::jsonb,
          error = NULL,
          lease_expires_at = NULL,
          completed_at = NOW(),
          updated_at = NOW()
      WHERE id = $1
        AND status = 'RUNNING'
        AND fencing_token = $3
        AND lease_expires_at > NOW()
      RETURNING id
      `,
      [compensation.id, JSON.stringify(result), compensation.fencing_token],
    );

    if (updated.rowCount !== 1) {
      await client.query("ROLLBACK");
      return;
    }

    const sequenceResult = await client.query(
      `
      SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence
      FROM workflow_events
      WHERE workflow_id = $1
      `,
      [compensation.workflow_id],
    );

    let sequence = Number(sequenceResult.rows[0].sequence);

    await client.query(
      `
      INSERT INTO workflow_events (
        id, workflow_id, sequence, event_type, data
      )
      VALUES ($1, $2, $3, $4, $5::jsonb)
      `,
      [
        crypto.randomUUID(),
        compensation.workflow_id,
        sequence++,
        "COMPENSATION_COMPLETED",
        JSON.stringify({
          compensationId: compensation.id,
          taskId: compensation.task_id,
          type: compensation.compensation_type,
        }),
      ],
    );

    // A failed compensation needs intervention or another retry.
    const unfinished = await client.query(
      `
      SELECT 1
      FROM compensation_operations
      WHERE workflow_id = $1
        AND status <> 'COMPLETED'
      LIMIT 1
      `,
      [compensation.workflow_id],
    );

    if (unfinished.rowCount === 0) {
      await client.query(
        `
        UPDATE workflows
        SET status = 'FAILED',
            updated_at = NOW()
        WHERE id = $1
          AND status = 'COMPENSATING'
        `,
        [compensation.workflow_id],
      );

      await client.query(
        `
        INSERT INTO workflow_events (
          id, workflow_id, sequence, event_type, data
        )
        VALUES ($1, $2, $3, $4, $5::jsonb)
        `,
        [
          crypto.randomUUID(),
          compensation.workflow_id,
          sequence,
          "WORKFLOW_FAILED",
          JSON.stringify({
            workflowId: compensation.workflow_id,
            reason: "FORWARD_EXECUTION_FAILED_COMPENSATIONS_COMPLETED",
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

// record in durable state that compensation operation failed
async function recordCompensationFailure(
  compensation: Compensation,
  error: { message: string; code?: string },
) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const workflow = await client.query(
      `
      SELECT status
      FROM workflows
      WHERE id = $1
      FOR UPDATE
      `,
      [compensation.workflow_id],
    );

    if (workflow.rowCount !== 1 || workflow.rows[0].status !== "COMPENSATING") {
      await client.query("ROLLBACK");
      return;
    }

    const retry =
      isRetryable(error) &&
      compensation.attempt_count < defaultRetryPolicy.maxAttempts;

    const delayMs = retry
      ? calculateRetryDelay(compensation.attempt_count, defaultRetryPolicy)
      : null;

    const updated = await client.query(
      `
      UPDATE compensation_operations
      SET status = $2,
          error = $3::jsonb,
          lease_expires_at = NULL,
          next_retry_at = CASE
            WHEN $2 = 'READY'
            THEN NOW() + ($4::double precision * INTERVAL '1 millisecond')
            ELSE NULL
          END,
          updated_at = NOW()
      WHERE id = $1
        AND status = 'RUNNING'
        AND fencing_token = $5
        AND lease_expires_at > NOW()
      RETURNING id
      `,
      [
        compensation.id,
        retry ? "READY" : "FAILED",
        JSON.stringify(error),
        delayMs,
        compensation.fencing_token,
      ],
    );

    if (updated.rowCount !== 1) {
      await client.query("ROLLBACK");
      return;
    }

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

const LEASE_DURATION_SECONDS = 60;
const HEARTBEAT_INTERVAL_MS = 20_000;

type CompensationOutcome =
  | { kind: "SUCCESS"; result: unknown }
  | { kind: "FAILURE"; error: unknown }
  | { kind: "LEASE_LOST" };

export async function runCompensationOnce(): Promise<boolean> {
  const compensation = await claimCompensation();

  if (!compensation) return false;

  let stopped = false;
  let leaseLost = false;

  let heartbeatTimer: ReturnType<typeof setTimeout> | undefined;
  let heartbeatInFlight: Promise<void> | undefined;

  let resolveLeaseLost!: (outcome: CompensationOutcome) => void;

  const leaseLostPromise = new Promise<CompensationOutcome>((resolve) => {
    resolveLeaseLost = resolve;
  });

  function stopHeartbeat() {
    stopped = true;

    if (heartbeatTimer) {
      clearTimeout(heartbeatTimer);
      heartbeatTimer = undefined;
    }
  }

  function markLeaseLost() {
    if (leaseLost) return;

    leaseLost = true;
    resolveLeaseLost({ kind: "LEASE_LOST" });
  }

  function scheduleHeartbeat() {
    if (stopped || leaseLost) return;

    heartbeatTimer = setTimeout(() => {
      heartbeatInFlight = (async () => {
        try {
          const renewed = await renewCompensationLease(
            compensation,
            LEASE_DURATION_SECONDS,
          );

          if (!renewed) {
            markLeaseLost();
            return;
          }
        } catch {
          // We cannot safely assume that ownership is still valid.
          markLeaseLost();
          return;
        }

        if (!stopped && !leaseLost) {
          scheduleHeartbeat();
        }
      })();

      void heartbeatInFlight;
    }, HEARTBEAT_INTERVAL_MS);
  }

  // Start renewing the lease while the compensation runs.
  scheduleHeartbeat();

  // Convert the operation into a result so Promise.race does not leave
  // an unhandled rejection if the lease is lost first.
  const workPromise: Promise<CompensationOutcome> = (async () => {
    try {
      if (compensation.compensation_type !== "REFUND_PAYMENT") {
        throw new Error(
          `Unsupported compensation: ${compensation.compensation_type}`,
        );
      }

      const result = await refundPayment(
        compensation.original_operation_id,
        compensation.idempotency_key,
      );

      return { kind: "SUCCESS", result };
    } catch (error) {
      return { kind: "FAILURE", error };
    }
  })();

  try {
    const outcome = await Promise.race([workPromise, leaseLostPromise]);

    // Stop future heartbeat scheduling and wait for any active renewal
    // before deciding whether this worker may persist a result.
    stopHeartbeat();

    if (heartbeatInFlight) {
      await heartbeatInFlight;
    }

    if (leaseLost || outcome.kind === "LEASE_LOST") {
      // Do not write completion or failure using a stale claim.
      // Recovery can reclaim this operation after its lease expires.
      return true;
    }

    if (outcome.kind === "SUCCESS") {
      await finishCompensation(compensation, outcome.result);
      return true;
    }

    const error =
      outcome.error instanceof Error
        ? {
            message: outcome.error.message,
            ...("code" in outcome.error &&
            typeof outcome.error.code === "string"
              ? { code: outcome.error.code }
              : {}),
          }
        : { message: String(outcome.error) };

    await recordCompensationFailure(compensation, error);

    return true;
  } finally {
    stopHeartbeat();
  }
}
