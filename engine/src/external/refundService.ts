import { pool } from "../db/client";

type RefundResult = {
  transactionId: string;
  operationId: string;
  amount: number;
  status: "REFUNDED";
};

export async function refundPayment(
  operationId: string,
  compensationIdempotencyKey: string,
): Promise<RefundResult> {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const paymentResult = await client.query(
      `
      SELECT
        idempotency_key,
        transaction_id,
        amount,
        status,
        refund_idempotency_key
      FROM payment_operations
      WHERE idempotency_key = $1
      FOR UPDATE
      `,
      [operationId],
    );

    if (paymentResult.rowCount !== 1) {
      throw new Error(`Payment not found: ${operationId}`);
    }

    const payment = paymentResult.rows[0];

    if (
      payment.refund_idempotency_key !== null &&
      payment.refund_idempotency_key !== compensationIdempotencyKey
    ) {
      throw new Error(
        `Payment ${operationId} already has a different refund key`,
      );
    }

    if (payment.status === "REFUNDED") {
      await client.query("COMMIT");

      return {
        transactionId: payment.transaction_id,
        operationId: payment.idempotency_key,
        amount: Number(payment.amount),
        status: "REFUNDED",
      };
    }

    if (payment.status !== "SUCCEEDED") {
      throw new Error(
        `Cannot refund payment ${operationId} in status ${payment.status}`,
      );
    }

    await client.query(
      `
      UPDATE payment_operations
      SET status = 'REFUNDED',
          refunded_at = NOW(),
          refund_idempotency_key = $2
      WHERE idempotency_key = $1
        AND status = 'SUCCEEDED'
      `,
      [operationId, compensationIdempotencyKey],
    );

    await client.query("COMMIT");

    return {
      transactionId: payment.transaction_id,
      operationId: payment.idempotency_key,
      amount: Number(payment.amount),
      status: "REFUNDED",
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
