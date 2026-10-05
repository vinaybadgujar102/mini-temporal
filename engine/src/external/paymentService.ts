import crypto from "node:crypto";
import { pool } from "../db/client";

type PaymentResult = {
  transactionId: string;
  operationId: string;
  amount: number;
  status: "SUCCEEDED";
};

export async function chargePayment(
  operationId: string,
  amount: number,
): Promise<PaymentResult> {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const inserted = await client.query(
      `
      INSERT INTO payment_operations (
        idempotency_key,
        transaction_id,
        amount,
        status
      )
      VALUES ($1, $2, $3, 'SUCCEEDED')
      ON CONFLICT (idempotency_key)
      DO NOTHING
      RETURNING
        idempotency_key,
        transaction_id,
        amount,
        status
      `,
      [operationId, crypto.randomUUID(), amount],
    );

    if (inserted.rows.length > 0) {
      await client.query("COMMIT");

      const payment = inserted.rows[0];

      console.log(`[PAYMENT] charged ${amount} for ${operationId}`);

      return {
        transactionId: payment.transaction_id,
        operationId: payment.idempotency_key,
        amount: payment.amount,
        status: payment.status,
      };
    }

    const existing = await client.query(
      `
      SELECT
        idempotency_key,
        transaction_id,
        amount,
        status
      FROM payment_operations
      WHERE idempotency_key = $1
      `,
      [operationId],
    );

    await client.query("COMMIT");

    const payment = existing.rows[0];

    console.log(
      `[PAYMENT] duplicate request for ${operationId}, returning existing payment`,
    );

    return {
      transactionId: payment.transaction_id,
      operationId: payment.idempotency_key,
      amount: payment.amount,
      status: payment.status,
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
