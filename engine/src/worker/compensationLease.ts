import { pool } from "../db/client";

export async function renewCompensationLease(
  compensation: {
    id: string;
    workflow_id: string;
    fencing_token: string;
  },
  leaseDurationSeconds = 60,
): Promise<boolean> {
  const client = await pool.connect();

  try {
    const result = await client.query(
      `
      UPDATE compensation_operations AS c
      SET lease_expires_at =
            NOW() + ($4::integer * INTERVAL '1 second'),
          updated_at = NOW()
      FROM workflows AS w
      WHERE c.id = $1
        AND c.workflow_id = $2
        AND c.status = 'RUNNING'
        AND c.fencing_token = $3
        AND c.lease_expires_at > NOW()
        AND w.id = c.workflow_id
        AND w.status = 'COMPENSATING'
      RETURNING c.id
      `,
      [
        compensation.id,
        compensation.workflow_id,
        compensation.fencing_token,
        leaseDurationSeconds,
      ],
    );

    return result.rowCount === 1;
  } finally {
    client.release();
  }
}
