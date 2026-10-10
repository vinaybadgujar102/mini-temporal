export type RetryPolicy = {
  maxAttempts: number;
  initialDelayMs: number;
  maxDelayMs: number;
  jitterFactor: number;
};

export const defaultRetryPolicy: RetryPolicy = {
  maxAttempts: 5,
  initialDelayMs: 1_000,
  maxDelayMs: 30_000,
  jitterFactor: 0.2,
};

export function isRetryable(error: {
  code?: string;
  message?: string;
}): boolean {
  const code = error.code ?? "";

  return ["503", "429", "ECONNRESET", "ETIMEDOUT"].includes(code);
}

export function calculateRetryDelay(
  failedAttempt: number,
  policy: RetryPolicy = defaultRetryPolicy,
): number {
  const exponentialDelay = Math.min(
    policy.initialDelayMs * 2 ** (failedAttempt - 1),
    policy.maxDelayMs,
  );

  const jitter = 1 + (Math.random() * 2 - 1) * policy.jitterFactor;

  return Math.round(exponentialDelay * jitter);
}
