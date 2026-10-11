import pino from "pino";

export const logger = pino({
  level: process.env.LOG_LEVEL ?? "info",

  base: {
    service: process.env.SERVICE_NAME ?? "mini-temporal",
    environment: process.env.NODE_ENV ?? "development",
  },

  timestamp: pino.stdTimeFunctions.isoTime,

  redact: {
    paths: [
      "*.password",
      "*.secret",
      "*.authorization",
      "*.accessToken",
      "*.refreshToken",
      "*.cardNumber",
      "*.cvv",
    ],
    censor: "[REDACTED]",
  },
});
