import { producer } from "../kafka/client";
import { logger } from "../observability/logger";
import { startMetricsServer } from "../observability/metricsServer";
import { publishOutbox } from "./publisher";

async function main() {
  await producer.connect();
  const metricsServer = startMetricsServer();

  try {
    while (true) {
      await publishOutbox();
      logger.info(
        {
          event: "outbox.service.started",
          metricsPort: Number(process.env.METRICS_PORT ?? 9464),
        },
        "Outbox publisher started",
      );
      while (true) {
        await publishOutbox();
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }
  } finally {
    try {
      await producer.disconnect();
    } finally {
      metricsServer.stop();
    }
  }
}

main().catch((error) => {
  logger.fatal(
    {
      event: "outbox.service.failed",
      err: error,
    },
    "Outbox publisher stopped unexpectedly",
  );

  process.exit(1);
});
