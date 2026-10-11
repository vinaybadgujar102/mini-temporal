import { metricsRegistry } from "./metrics";

export function startMetricsServer() {
  const port = Number(process.env.METRICS_PORT ?? 9464);

  const server = Bun.serve({
    port,

    async fetch(request) {
      const url = new URL(request.url);

      if (url.pathname === "/healthz") {
        return new Response("ok");
      }

      if (url.pathname === "/metrics") {
        return new Response(await metricsRegistry.metrics(), {
          headers: {
            "Content-Type": metricsRegistry.contentType,
          },
        });
      }

      return new Response("Not Found", { status: 404 });
    },
  });

  return server;
}
