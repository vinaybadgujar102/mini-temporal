import { startWorkflow } from "../workflow/startWorkflow";

if (process.env.BENCHMARK_MODE !== "true") {
  throw new Error(
    "Benchmark server disabled. Set BENCHMARK_MODE=true to run locally.",
  );
}

const port = Number(process.env.BENCHMARK_PORT ?? 3457);

Bun.serve({
  hostname: "127.0.0.1",
  port,

  async fetch(req) {
    const url = new URL(req.url);

    if (req.method === "GET" && url.pathname === "/healthz") {
      return Response.json({ status: "ok" });
    }

    if (req.method !== "POST" || url.pathname !== "/bench/workflows") {
      return Response.json({ error: "Not found" }, { status: 404 });
    }

    let body: unknown;

    try {
      body = await req.json();
    } catch {
      return Response.json({ error: "Invalid JSON" }, { status: 400 });
    }

    if (
      typeof body !== "object" ||
      body === null ||
      !("runId" in body) ||
      typeof body.runId !== "string" ||
      !/^[a-zA-Z0-9_-]{1,40}$/.test(body.runId)
    ) {
      return Response.json(
        { error: "runId must be 1–40 letters, numbers, _ or -" },
        { status: 400 },
      );
    }

    const runId = body.runId;

    try {
      const workflowId = await startWorkflow({
        type: "PERFORMANCE_BENCHMARK",
        name: `k6-bench-${runId}-${crypto.randomUUID()}`,
        version: 1,
        tasks: [
          {
            id: "task-1",
            name: "benchmark-noop",
            type: "NOOP",
          },
        ],
      });

      return Response.json({ workflowId }, { status: 201 });
    } catch (error) {
      console.error("Benchmark workflow creation failed:", error);

      return Response.json(
        { error: "Workflow creation failed" },
        { status: 500 },
      );
    }
  },
});

console.log(`Benchmark server listening on http://127.0.0.1:${port}`);
