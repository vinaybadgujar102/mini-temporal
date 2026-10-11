import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

const creationLatency = new Trend("workflow_creation_latency", true);
const creationFailures = new Counter("workflow_creation_failures");
const creationSuccessRate = new Rate("workflow_creation_success_rate");

const baseUrl = __ENV.BASE_URL || "http://127.0.0.1:3457";
const runId = __ENV.RUN_ID || "smoke-k6-001";

export const options = {
  scenarios: {
    workflow_creation: {
      executor: "constant-arrival-rate",
      rate: 5,
      timeUnit: "1s",
      duration: "60s",
      preAllocatedVUs: 5,
      maxVUs: 15,
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    workflow_creation_latency: ["p(95)<500"],
    workflow_creation_success_rate: ["rate>0.99"],
  },
};

export default function () {
  const response = http.post(
    `${baseUrl}/bench/workflows`,
    JSON.stringify({ runId }),
    {
      headers: { "Content-Type": "application/json" },
      tags: { operation: "create_workflow" },
    },
  );

  const success = check(response, {
    "status is 201": (r) => r.status === 201,
    "workflow ID returned": (r) => {
      try {
        return Boolean(JSON.parse(r.body).workflowId);
      } catch {
        return false;
      }
    },
  });

  creationLatency.add(response.timings.duration);
  creationSuccessRate.add(success);

  if (!success) {
    creationFailures.add(1);
    console.error(`Creation failed: status=${response.status}`);
  }

  sleep(0.1);
}
