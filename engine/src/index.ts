import { pool } from "./db/client";
import { startWorkflow } from "./workflow/startWorkflow";

export { startWorkflow } from "./workflow/startWorkflow";
export type {
  WorkflowDefinition,
  WorkflowTask,
} from "./workflow/startWorkflow";

const workflowId = await startWorkflow({
  type: "ORDER_PROCESSING",
  name: "Process Order",
  version: 1,

  tasks: [
    {
      id: "charge",
      name: "Charge Payment",
      type: "CHARGE_PAYMENT",
    },
    {
      id: "reserve",
      name: "Reserve Inventory",
      type: "RESERVE_INVENTORY",
    },
    {
      id: "email",
      name: "Send Confirmation",
      type: "SEND_EMAIL",
      dependsOn: ["charge", "reserve"],
    },
  ],
});

console.log("Started:", workflowId);

await pool.end();
