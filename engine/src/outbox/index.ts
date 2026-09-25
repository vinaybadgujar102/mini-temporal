import { producer } from "../kafka/client";
import { publishOutbox } from "./publisher";

async function main() {
  await producer.connect();

  try {
    await publishOutbox();
  } finally {
    await producer.disconnect();
  }
}

main();
