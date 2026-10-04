import { producer } from "../kafka/client";
import { publishOutbox } from "./publisher";

async function main() {
  await producer.connect();

  try {
    while (true) {
      await publishOutbox();
      await new Promise((r) => setTimeout(r, 500));
    }
  } finally {
    await producer.disconnect();
  }
}

main();
