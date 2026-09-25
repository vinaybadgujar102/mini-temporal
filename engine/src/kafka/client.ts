import { Kafka } from "kafkajs";

export const kafka = new Kafka({
  clientId: "mini-temporel",
  brokers: ["localhost:9092"],
  enforceRequestTimeout: false,
});

export const producer = kafka.producer();
