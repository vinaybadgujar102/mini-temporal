"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.producer = exports.kafka = void 0;
var kafkajs_1 = require("kafkajs");
exports.kafka = new kafkajs_1.Kafka({
    clientId: "mini-temporel",
    brokers: ["localhost:9092"],
    enforceRequestTimeout: false,
});
exports.producer = exports.kafka.producer();
