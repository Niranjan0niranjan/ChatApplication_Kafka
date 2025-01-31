import { Kafka, Producer } from "kafkajs";
import fs from "fs";
import path from "path";
import prismaClient from "./prisma";
import { log } from "console";

// Initialize Kafka client with brokers, SSL, and SASL configurations
const kafka = new Kafka({
  brokers: ["kafka-2d6e4dc9-cse-fd5a.b.aivencloud.com:15519"],
  ssl: {
    ca: [fs.readFileSync(path.resolve("./ca.pem"), "utf-8")],
  },
  sasl: {
    username: "avnadmin",
    password: "",
    mechanism: "plain",
  },
});

let producer: null | Producer = null;

// Function to create a Kafka producer if not already present
export async function createProducer() {
  if (producer){
    log("producerWasAlreadyPresent");
    return producer;
  } 

  const _producer = kafka.producer();
  await _producer.connect();
  producer = _producer;
  return producer;
}

// Function to produce a message to the Kafka topic "MESSAGES"
export async function produceMessage(message: string) {
  const producer = await createProducer();
  await producer.send({
    messages: [{ key: `message-${Date.now()}`, value: message }],
    topic: "MESSAGES",
  });
  return true;
}

// Function to start a Kafka message consumer
export async function startMessageConsumer() {
  console.log("Consumer is running..");
  const consumer = kafka.consumer({ groupId: "default" });
  await consumer.connect();
  await consumer.subscribe({ topic: "MESSAGES", fromBeginning: true });

  await consumer.run({
    autoCommit: true,
    eachMessage: async ({ message, pause }) => {
      if (!message.value) return;
      console.log(`New Message Recv..`);
      try {
        // Store the received message in the database using Prisma
        await prismaClient.message.create({
          data: {
            text: message.value?.toString(),
          },
        });
      } catch (err) {
        console.log("Something is wrong");
        pause();
        setTimeout(() => {
          consumer.resume([{ topic: "MESSAGES" }]);
        }, 60 * 1000);
      }
    },
  });
}

export default kafka;
