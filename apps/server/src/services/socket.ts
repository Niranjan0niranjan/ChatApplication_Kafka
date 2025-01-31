import { Server } from "socket.io";
import Redis from "ioredis";
import prismaClient from "./prisma";
import { produceMessage } from "./kafka";

// Initialize Redis clients for publishing and subscribing to messages
const pub = new Redis({
    host: "redis-19400.crce182.ap-south-1-1.ec2.redns.redis-cloud.com",
    port: 19400,
    username: "default",
    password: "8u3I6ddaSt9i880HIKbxCZveJOIFd0PK",
    // tls: false // Disables SSL
});

const sub = new Redis({
    host: "redis-19400.crce182.ap-south-1-1.ec2.redns.redis-cloud.com",
    port: 19400,
    username: "default",
    password: "8u3I6ddaSt9i880HIKbxCZveJOIFd0PK",
    // tls: false // Disables SSL
});

// SocketService class to manage socket connections and messaging
class SocketService {
  private _io: Server;

  constructor() {
    console.log("Init Socket Service...");
    this._io = new Server({
      cors: {
        allowedHeaders: ["*"],
        origin: "*",
      },
    });
    // Subscribe to the "MESSAGES" channel
    sub.subscribe("MESSAGES");
  }

  public initListeners() {
    const io = this.io;
    console.log("Init Socket Listeners...");

    io.on("connect", (socket) => {
      console.log(`New Socket Connected`, socket.id);
      socket.on("event:message", async ({ message }: { message: string }) => {
        console.log("New Message Rec.", message);
        // Publish this message to Redis
        await pub.publish("MESSAGES", JSON.stringify({ message }));
      });
    });

    // Handle messages received from Redis
    sub.on("message", async (channel, message) => {
      if (channel === "MESSAGES") {
        console.log("new message from redis", message);
        io.emit("message", message);
        await produceMessage(message);
        // Optionally store the message in the database
        // await prismaClient.message.create({
        //     data: {
        //         text: message
        //     }
        // });
        console.log("Message Produced to Kafka Broker");
      }
    });
  }

  get io() {
    return this._io;
  }
}

export default SocketService;
