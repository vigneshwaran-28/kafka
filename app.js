const kafka = require("kafka-node");

const user = new kafka.KafkaClient({
  kafkaHost: "localhost:3480",
});

user.on("ready", () => {
  console.log("Kafka Connected");
});

user.on("error", (error) => {
  console.error("Error connecting to Kafka:", error);
});

const producer = new kafka.Producer(user);

producer.on("ready", () => {
  const payload = [
    {
      topic: "My-topic",
      messages: "Hello!",
    },
  ];

  producer.send(payload, (error, data) => {
    if (error) {
      console.error("Error in publishing message:", error);
    } else {
      console.log("Message successfully published:", data);
    }
  });
});

producer.on("error", (error) => {
  console.error("Error connecting to Kafka:", error);
});

const consumer = new kafka.Consumer(
  new kafka.KafkaClient({ kafkaHost: "localhost:3480" }),
  [{ topic: "my-topic" }]
);

// Callback function to handle messages received
function processMessage(message) {
  // output the message
  console.log(message.value);
}

// Register the callback function with the consumer
consumer.on("message", processMessage);