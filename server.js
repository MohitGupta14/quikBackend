const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const cors = require('cors');
const redis = require('redis');
const { Kafka } = require('kafkajs');

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

// Improved Redis setup with connection management
const redisClient = redis.createClient({
  url: 'redis://redis-stack:6379',
  socket: {
    reconnectStrategy: (retries) => {
      if (retries > 10) {
        console.error('Max redis reconnection attempts reached');
        return new Error('Max redis reconnection attempts reached');
      }
      // Reconnect after retries * 1000 ms
      return Math.min(retries * 1000, 5000);
    }
  }
});

// Redis error handling and connection management
async function setupRedis() {
  try {
    redisClient.on('error', (err) => {
      console.error('Redis Client Error:', err);
    });

    redisClient.on('connect', () => {
      console.log('Connected to Redis');
    });

    redisClient.on('reconnecting', () => {
      console.log('Reconnecting to Redis...');
    });

    await redisClient.connect();
  } catch (error) {
    console.error('Failed to connect to Redis:', error);
    process.exit(1);
  }
}

app.use(cors());
app.use(express.json());

// Kafka setup remains the same
const kafka = new Kafka({
  clientId: 'chat-app',
  brokers: ['kafka:9093'],  
});

const kafkaProducer = kafka.producer();
const kafkaConsumer = kafka.consumer({ groupId: 'chat-group' });

// Kafka setup function remains the same
async function setupKafka() {
  try {
    await kafkaProducer.connect();
    console.log('Kafka Producer connected');

    await kafkaConsumer.connect();
    console.log('Kafka Consumer connected');

    await kafkaConsumer.subscribe({ topic: 'chat-messages', fromBeginning: true });

    await kafkaConsumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const parsedMessage = JSON.parse(message.value.toString());
          console.log('Received message from Kafka:', parsedMessage);
          io.to(parsedMessage.roomId).emit('receive-message', parsedMessage);
        } catch (error) {
          console.error('Error processing Kafka message:', error);
        }
      },
    });
  } catch (error) {
    console.error('Error in Kafka setup:', error);
    setTimeout(setupKafka, 5000);
  }
}

// Improved Socket.IO setup with proper Redis error handling
io.on('connection', (socket) => {
  console.log('New client connected:', socket.id);

  socket.on('join-room', async (roomId) => {
    try {
      socket.join(roomId);
      console.log(`${socket.id} joined room ${roomId}`);

      if (!redisClient.isOpen) {
        throw new Error('Redis client is not connected');
      }

      const messages = await redisClient.lRange(`room:${roomId}`, 0, -1);
      const parsedMessages = messages
        .reverse()
        .map((msg) => {
          try {
            return JSON.parse(msg);
          } catch (e) {
            console.error('Error parsing message:', e);
            return null;
          }
        })
        .filter(Boolean);

      console.log('Previous messages:', parsedMessages);
      socket.emit('previous-messages', parsedMessages);
    } catch (error) {
      console.error('Error in join-room handler:', error);
      socket.emit('error', { 
        message: 'Failed to load previous messages',
        details: error.message 
      });
    }
  });

  socket.on('send-message', async (data) => {
    try {
      if (!redisClient.isOpen) {
        throw new Error('Redis client is not connected');
      }

      const message = {
        message: data.message,
        sender: data.sender,
        roomId: data.roomId,
        timestamp: new Date().toISOString(),
      };

      await redisClient.lPush(`room:${data.roomId}`, JSON.stringify(message));

      await kafkaProducer.send({
        topic: 'chat-messages',
        messages: [{ value: JSON.stringify(message) }],
      });

      console.log('Message sent to Kafka:', message);
    } catch (error) {
      console.error('Error in send-message handler:', error);
      socket.emit('error', { 
        message: 'Failed to send message',
        details: error.message 
      });
    }
  });

  socket.on('disconnect', () => {
    console.log(`${socket.id} disconnected`);
  });
});

// Improved server startup with proper initialization sequence
const PORT = process.env.PORT || 8000;
async function startServer() {
  try {
    await setupRedis();
    await setupKafka();
    
    server.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

startServer();

// Enhanced shutdown handling
process.on('SIGTERM', async () => {
  console.log('Shutting down...');
  try {
    await kafkaProducer.disconnect();
    await kafkaConsumer.disconnect();
    await redisClient.quit();
    server.close(() => {
      console.log('Server closed');
      process.exit(0);
    });
  } catch (error) {
    console.error('Error during shutdown:', error);
    process.exit(1);
  }
});