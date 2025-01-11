const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const cors = require('cors');
const redis = require('redis');

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

const redisClient = redis.createClient({
  host: '127.0.0.1',  // Adjust if using a remote Redis instance
  port: 6379,
});

redisClient.on('connect', () => {
  console.log('Connected to Redis');
});

redisClient.on('error', (err) => {
  console.error(`Error connecting to Redis: ${err}`);
});

app.use(cors());  // CORS middleware
app.use(express.json()); // JSON parsing middleware

io.on('connection', (socket) => {

  if (!redisClient.isOpen) {
    redisClient.connect();
  }
  
  console.log('New client connected:', socket.id);

  // Handle 'join-room' event
  socket.on('join-room', async (roomId) => {
    socket.join(roomId);
    console.log(`${socket.id} joined room ${roomId}`);

    try {
      // Fetch all messages from Redis for the given roomId
      let messages = await redisClient.lRange(`room:${roomId}`, 0, -1);
      messages = messages.reverse();

      console.log('Messages from Redis:', messages);

      // Parse messages from JSON format
      const parsedMessages = messages
        .map((msg) => {
          try {
            return JSON.parse(msg);
          } catch (error) {
            console.error('Error parsing message:', msg, error);
            return null; // Skip invalid messages
          }
        })
        .filter((msg) => msg !== null);

      console.log('Previous messages:', parsedMessages);

      // Emit the parsed messages to the client
      socket.emit('previous-messages', parsedMessages);
    } catch (error) {
      console.error('Error fetching messages from Redis or emitting event:', error);
      socket.emit('error', { message: 'Failed to load previous messages' });
    }
  });

  // Handle 'send-message' event
  socket.on('send-message', async (data) => {
    try {
      // Save message to Redis
      const message = {
        message: data.message,
        sender: data.sender,
        roomId: data.roomId,
        timestamp: new Date(),
      };
      console.log('Saving message:', message);

      await redisClient.lPush(`room:${data.roomId}`, JSON.stringify(message));

      // Emit the message to all clients in the room
      io.to(data.roomId).emit('receive-message', message);
    } catch (error) {
      console.error('Error saving message to Redis or emitting event:', error);
      socket.emit('error', { message: 'Failed to send message' });
    }
  });

  // Handle disconnect event
  socket.on('disconnect', () => {
    console.log(`${socket.id} disconnected`);
  });
});

const PORT = process.env.PORT || 8000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
