// src/socket/socketHandler.js
const logger = require('../config/logger');
const { client: redisClient } = require('../services/redisService');
const { producer: kafkaProducer } = require('../services/kafkaService');
const { MESSAGE_RETENTION_LIMIT } = require('../config/constants');

function setupSocketIO(io) {
  io.on('connection', (socket) => {
    logger.info(`Client connected: ${socket.id}`);

    socket.on('join-room', async (roomId) => {
      try {
        if (!roomId || typeof roomId !== 'string') {
          throw new Error('Invalid room ID');
        }

        const previousRooms = Array.from(socket.rooms);
        for (const room of previousRooms) {
          if (room !== socket.id) {
            await socket.leave(room);
          }
        }

        await socket.join(roomId);
        logger.info(`${socket.id} joined room ${roomId}`);

        const messages = await redisClient.lRange(`room:${roomId}`, 0, -1);
        const parsedMessages = messages
          .reverse()
          .map(msg => {
            try {
              return JSON.parse(msg);
            } catch (e) {
              logger.error('Message parsing error:', e);
              return null;
            }
          })
          .filter(Boolean);

        socket.emit('previous-messages', parsedMessages);
      } catch (error) {
        logger.error('Join room error:', error);
        socket.emit('error', {
          message: 'Failed to join room',
          details: error.message
        });
      }
    });

    socket.on('send-message', async (data) => {
      try {
        if (!data || !data.roomId || !data.message) {
          throw new Error('Invalid message data');
        }

        const message = {
          ...data,
          timestamp: new Date().toISOString(),
          id: `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`
        };

        const multi = redisClient.multi();
        multi.lPush(`room:${data.roomId}`, JSON.stringify(message));
        multi.lTrim(`room:${data.roomId}`, 0, MESSAGE_RETENTION_LIMIT - 1);
        await multi.exec();

        await kafkaProducer.send({
          topic: 'chat-messages',
          messages: [{
            key: data.roomId,
            value: JSON.stringify(message)
          }],
        });

        logger.info(`Message ${message.id} sent to room ${data.roomId}`);
      } catch (error) {
        logger.error('Send message error:', error);
        socket.emit('error', {
          message: 'Failed to send message',
          details: error.message
        });
      }
    });

    socket.on('disconnect', (reason) => {
      logger.info(`Client disconnected: ${socket.id}, reason: ${reason}`);
    });

    socket.on('error', (error) => {
      logger.error(`Socket error for ${socket.id}:`, error);
    });
  });
}

module.exports = setupSocketIO;