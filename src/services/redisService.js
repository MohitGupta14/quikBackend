// src/services/redisService.js
const { createClient } = require('redis');
const { createAdapter } = require('@socket.io/redis-adapter');
const logger = require('../config/logger');
const { REDIS_RECONNECT_MAX_ATTEMPTS, REDIS_RECONNECT_MAX_DELAY } = require('../config/constants');

const redisConfig = {
  url: process.env.REDIS_URL || 'redis://redis-stack:6379',
  socket: {
    reconnectStrategy: (retries) => {
      if (retries > REDIS_RECONNECT_MAX_ATTEMPTS) {
        logger.error('Max Redis reconnection attempts reached');
        return new Error('Max Redis reconnection attempts reached');
      }
      return Math.min(retries * 1000, REDIS_RECONNECT_MAX_DELAY);
    },
    connectTimeout: 10000,
    keepAlive: 5000,
  },
};

const client = createClient(redisConfig);
const pubClient = createClient(redisConfig);
const subClient = pubClient.duplicate();

async function connect() {
  try {
    await client.connect();
    
    client.on('error', (err) => {
      logger.error('Redis Client Error:', err);
    });

    client.on('reconnecting', () => {
      logger.info('Reconnecting to Redis...');
    });

    logger.info('Connected to Redis');
  } catch (error) {
    logger.error('Redis connection failed:', error);
    throw error;
  }
}

async function initializeAdapter(io) {
  let attempts = 0;
  const maxAttempts = 5;

  while (attempts < maxAttempts) {
    try {
      await Promise.all([pubClient.connect(), subClient.connect()]);
      io.adapter(createAdapter(pubClient, subClient));
      logger.info('Socket.IO Redis adapter initialized');
      return;
    } catch (error) {
      attempts++;
      logger.error(`Redis adapter initialization attempt ${attempts} failed:`, error);
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }
  throw new Error('Redis adapter initialization failed after max attempts');
}

async function shutdown() {
  await Promise.all([
    client.quit(),
    pubClient.quit(),
    subClient.quit()
  ]);
}

module.exports = {
  client,
  connect,
  initializeAdapter,
  shutdown
};