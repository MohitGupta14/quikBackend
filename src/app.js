const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const cors = require('cors');
const logger = require('./config/logger');
const redisService = require('./services/redisService');
const kafkaService = require('./services/kafkaService');
const setupSocketIO = require('./socket/socketHandler');
const { SHUTDOWN_TIMEOUT } = require('./config/constants');

const app = express();
const server = http.createServer(app);

const io = socketIo(server, {
  cors: {
    origin: process.env.CORS_ORIGIN || '*',
    methods: ['GET', 'POST'],
    credentials: true
  },
  transports: ['websocket', 'polling'],
  pingTimeout: 10000,
  pingInterval: 25000,
  connectTimeout: 5000,
});

// Middleware setup
app.use(cors({
  origin: process.env.CORS_ORIGIN || '*',
  methods: ['GET', 'POST'],
  credentials: true
}));
app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true, limit: '1mb' }));

// Health check route
app.get('/health', async (req, res) => {
  try {
    const health = {
      status: 'healthy',
      redis: redisService.client.isReady,
      kafka: kafkaService.producer.isConnected() && kafkaService.consumer.isRunning(),
      uptime: process.uptime(),
    };
    
    res.status(200).json(health);
  } catch (error) {
    res.status(503).json({
      status: 'unhealthy',
      error: error.message
    });
  }
});

// Shutdown handler
async function shutdown(signal) {
  logger.info(`Starting graceful shutdown... Signal: ${signal}`);
  
  let exitCode = 0;
  
  try {
    const shutdownPromise = Promise.all([
      kafkaService.shutdown(),
      redisService.shutdown(),
      new Promise((resolve) => {
        server.close(resolve);
      })
    ]);

    await Promise.race([
      shutdownPromise,
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Shutdown timeout')), SHUTDOWN_TIMEOUT)
      ),
    ]);

    logger.info('Graceful shutdown completed');
  } catch (error) {
    logger.error('Shutdown error:', error);
    exitCode = 1;
  } finally {
    process.exit(exitCode);
  }
}

// Error handlers
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('uncaughtException', (error) => {
  logger.error('Uncaught Exception:', error);
  shutdown('uncaughtException');
});
process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled Rejection:', reason);
  shutdown('unhandledRejection');
});

// Server startup
async function start() {
  try {
    await redisService.connect();
    await redisService.initializeAdapter(io);
    await kafkaService.setup(io);
    setupSocketIO(io);

    const PORT = process.env.PORT || 8000;
    server.listen(PORT, () => {
      logger.info(`Server running on port ${PORT}`);
      logger.info(`Environment: ${process.env.NODE_ENV || 'development'}`);
    });
  } catch (error) {
    logger.error('Server startup failed:', error);
    await shutdown();
  }
}

module.exports = { start, app, server };