// server/index.js — NetWatch NG Express server
require('dotenv').config();
const express  = require('express');
const cors     = require('cors');
const path     = require('path');
const { bootstrap } = require('./db');
const routes   = require('./routes');
const { runSyncIODA } = require('../scripts/syncIODA');

const app  = express();
const PORT = process.env.PORT || 3001;

// ─── Middleware ───────────────────────────────────────────────────
app.use(cors());                          // allow requests from the dashboard
app.use(express.json({ limit: '2mb' })); // parse JSON bodies (bulk readings)
app.use(express.static(path.join(__dirname, '../public'))); // serve dashboard HTML

// ─── API routes ───────────────────────────────────────────────────
app.use('/api', routes);

// ─── Serve dashboard for any non-API route ────────────────────────
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

// ─── Start ────────────────────────────────────────────────────────
async function start() {
  try {
    await bootstrap();              // create tables if they don't exist
    const IODA_SYNC_MS = 6 * 60 * 60 * 1000;
    app.listen(PORT, () => {
      console.log('');
      console.log('┌─────────────────────────────────────────┐');
      console.log('│   🗼  NetWatch NG  —  Server running     │');
      console.log(`│   http://localhost:${PORT}                  │`);
      console.log('└─────────────────────────────────────────┘');
      console.log('');
      console.log('  API endpoints:');
      console.log(`  GET  /api/health`);
      console.log(`  GET  /api/towers`);
      console.log(`  GET  /api/towers/stats`);
      console.log(`  GET  /api/readings/latest`);
      console.log(`  GET  /api/readings/history`);
      console.log(`  GET  /api/readings/summary`);
      console.log(`  POST /api/readings`);
      console.log(`  GET  /api/speedtests`);
      console.log(`  POST /api/speedtests`);
      console.log(`  GET  /api/outages`);
      console.log(`  GET  /api/outages/stats`);
      console.log(`  GET  /api/outages/correlate`);
      console.log(`  GET  /api/outages/risk`);
      console.log(`  POST /api/outages/sync`);
      console.log('');
    });

    setInterval(async () => {
      try {
        const result = await runSyncIODA();
        console.log(`[IODA] Auto sync complete: attempted=${result.attempted}, inserted=${result.inserted}`);
      } catch (err) {
        console.error(`[IODA] Auto sync failed: ${err.message}`);
      }
    }, IODA_SYNC_MS);
  } catch (err) {
    console.error('❌  Failed to start server:', err.message);
    process.exit(1);
  }
}

start();
