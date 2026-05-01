// server/db.js — PostgreSQL connection pool + schema bootstrap
require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool(
  process.env.DATABASE_URL
    ? { connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } }
    : {
        host:     process.env.DB_HOST     || 'localhost',
        port:     parseInt(process.env.DB_PORT) || 5432,
        database: process.env.DB_NAME     || 'netwatch_ng',
        user:     process.env.DB_USER     || 'postgres',
        password: process.env.DB_PASSWORD || '',
      }
);

// ─── Create all tables if they don't exist ───────────────────────
async function bootstrap() {
  const client = await pool.connect();
  try {
    await client.query(`
      -- Real towers imported from OpenCelliD (621.csv)
      CREATE TABLE IF NOT EXISTS towers (
        id          SERIAL PRIMARY KEY,
        cell_id     BIGINT,
        radio       VARCHAR(10),        -- GSM | UMTS | LTE | NR
        mcc         INT,                -- 621 = Nigeria
        mnc         INT,                -- operator code
        lac         INT,                -- Location Area Code
        lon         DOUBLE PRECISION,
        lat         DOUBLE PRECISION,
        range_m     INT,                -- estimated coverage radius (metres)
        samples     INT,                -- number of crowd-sourced measurements
        average_signal INT,            -- avg signal in dBm (0 if unknown)
        created_at  BIGINT,            -- unix timestamp
        updated_at  BIGINT,            -- unix timestamp
        operator    VARCHAR(20),        -- MTN | Airtel | Glo | 9mobile | Unknown
        network     VARCHAR(5),         -- 2G | 3G | 4G | 5G
        city        VARCHAR(60),        -- nearest Nigerian city (resolved on import)
        UNIQUE(cell_id, lac, mnc)
      );

      CREATE INDEX IF NOT EXISTS idx_towers_operator  ON towers(operator);
      CREATE INDEX IF NOT EXISTS idx_towers_network   ON towers(network);
      CREATE INDEX IF NOT EXISTS idx_towers_city      ON towers(city);
      CREATE INDEX IF NOT EXISTS idx_towers_latlon    ON towers(lat, lon);

      -- Live network performance readings (auto-collected every 3 s)
      CREATE TABLE IF NOT EXISTS readings (
        id           SERIAL PRIMARY KEY,
        tower_id     INT REFERENCES towers(id) ON DELETE CASCADE,
        cell_id      BIGINT,
        city         VARCHAR(60),
        operator     VARCHAR(20),
        network      VARCHAR(5),
        signal_dbm   DOUBLE PRECISION,
        latency_ms   INT,
        dl_mbps      DOUBLE PRECISION,
        ul_mbps      DOUBLE PRECISION,
        rsrp         DOUBLE PRECISION,
        rsrq         DOUBLE PRECISION,
        recorded_at  TIMESTAMP DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_readings_tower      ON readings(tower_id);
      CREATE INDEX IF NOT EXISTS idx_readings_operator   ON readings(operator);
      CREATE INDEX IF NOT EXISTS idx_readings_city       ON readings(city);
      CREATE INDEX IF NOT EXISTS idx_readings_recorded   ON readings(recorded_at DESC);

      -- Speed test results
      CREATE TABLE IF NOT EXISTS speed_tests (
        id          SERIAL PRIMARY KEY,
        server_city VARCHAR(60),
        operator    VARCHAR(20),
        dl_mbps     DOUBLE PRECISION,
        ul_mbps     DOUBLE PRECISION,
        ping_ms     DOUBLE PRECISION,
        jitter_ms   DOUBLE PRECISION,
        rating      VARCHAR(20),
        tested_at   TIMESTAMP DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_speedtest_city ON speed_tests(server_city);

      -- IODA outage events (country + ASN/operator-level)
      CREATE TABLE IF NOT EXISTS outages (
        id           SERIAL PRIMARY KEY,
        source       VARCHAR(20) DEFAULT 'IODA',
        entity_type  VARCHAR(20) NOT NULL, -- country | asn
        entity_code  VARCHAR(40) NOT NULL, -- NG or ASN id
        operator     VARCHAR(20) NOT NULL, -- MTN | Airtel | Glo | 9mobile | Nigeria
        severity     DOUBLE PRECISION,
        signal_type  VARCHAR(20),          -- bgp | ping | ibr
        started_at   TIMESTAMP NOT NULL,
        ended_at     TIMESTAMP,
        duration_min INT,
        raw_data     JSONB
      );

      CREATE INDEX IF NOT EXISTS idx_outages_operator    ON outages(operator);
      CREATE INDEX IF NOT EXISTS idx_outages_started_at  ON outages(started_at DESC);
      CREATE INDEX IF NOT EXISTS idx_outages_entity_code ON outages(entity_code);
      CREATE UNIQUE INDEX IF NOT EXISTS idx_outages_unique
        ON outages(source, entity_type, entity_code, operator, signal_type, started_at, ended_at);
    `);
    console.log('✅  Database schema ready');
  } finally {
    client.release();
  }
}

module.exports = { pool, bootstrap };
