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

      -- Synthetic QoS baseline from ElectricSheep Africa / Amon Din.
      -- Uses source_tower_id because dataset tower IDs are labels like CAL-6586,
      -- not numeric OpenCelliD tower primary keys.
      CREATE TABLE IF NOT EXISTS qos_metrics (
        id                   SERIAL PRIMARY KEY,
        metric_id            VARCHAR(40) NOT NULL UNIQUE,
        measured_at          TIMESTAMP NOT NULL,
        source_tower_id      VARCHAR(40),
        city                 VARCHAR(60),
        operator             VARCHAR(20),
        network              VARCHAR(10),
        latency_ms           DOUBLE PRECISION,
        jitter_ms            DOUBLE PRECISION,
        throughput_mbps      DOUBLE PRECISION,
        packet_loss_rate     DOUBLE PRECISION,
        error_rate           DOUBLE PRECISION,
        signal_strength_dbm  DOUBLE PRECISION,
        active_users         INT,
        source               VARCHAR(80) DEFAULT 'ElectricSheep Africa synthetic QoS dataset',
        imported_at          TIMESTAMP DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_qos_metrics_measured_at ON qos_metrics(measured_at DESC);
      CREATE INDEX IF NOT EXISTS idx_qos_metrics_city        ON qos_metrics(city);
      CREATE INDEX IF NOT EXISTS idx_qos_metrics_operator    ON qos_metrics(operator);
      CREATE INDEX IF NOT EXISTS idx_qos_metrics_network     ON qos_metrics(network);
      CREATE INDEX IF NOT EXISTS idx_qos_metrics_source_tower ON qos_metrics(source_tower_id);

      -- Synthetic geospatial coverage baseline from ElectricSheep Africa / Amon Din.
      CREATE TABLE IF NOT EXISTS coverage_data (
        id                    SERIAL PRIMARY KEY,
        measurement_id        VARCHAR(40) NOT NULL UNIQUE,
        lat                   DOUBLE PRECISION NOT NULL,
        lon                   DOUBLE PRECISION NOT NULL,
        city                  VARCHAR(60),
        operator              VARCHAR(20),
        network               VARCHAR(10),
        signal_strength_dbm   DOUBLE PRECISION,
        coverage_quality      VARCHAR(20),
        download_speed_mbps   DOUBLE PRECISION,
        upload_speed_mbps     DOUBLE PRECISION,
        latency_ms            DOUBLE PRECISION,
        source_tower_id       VARCHAR(40),
        distance_to_tower_km  DOUBLE PRECISION,
        indoor_outdoor        VARCHAR(20),
        source                VARCHAR(90) DEFAULT 'ElectricSheep Africa synthetic coverage dataset',
        imported_at           TIMESTAMP DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_coverage_data_city         ON coverage_data(city);
      CREATE INDEX IF NOT EXISTS idx_coverage_data_operator     ON coverage_data(operator);
      CREATE INDEX IF NOT EXISTS idx_coverage_data_network      ON coverage_data(network);
      CREATE INDEX IF NOT EXISTS idx_coverage_data_latlon       ON coverage_data(lat, lon);
      CREATE INDEX IF NOT EXISTS idx_coverage_data_source_tower ON coverage_data(source_tower_id);

      -- Synthetic network event logs from ElectricSheep Africa / Amon Din.
      CREATE TABLE IF NOT EXISTS network_events (
        id                   SERIAL PRIMARY KEY,
        event_id             VARCHAR(40) NOT NULL UNIQUE,
        event_time           TIMESTAMP NOT NULL,
        source_tower_id      VARCHAR(40),
        city                 VARCHAR(60),
        event_type           VARCHAR(60),
        severity             VARCHAR(20),
        affected_users       INT,
        packet_loss_percent  DOUBLE PRECISION,
        network              VARCHAR(10),
        operator             VARCHAR(20),
        source               VARCHAR(90) DEFAULT 'ElectricSheep Africa synthetic network event logs',
        imported_at          TIMESTAMP DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_network_events_time         ON network_events(event_time DESC);
      CREATE INDEX IF NOT EXISTS idx_network_events_operator     ON network_events(operator);
      CREATE INDEX IF NOT EXISTS idx_network_events_city         ON network_events(city);
      CREATE INDEX IF NOT EXISTS idx_network_events_event_type   ON network_events(event_type);
      CREATE INDEX IF NOT EXISTS idx_network_events_source_tower ON network_events(source_tower_id);

      -- Synthetic hardware sensor readings from ElectricSheep Africa / Amon Din.
      CREATE TABLE IF NOT EXISTS hardware_sensors (
        id                    SERIAL PRIMARY KEY,
        sensor_id             VARCHAR(40) NOT NULL UNIQUE,
        sensor_time           TIMESTAMP NOT NULL,
        source_tower_id       VARCHAR(40),
        city                  VARCHAR(60),
        equipment_type        VARCHAR(40),
        temperature_celsius   DOUBLE PRECISION,
        power_draw_watts      DOUBLE PRECISION,
        voltage_v             DOUBLE PRECISION,
        humidity_percent      DOUBLE PRECISION,
        vibration_level       DOUBLE PRECISION,
        health_status         VARCHAR(20),
        alert_triggered       BOOLEAN,
        source                VARCHAR(90) DEFAULT 'ElectricSheep Africa synthetic hardware sensor data',
        imported_at           TIMESTAMP DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_hardware_sensors_time         ON hardware_sensors(sensor_time DESC);
      CREATE INDEX IF NOT EXISTS idx_hardware_sensors_city         ON hardware_sensors(city);
      CREATE INDEX IF NOT EXISTS idx_hardware_sensors_equipment    ON hardware_sensors(equipment_type);
      CREATE INDEX IF NOT EXISTS idx_hardware_sensors_health       ON hardware_sensors(health_status);
      CREATE INDEX IF NOT EXISTS idx_hardware_sensors_alert        ON hardware_sensors(alert_triggered);
      CREATE INDEX IF NOT EXISTS idx_hardware_sensors_source_tower ON hardware_sensors(source_tower_id);
    `);
    console.log('✅  Database schema ready');
  } finally {
    client.release();
  }
}

module.exports = { pool, bootstrap };
