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

      -- Synthetic base-station uptime logs from ElectricSheep Africa / Amon Din.
      CREATE TABLE IF NOT EXISTS uptime_logs (
        id                    SERIAL PRIMARY KEY,
        source_tower_id       VARCHAR(40) NOT NULL,
        log_date              DATE NOT NULL,
        operator              VARCHAR(20),
        city                  VARCHAR(60),
        state                 VARCHAR(60),
        uptime_percentage     DOUBLE PRECISION,
        downtime_minutes      DOUBLE PRECISION,
        outage_count          INT,
        outage_reason         VARCHAR(40),
        network               VARCHAR(10),
        avg_users_affected    INT,
        source                VARCHAR(90) DEFAULT 'ElectricSheep Africa synthetic base-station uptime logs',
        imported_at           TIMESTAMP DEFAULT NOW(),
        UNIQUE(source_tower_id, log_date, operator, network)
      );

      CREATE INDEX IF NOT EXISTS idx_uptime_logs_date          ON uptime_logs(log_date DESC);
      CREATE INDEX IF NOT EXISTS idx_uptime_logs_operator      ON uptime_logs(operator);
      CREATE INDEX IF NOT EXISTS idx_uptime_logs_city          ON uptime_logs(city);
      CREATE INDEX IF NOT EXISTS idx_uptime_logs_state         ON uptime_logs(state);
      CREATE INDEX IF NOT EXISTS idx_uptime_logs_outage_reason ON uptime_logs(outage_reason);
      CREATE INDEX IF NOT EXISTS idx_uptime_logs_source_tower  ON uptime_logs(source_tower_id);

      -- Synthetic tower energy records from ElectricSheep Africa / Amon Din.
      CREATE TABLE IF NOT EXISTS energy_consumption (
        id                       SERIAL PRIMARY KEY,
        source_tower_id          VARCHAR(40) NOT NULL,
        usage_date               DATE NOT NULL,
        city                     VARCHAR(60),
        power_source             VARCHAR(40),
        daily_consumption_kwh    DOUBLE PRECISION,
        cost_per_kwh_ngn         DOUBLE PRECISION,
        total_cost_ngn           DOUBLE PRECISION,
        grid_availability_hours  INT,
        generator_runtime_hours  INT,
        solar_generation_kwh     DOUBLE PRECISION,
        fuel_consumed_liters     DOUBLE PRECISION,
        carbon_emissions_kg      DOUBLE PRECISION,
        source                   VARCHAR(90) DEFAULT 'ElectricSheep Africa synthetic tower energy records',
        imported_at              TIMESTAMP DEFAULT NOW(),
        UNIQUE(source_tower_id, usage_date, power_source)
      );

      CREATE INDEX IF NOT EXISTS idx_energy_consumption_date         ON energy_consumption(usage_date DESC);
      CREATE INDEX IF NOT EXISTS idx_energy_consumption_city         ON energy_consumption(city);
      CREATE INDEX IF NOT EXISTS idx_energy_consumption_power_source ON energy_consumption(power_source);
      CREATE INDEX IF NOT EXISTS idx_energy_consumption_source_tower ON energy_consumption(source_tower_id);

      -- Synthetic maintenance work orders from ElectricSheep Africa / Amon Din.
      CREATE TABLE IF NOT EXISTS maintenance_orders (
        id                     SERIAL PRIMARY KEY,
        work_order_id          VARCHAR(40) NOT NULL UNIQUE,
        source_tower_id        VARCHAR(40),
        city                   VARCHAR(60),
        operator               VARCHAR(20),
        maintenance_type       VARCHAR(40),
        issue_category         VARCHAR(60),
        priority               VARCHAR(20),
        status                 VARCHAR(30),
        scheduled_date         DATE,
        completed_date         DATE,
        technician_id          VARCHAR(40),
        repair_duration_hours  DOUBLE PRECISION,
        parts_replaced         VARCHAR(120),
        cost_ngn               DOUBLE PRECISION,
        downtime_minutes       DOUBLE PRECISION,
        source                 VARCHAR(90) DEFAULT 'ElectricSheep Africa synthetic maintenance work orders',
        imported_at            TIMESTAMP DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_maintenance_orders_tower    ON maintenance_orders(source_tower_id);
      CREATE INDEX IF NOT EXISTS idx_maintenance_orders_city     ON maintenance_orders(city);
      CREATE INDEX IF NOT EXISTS idx_maintenance_orders_operator ON maintenance_orders(operator);
      CREATE INDEX IF NOT EXISTS idx_maintenance_orders_type     ON maintenance_orders(maintenance_type);
      CREATE INDEX IF NOT EXISTS idx_maintenance_orders_issue    ON maintenance_orders(issue_category);
      CREATE INDEX IF NOT EXISTS idx_maintenance_orders_status   ON maintenance_orders(status);
      CREATE INDEX IF NOT EXISTS idx_maintenance_orders_sched    ON maintenance_orders(scheduled_date DESC);

      -- Synthetic ultra-low-latency records from ElectricSheep Africa / Amon Din.
      CREATE TABLE IF NOT EXISTS latency_logs (
        id                    SERIAL PRIMARY KEY,
        log_id                VARCHAR(40) NOT NULL UNIQUE,
        measured_at           TIMESTAMP NOT NULL,
        source_tower_id       VARCHAR(40),
        city                  VARCHAR(60),
        operator              VARCHAR(20),
        network               VARCHAR(10),
        application_type      VARCHAR(60),
        latency_ms            DOUBLE PRECISION,
        jitter_ms             DOUBLE PRECISION,
        packet_loss_percent   DOUBLE PRECISION,
        throughput_mbps       DOUBLE PRECISION,
        edge_server_id        VARCHAR(40),
        edge_distance_km      DOUBLE PRECISION,
        sla_target_ms         DOUBLE PRECISION,
        sla_met               BOOLEAN,
        reliability_percent   DOUBLE PRECISION,
        active_connections    INT,
        source                VARCHAR(90) DEFAULT 'ElectricSheep Africa synthetic ultra-low-latency logs',
        imported_at           TIMESTAMP DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_latency_logs_measured_at ON latency_logs(measured_at DESC);
      CREATE INDEX IF NOT EXISTS idx_latency_logs_tower       ON latency_logs(source_tower_id);
      CREATE INDEX IF NOT EXISTS idx_latency_logs_city        ON latency_logs(city);
      CREATE INDEX IF NOT EXISTS idx_latency_logs_operator    ON latency_logs(operator);
      CREATE INDEX IF NOT EXISTS idx_latency_logs_network     ON latency_logs(network);

      -- Synthetic cell-tower handover records from ElectricSheep Africa / Amon Din.
      CREATE TABLE IF NOT EXISTS handover_records (
        id                         SERIAL PRIMARY KEY,
        handover_id                VARCHAR(40) NOT NULL UNIQUE,
        handover_time              TIMESTAMP NOT NULL,
        source_tower_id            VARCHAR(40),
        target_tower_id            VARCHAR(40),
        source_city                VARCHAR(60),
        target_city                VARCHAR(60),
        operator                   VARCHAR(20),
        network                    VARCHAR(10),
        handover_type              VARCHAR(40),
        success                    BOOLEAN,
        duration_ms                DOUBLE PRECISION,
        source_signal_dbm          DOUBLE PRECISION,
        target_signal_dbm          DOUBLE PRECISION,
        failure_reason             VARCHAR(80),
        active_call                BOOLEAN,
        active_data_session        BOOLEAN,
        device_speed_kmh           DOUBLE PRECISION,
        source                     VARCHAR(90) DEFAULT 'ElectricSheep Africa synthetic cell-tower handover data',
        imported_at                TIMESTAMP DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_handover_records_time      ON handover_records(handover_time DESC);
      CREATE INDEX IF NOT EXISTS idx_handover_records_operator  ON handover_records(operator);
      CREATE INDEX IF NOT EXISTS idx_handover_records_network   ON handover_records(network);
      CREATE INDEX IF NOT EXISTS idx_handover_records_success   ON handover_records(success);
      CREATE INDEX IF NOT EXISTS idx_handover_records_source    ON handover_records(source_tower_id);
      CREATE INDEX IF NOT EXISTS idx_handover_records_target    ON handover_records(target_tower_id);
      CREATE INDEX IF NOT EXISTS idx_handover_records_cities    ON handover_records(source_city, target_city);

      -- Synthetic dropped-call records from ElectricSheep Africa / Amon Din.
      CREATE TABLE IF NOT EXISTS dropped_calls (
        id                         SERIAL PRIMARY KEY,
        drop_id                    VARCHAR(40) NOT NULL UNIQUE,
        dropped_at                 TIMESTAMP NOT NULL,
        operator                   VARCHAR(20),
        calling_number             VARCHAR(30),
        called_number              VARCHAR(30),
        source_tower_id            VARCHAR(40),
        city                       VARCHAR(60),
        call_duration_before_drop  INT,
        drop_reason                VARCHAR(60),
        signal_strength_dbm        DOUBLE PRECISION,
        network                    VARCHAR(10),
        source                     VARCHAR(90) DEFAULT 'ElectricSheep Africa synthetic dropped-call records',
        imported_at                TIMESTAMP DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_dropped_calls_time      ON dropped_calls(dropped_at DESC);
      CREATE INDEX IF NOT EXISTS idx_dropped_calls_operator  ON dropped_calls(operator);
      CREATE INDEX IF NOT EXISTS idx_dropped_calls_city      ON dropped_calls(city);
      CREATE INDEX IF NOT EXISTS idx_dropped_calls_reason    ON dropped_calls(drop_reason);
      CREATE INDEX IF NOT EXISTS idx_dropped_calls_tower     ON dropped_calls(source_tower_id);

      -- Synthetic mobility traces from ElectricSheep Africa / Amon Din.
      CREATE TABLE IF NOT EXISTS mobility_traces (
        id                  SERIAL PRIMARY KEY,
        trace_id            VARCHAR(40) NOT NULL UNIQUE,
        trace_time          TIMESTAMP NOT NULL,
        customer_id         VARCHAR(40),
        lat                 DOUBLE PRECISION,
        lon                 DOUBLE PRECISION,
        city                VARCHAR(60),
        source_tower_id     VARCHAR(40),
        movement_speed_kmh  DOUBLE PRECISION,
        direction_degrees   INT,
        user_density        VARCHAR(20),
        time_of_day         VARCHAR(20),
        day_of_week         VARCHAR(20),
        location_type       VARCHAR(40),
        source              VARCHAR(90) DEFAULT 'ElectricSheep Africa synthetic mobility traces',
        imported_at         TIMESTAMP DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_mobility_traces_time   ON mobility_traces(trace_time DESC);
      CREATE INDEX IF NOT EXISTS idx_mobility_traces_city   ON mobility_traces(city);
      CREATE INDEX IF NOT EXISTS idx_mobility_traces_latlon ON mobility_traces(lat, lon);
      CREATE INDEX IF NOT EXISTS idx_mobility_traces_tower  ON mobility_traces(source_tower_id);

      -- Synthetic 4G/5G penetration data from ElectricSheep Africa / Amon Din.
      CREATE TABLE IF NOT EXISTS network_penetration (
        id                         SERIAL PRIMARY KEY,
        city                       VARCHAR(60) NOT NULL,
        state                      VARCHAR(60) NOT NULL,
        operator                   VARCHAR(20) NOT NULL,
        month                      DATE NOT NULL,
        total_users                INT,
        users_2g                   INT,
        users_3g                   INT,
        users_4g                   INT,
        users_5g                   INT,
        penetration_4g_percent     DOUBLE PRECISION,
        penetration_5g_percent     DOUBLE PRECISION,
        growth_rate_4g_percent     DOUBLE PRECISION,
        growth_rate_5g_percent     DOUBLE PRECISION,
        source                     VARCHAR(100) DEFAULT 'ElectricSheep Africa synthetic 4G/5G penetration data',
        imported_at                TIMESTAMP DEFAULT NOW(),
        UNIQUE(city, state, operator, month)
      );

      CREATE INDEX IF NOT EXISTS idx_network_penetration_month    ON network_penetration(month DESC);
      CREATE INDEX IF NOT EXISTS idx_network_penetration_city     ON network_penetration(city);
      CREATE INDEX IF NOT EXISTS idx_network_penetration_state    ON network_penetration(state);
      CREATE INDEX IF NOT EXISTS idx_network_penetration_operator ON network_penetration(operator);

      -- Synthetic technician activity logs from ElectricSheep Africa / Amon Din.
      CREATE TABLE IF NOT EXISTS technician_logs (
        id                   SERIAL PRIMARY KEY,
        activity_id          VARCHAR(40) NOT NULL UNIQUE,
        technician_id        VARCHAR(40),
        source_tower_id      VARCHAR(40),
        city                 VARCHAR(60),
        operator             VARCHAR(20),
        activity_type        VARCHAR(60),
        priority             VARCHAR(20),
        status               VARCHAR(30),
        started_at           TIMESTAMP,
        ended_at             TIMESTAMP,
        duration_min         INT,
        issue_resolved       BOOLEAN,
        travel_km            DOUBLE PRECISION,
        materials_used       VARCHAR(120),
        notes                TEXT,
        source               VARCHAR(90) DEFAULT 'ElectricSheep Africa synthetic technician activity logs',
        imported_at          TIMESTAMP DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_technician_logs_started    ON technician_logs(started_at DESC);
      CREATE INDEX IF NOT EXISTS idx_technician_logs_technician ON technician_logs(technician_id);
      CREATE INDEX IF NOT EXISTS idx_technician_logs_tower      ON technician_logs(source_tower_id);
      CREATE INDEX IF NOT EXISTS idx_technician_logs_city       ON technician_logs(city);
      CREATE INDEX IF NOT EXISTS idx_technician_logs_operator   ON technician_logs(operator);
      CREATE INDEX IF NOT EXISTS idx_technician_logs_status     ON technician_logs(status);
    `);
    console.log('✅  Database schema ready');
  } finally {
    client.release();
  }
}

module.exports = { pool, bootstrap };
