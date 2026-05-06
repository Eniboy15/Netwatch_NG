// scripts/importHardwareSensors.js
// Run once: node scripts/importHardwareSensors.js  (or: npm run import:hardware)
//
// Imports ElectricSheep Africa synthetic hardware sensor data.

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { pool, bootstrap } = require('../server/db');

const CSV_PATH = process.env.HARDWARE_SENSORS_CSV_PATH
  || path.join(__dirname, '../data/hardware_sensor_data.csv');

const BATCH_SIZE = 100;

function first(row, names) {
  for (const name of names) {
    if (row[name] !== undefined && row[name] !== '') return row[name];
  }
  return null;
}

function toNumber(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function toBoolean(value) {
  if (value === null || value === undefined || value === '') return null;
  const raw = String(value).trim().toLowerCase();
  if (['true', 'yes', 'y', '1'].includes(raw)) return true;
  if (['false', 'no', 'n', '0'].includes(raw)) return false;
  return null;
}

function cleanText(value, fallback, maxLen) {
  const raw = value === null || value === undefined ? '' : String(value).trim();
  return (raw || fallback).slice(0, maxLen);
}

function normalizeRow(row, rowNumber) {
  const sensorTime = new Date(first(row, ['timestamp', 'sensor_time', 'recorded_at']));
  if (Number.isNaN(sensorTime.getTime())) return null;

  return {
    sensor_id: cleanText(first(row, ['sensor_id', 'reading_id', 'id']), `HWS-${rowNumber}`, 40),
    sensor_time: sensorTime.toISOString(),
    source_tower_id: cleanText(first(row, ['tower_id', 'source_tower_id']), 'Unknown', 40),
    city: cleanText(first(row, ['city']), 'Unknown', 60),
    equipment_type: cleanText(first(row, ['equipment_type', 'equipment']), 'Unknown', 40),
    temperature_celsius: toNumber(first(row, ['temperature_celsius', 'temperature_c', 'temperature'])),
    power_draw_watts: toNumber(first(row, ['power_draw_watts', 'power_watts', 'power_draw'])),
    voltage_v: toNumber(first(row, ['voltage_v', 'voltage'])),
    humidity_percent: toNumber(first(row, ['humidity_percent', 'humidity'])),
    vibration_level: toNumber(first(row, ['vibration_level', 'vibration'])),
    health_status: cleanText(first(row, ['health_status', 'status']), 'Unknown', 20),
    alert_triggered: toBoolean(first(row, ['alert_triggered', 'alert_flag', 'alert'])),
  };
}

async function flushBatch(rows) {
  const values = [];
  const params = [];
  let p = 1;

  for (const r of rows) {
    values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    params.push(
      r.sensor_id,
      r.sensor_time,
      r.source_tower_id,
      r.city,
      r.equipment_type,
      r.temperature_celsius,
      r.power_draw_watts,
      r.voltage_v,
      r.humidity_percent,
      r.vibration_level,
      r.health_status,
      r.alert_triggered
    );
  }

  await pool.query(
    `INSERT INTO hardware_sensors
       (sensor_id, sensor_time, source_tower_id, city, equipment_type,
        temperature_celsius, power_draw_watts, voltage_v, humidity_percent,
        vibration_level, health_status, alert_triggered)
     VALUES ${values.join(',')}
     ON CONFLICT (sensor_id) DO NOTHING`,
    params
  );
}

async function importHardwareSensors() {
  await bootstrap();

  if (!fs.existsSync(CSV_PATH)) {
    console.error(`CSV not found at: ${CSV_PATH}`);
    console.error('Download it into data/hardware_sensor_data.csv or set HARDWARE_SENSORS_CSV_PATH.');
    process.exit(1);
  }

  console.log(`Reading ${CSV_PATH} ...`);

  let batch = [];
  let parsed = 0;
  let inserted = 0;
  let skipped = 0;

  const parser = fs
    .createReadStream(CSV_PATH)
    .pipe(parse({
      columns: true,
      skip_empty_lines: true,
      trim: true,
    }));

  for await (const row of parser) {
    parsed++;
    const normalized = normalizeRow(row, parsed);

    if (!normalized) {
      skipped++;
      continue;
    }

    batch.push(normalized);

    if (batch.length >= BATCH_SIZE) {
      await flushBatch(batch);
      inserted += batch.length;
      process.stdout.write(`\rInserted ${inserted.toLocaleString()} hardware sensor rows...`);
      batch = [];
    }
  }

  if (batch.length) {
    await flushBatch(batch);
    inserted += batch.length;
  }

  const { rows: summary } = await pool.query(`
    SELECT equipment_type, health_status, COUNT(*)::int AS rows
    FROM hardware_sensors
    GROUP BY equipment_type, health_status
    ORDER BY equipment_type, health_status
  `);

  console.log(`\nImport complete: ${inserted.toLocaleString()} parsed rows sent, ${skipped.toLocaleString()} skipped.`);
  console.log(`CSV rows parsed: ${parsed.toLocaleString()}`);
  console.table(summary);

  await pool.end();
}

importHardwareSensors().catch((err) => {
  console.error('Import failed:', err.stack || err.message || err);
  process.exit(1);
});
