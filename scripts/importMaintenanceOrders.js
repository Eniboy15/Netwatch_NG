// scripts/importMaintenanceOrders.js
// Run once: node scripts/importMaintenanceOrders.js  (or: npm run import:maintenance)
//
// Imports ElectricSheep Africa synthetic maintenance work orders.

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { pool, bootstrap } = require('../server/db');

const CSV_PATH = process.env.MAINTENANCE_ORDERS_CSV_PATH
  || path.join(__dirname, '../data/maintenance_work_orders.csv');

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

function toDate(value) {
  if (value === null || value === undefined || value === '') return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d.toISOString().slice(0, 10);
}

function cleanText(value, fallback, maxLen) {
  const raw = value === null || value === undefined ? '' : String(value).trim();
  return (raw || fallback).slice(0, maxLen);
}

function normalizeRow(row, rowNumber) {
  return {
    work_order_id: cleanText(first(row, ['work_order_id', 'order_id', 'maintenance_id', 'id']), `MWO-${rowNumber}`, 40),
    source_tower_id: cleanText(first(row, ['tower_id', 'source_tower_id']), 'Unknown', 40),
    city: cleanText(first(row, ['city']), 'Unknown', 60),
    operator: cleanText(first(row, ['operator', 'provider']), 'Unknown', 20),
    maintenance_type: cleanText(first(row, ['maintenance_type', 'type']), 'Unknown', 40),
    issue_category: cleanText(first(row, ['issue_category', 'issue_type', 'fault_category']), 'Unknown', 60),
    priority: cleanText(first(row, ['priority']), 'Unknown', 20),
    status: cleanText(first(row, ['status', 'work_order_status']), 'Unknown', 30),
    scheduled_date: toDate(first(row, ['scheduled_date', 'schedule_date'])),
    completed_date: toDate(first(row, ['completed_date', 'completion_date', 'closed_date'])),
    technician_id: cleanText(first(row, ['technician_id', 'assigned_technician']), 'Unknown', 40),
    repair_duration_hours: toNumber(first(row, ['repair_duration_hours', 'duration_hours'])),
    parts_replaced: cleanText(first(row, ['parts_replaced', 'parts_used']), 'None', 120),
    cost_ngn: toNumber(first(row, ['cost_ngn', 'repair_cost_ngn', 'maintenance_cost_ngn'])),
    downtime_minutes: toNumber(first(row, ['downtime_minutes', 'downtime_min'])),
  };
}

async function flushBatch(rows) {
  const values = [];
  const params = [];
  let p = 1;

  for (const r of rows) {
    values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    params.push(
      r.work_order_id,
      r.source_tower_id,
      r.city,
      r.operator,
      r.maintenance_type,
      r.issue_category,
      r.priority,
      r.status,
      r.scheduled_date,
      r.completed_date,
      r.technician_id,
      r.repair_duration_hours,
      r.parts_replaced,
      r.cost_ngn,
      r.downtime_minutes
    );
  }

  await pool.query(
    `INSERT INTO maintenance_orders
       (work_order_id, source_tower_id, city, operator, maintenance_type,
        issue_category, priority, status, scheduled_date, completed_date,
        technician_id, repair_duration_hours, parts_replaced, cost_ngn,
        downtime_minutes)
     VALUES ${values.join(',')}
     ON CONFLICT (work_order_id) DO NOTHING`,
    params
  );
}

async function importMaintenanceOrders() {
  await bootstrap();

  if (!fs.existsSync(CSV_PATH)) {
    console.error(`CSV not found at: ${CSV_PATH}`);
    console.error('Download it into data/maintenance_work_orders.csv or set MAINTENANCE_ORDERS_CSV_PATH.');
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

    if (!normalized.work_order_id) {
      skipped++;
      continue;
    }

    batch.push(normalized);

    if (batch.length >= BATCH_SIZE) {
      await flushBatch(batch);
      inserted += batch.length;
      process.stdout.write(`\rInserted ${inserted.toLocaleString()} maintenance orders...`);
      batch = [];
    }
  }

  if (batch.length) {
    await flushBatch(batch);
    inserted += batch.length;
  }

  const { rows: summary } = await pool.query(`
    SELECT maintenance_type, status, COUNT(*)::int AS rows
    FROM maintenance_orders
    GROUP BY maintenance_type, status
    ORDER BY maintenance_type, status
  `);

  console.log(`\nImport complete: ${inserted.toLocaleString()} parsed rows sent, ${skipped.toLocaleString()} skipped.`);
  console.log(`CSV rows parsed: ${parsed.toLocaleString()}`);
  console.table(summary);

  await pool.end();
}

importMaintenanceOrders().catch((err) => {
  console.error('Import failed:', err.stack || err.message || err);
  process.exit(1);
});
