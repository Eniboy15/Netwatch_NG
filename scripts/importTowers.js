// scripts/importTowers.js
// Run once:  node scripts/importTowers.js  (or: npm run import)
//
// Reads the OpenCelliD 621.csv file and bulk-inserts all Nigerian
// towers into the towers table, resolving operator names and city.

require('dotenv').config();
const fs      = require('fs');
const path    = require('path');
const { parse } = require('csv-parse');
const { pool, bootstrap } = require('../server/db');

// ─── Config ───────────────────────────────────────────────────────
const CSV_PATH = process.env.CSV_PATH
  || path.join(__dirname, '../data/621.csv');

const BATCH_SIZE = 100;   // rows inserted per DB transaction

// ─── Operator lookup (MNC → name) ─────────────────────────────────
const MNC_MAP = {
  30: 'MTN',
  20: 'Airtel',
  50: 'Glo',
  60: '9mobile',
};

// ─── Radio → network generation ───────────────────────────────────
const RADIO_MAP = {
  GSM:  '2G',
  UMTS: '3G',
  LTE:  '4G',
  NR:   '5G',
};

// ─── Nigerian cities with bounding boxes ─────────────────────────
// Format: { name, lat, lon }  — used for nearest-city resolution
const CITIES = [
  { name: 'Lagos',          lat:  6.5244, lon:  3.3792 },
  { name: 'Abuja',          lat:  9.0765, lon:  7.3986 },
  { name: 'Kano',           lat: 12.0022, lon:  8.5920 },
  { name: 'Port Harcourt',  lat:  4.8156, lon:  7.0498 },
  { name: 'Ibadan',         lat:  7.3775, lon:  3.9470 },
  { name: 'Kaduna',         lat: 10.5222, lon:  7.4383 },
  { name: 'Enugu',          lat:  6.4584, lon:  7.5464 },
  { name: 'Benin City',     lat:  6.3350, lon:  5.6037 },
  { name: 'Maiduguri',      lat: 11.8333, lon: 13.1500 },
  { name: 'Jos',            lat:  9.8965, lon:  8.8583 },
  { name: 'Warri',          lat:  5.5167, lon:  5.7500 },
  { name: 'Aba',            lat:  5.1066, lon:  7.3667 },
  { name: 'Ilorin',         lat:  8.4966, lon:  4.5420 },
  { name: 'Uyo',            lat:  5.0481, lon:  7.9237 },
  { name: 'Zaria',          lat: 11.0667, lon:  7.7000 },
  { name: 'Sokoto',         lat: 13.0622, lon:  5.2339 },
  { name: 'Onitsha',        lat:  6.1667, lon:  6.7833 },
  { name: 'Calabar',        lat:  4.9500, lon:  8.3250 },
  { name: 'Akure',          lat:  7.2500, lon:  5.1950 },
  { name: 'Abeokuta',       lat:  7.1558, lon:  3.3450 },
];

function nearestCity(lat, lon) {
  let best = CITIES[0], bestDist = Infinity;
  for (const c of CITIES) {
    const dlat = c.lat - lat, dlon = c.lon - lon;
    const d = dlat * dlat + dlon * dlon;   // no need for full haversine for nearest-lookup
    if (d < bestDist) { bestDist = d; best = c; }
  }
  return best.name;
}

// ─── Main import ──────────────────────────────────────────────────
async function importTowers() {
  await bootstrap();

  // Check if already imported
  const { rows: check } = await pool.query('SELECT COUNT(*) FROM towers');
  if (parseInt(check[0].count) > 0) {
    console.log(`ℹ️  towers table already has ${check[0].count} rows.`);
    const answer = process.argv[2];
    if (answer !== '--force') {
      console.log('   Pass --force to re-import. Skipping.');
      process.exit(0);
    }
    console.log('   --force passed, truncating and re-importing...');
    await pool.query('TRUNCATE towers CASCADE');
  }

  if (!fs.existsSync(CSV_PATH)) {
    console.error(`❌  CSV not found at: ${CSV_PATH}`);
    console.error('    Copy 621.csv into the data/ folder or set CSV_PATH in .env');
    process.exit(1);
  }

  console.log(`📂  Reading ${CSV_PATH} …`);

  let batch    = [];
  let total    = 0;
  let skipped  = 0;

  const parser = fs
    .createReadStream(CSV_PATH)
    .pipe(parse({ relax_column_count: true }));

  for await (const row of parser) {
    // OpenCelliD column order (no header):
    // 0:radio 1:mcc 2:mnc 3:lac 4:cell_id 5:unit 6:lon 7:lat
    // 8:range 9:samples 10:changeable 11:created_at 12:updated_at 13:average_signal
    const [radio, mcc, mnc, lac, cell_id, , lon, lat,
           range_m, samples, , created_at, updated_at, average_signal] = row;

    const latF = parseFloat(lat);
    const lonF = parseFloat(lon);

    // Skip rows with invalid coordinates
    if (isNaN(latF) || isNaN(lonF) || latF === 0 || lonF === 0) {
      skipped++; continue;
    }

    batch.push({
      cell_id:        parseInt(cell_id),
      radio:          radio || 'GSM',
      mcc:            parseInt(mcc),
      mnc:            parseInt(mnc),
      lac:            parseInt(lac),
      lon:            lonF,
      lat:            latF,
      range_m:        parseInt(range_m) || 0,
      samples:        parseInt(samples) || 1,
      average_signal: parseInt(average_signal) || 0,
      created_at:     parseInt(created_at) || 0,
      updated_at:     parseInt(updated_at) || 0,
      operator:       MNC_MAP[parseInt(mnc)] || 'Unknown',
      network:        RADIO_MAP[radio]       || '2G',
      city:           nearestCity(latF, lonF),
    });

    // Flush batch
    if (batch.length >= BATCH_SIZE) {
      await flushBatch(batch);
      total += batch.length;
      process.stdout.write(`\r   Inserted ${total.toLocaleString()} towers…`);
      batch = [];
    }
  }

  // Final partial batch
  if (batch.length > 0) {
    await flushBatch(batch);
    total += batch.length;
  }

  console.log(`\n✅  Import complete: ${total.toLocaleString()} towers inserted, ${skipped} skipped.`);

  // Summary
  const { rows } = await pool.query(`
    SELECT operator, network, COUNT(*) AS count
    FROM towers
    GROUP BY operator, network
    ORDER BY operator, network
  `);
  console.log('\n📊  Tower breakdown:');
  console.table(rows.map(r => ({ operator: r.operator, network: r.network, towers: parseInt(r.count) })));

  await pool.end();
}

async function flushBatch(rows) {
  // Build parameterised multi-row INSERT
  const values = [];
  const params = [];
  let p = 1;

  for (const r of rows) {
    values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    params.push(
      r.cell_id, r.radio, r.mcc, r.mnc, r.lac,
      r.lon, r.lat, r.range_m, r.samples, r.average_signal,
      r.created_at, r.updated_at, r.operator, r.network, r.city
    );
  }

  await pool.query(
    `INSERT INTO towers
       (cell_id,radio,mcc,mnc,lac,lon,lat,range_m,samples,average_signal,
        created_at,updated_at,operator,network,city)
     VALUES ${values.join(',')}
     ON CONFLICT (cell_id, lac, mnc) DO NOTHING`,
    params
  );
}

importTowers().catch(err => {
  console.error('❌  Import failed:', err.message);
  process.exit(1);
});
