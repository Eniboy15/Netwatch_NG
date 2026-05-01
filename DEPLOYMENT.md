# NetWatch NG Deployment + Phone Ingest

## 1) Required environment variables

Set these in your host (Railway/Render):

- `PORT=3001` (or let host set this automatically)
- `DATABASE_URL=postgresql://...` (preferred in cloud)
- `INGEST_API_KEY=<long-random-secret>`

If you are not using `DATABASE_URL`, set:

- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`

## 2) Deploy app

1. Push this project to GitHub.
2. Create a new Railway or Render web service from the repo.
3. Set build command: `npm install`
4. Set start command: `npm start`
5. Add environment variables above.
6. Deploy and verify `GET /api/health`.

Because frontend and API are served by the same Express app, `public/index.html` now auto-uses `window.location.origin + /api` in deployed environments.

## 3) Phone reading endpoint

Use:

- `POST /api/readings/device`
- Header: `x-netwatch-key: <INGEST_API_KEY>`
- Body: one reading object

Example body:

```json
{
  "cell_id": 123456789,
  "city": "Lagos",
  "operator": "MTN",
  "network": "4G",
  "signal_dbm": -83,
  "latency_ms": 42,
  "dl_mbps": 18.7,
  "ul_mbps": 7.1,
  "rsrp": -95,
  "rsrq": -10.5,
  "recorded_at": "2026-04-27T22:40:00.000Z"
}
```

Notes:

- `signal_dbm` is required.
- If `tower_id` is omitted, backend auto-resolves it from `cell_id` when possible.

## 4) Real-data dashboard mode

Open dashboard with `?sim=off` to disable simulated collection and poll live DB readings:

- `https://<your-domain>/?sim=off`

Default (without query param) keeps simulator on.
