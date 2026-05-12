# Enhance Outages & Predictions UI

## Goal Description
Improve reliability and user experience of the NetWatch NG dashboard by adding robust error handling, loading spinners, and populating data for the Outages and Predictions tabs. This addresses the current concerns about outage monitoring and the prediction model.

## User Review Required
> [!IMPORTANT] Review the proposed UI changes and confirm if any additional visual tweaks or data fields are needed.

## Open Questions
> [!WARNING] Should the predictions panel display a risk score histogram or just numeric values? Clarify desired visualization.

## Proposed Changes

---
### Frontend (public/index.html & scripts)
- Add a generic `showSpinner(targetId)` / `hideSpinner(targetId)` utility.
- Wrap API calls (`fetch(`${API}/uptime`)`, `refreshPredictions()`) with try/catch and invoke spinners.
- Update `refreshPredictions()` to handle fetch errors and display an error toast.
- Populate the outage table (`#out-tb`) with data from `/api/uptime` response (use existing `outage_logs` array).
- Replace placeholder values (`-`) in prediction cards (`#pred-top`, `#pred-avg`, etc.) with actual data from `/api/predictions` (if endpoint exists) or mock data.
- Update the API status badge to show a loading state while checking health.
- Minor CSS tweaks for spinner overlay and consistent font sizes.

---
### Server (server/routes.js)
- Ensure `/api/uptime` returns `uptime_logs` field (currently `uptime_logs` is not present). Add alias or rename to match frontend expectations.
- Add a new endpoint `/api/predictions` (if not existing) that returns a JSON with keys: `topRisk`, `avgRisk`, `highRiskProviders`, `confidence`.
- Implement proper error responses (status 500) with JSON `{ error: "..." }`.

---
### Verification Plan
- Manual testing: open the site, switch to Outages and Predictions tabs, ensure data loads, spinners appear, and errors are shown when API is down (simulate by stopping the server).
- Automated: run `npm run dev`, open `http://localhost:3000`, check console for no errors.

---
### Future Enhancements (optional)
- Add real-time WebSocket updates for outages.
- Visual heatmap for risk scores.
