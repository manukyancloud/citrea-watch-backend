# Citrea Watch

Backend API for Citrea Watch, focused on TVL tracking, bridge activity, gas analytics, and network health summaries.

## Tech Stack
- Node.js (18+)
- TypeScript
- Express
- ethers
- better-sqlite3
- axios
- pino-http
- helmet
- express-rate-limit

## Local Development
```bash
npm install
cp .env.example .env
npm run dev
```

## Build & Run
```bash
npm run build
npm start
```

## Environment Variables
See .env.example for the full list. Key variables:

- RPC_URL: Citrea RPC endpoint
- BRIDGE_SUBGRAPH_URL: Goldsky subgraph GraphQL endpoint for bridge events
- SUBGRAPH_LAG_BLOCKS: Blocks to treat as “live” (excluded from subgraph due to lag)
- EXPLORER_API_BASE: Explorer API base URL
- ALLOWED_ORIGINS: Comma-separated allowlist for CORS
- RATE_LIMIT_WINDOW_MS / RATE_LIMIT_MAX: API rate limiting
- CACHE_CONTROL: Response cache headers
- SENTRY_DSN: Optional error tracking

## Endpoints
- GET /health
- GET /api/tvl/global
- GET /api/bridge/summary
- GET /api/bridge/timeseries
- GET /api/gas/timeseries
- GET /api/gas/heatmap
- GET /api/explorer/summary

## Notes
- The backend maintains rolling caches and history snapshots for TVL and gas analytics.
- SQLite data files are stored locally under data/.
