import "dotenv/config";

import express, { type Request, type Response, type RequestHandler } from "express";
import cors from "cors";
import helmet from "helmet";
import rateLimit from "express-rate-limit";
import pinoHttp from "pino-http";
import * as Sentry from "@sentry/node";

import {
	getGlobalTvlSnapshot,
	getBridgeSummary,
	getBridgeTimeseries,
	getGasHeatmap,
	getGasTimeseries,
	getHistoricalData,
	getTvlChanges,
	startAggregationJobs,
} from "./services/tvlService";
import { getExplorerSummary } from "./services/explorerService";
import { env } from "./config/env";

const app = express();

if (env.SENTRY_DSN) {
	Sentry.init({ dsn: env.SENTRY_DSN, environment: env.NODE_ENV });
}

app.disable("x-powered-by");
app.set("trust proxy", 1);

const allowedOrigins = env.ALLOWED_ORIGINS.split(",")
	.map((origin: string) => origin.trim())
	.filter(Boolean);

const helmetMiddleware = helmet({
	crossOriginResourcePolicy: { policy: "cross-origin" },
	contentSecurityPolicy: false,
}) as RequestHandler;

app.use(helmetMiddleware);
app.use(
	cors({
		origin: (origin: string | undefined, callback: (err: Error | null, allow?: boolean) => void) => {
			if (!origin || allowedOrigins.length === 0) {
				return callback(null, true);
			}
			if (allowedOrigins.includes(origin)) {
				return callback(null, true);
			}
			return callback(new Error("Not allowed by CORS"));
		},
		methods: ["GET", "OPTIONS"],
		allowedHeaders: ["Content-Type", "Authorization"],
		maxAge: 600,
	})
);

app.use(
	pinoHttp({
		customLogLevel: (_req: Request, res: Response) => {
			if (res.statusCode >= 500) return "error";
			if (res.statusCode >= 400) return "warn";
			return "info";
		},
	})
);

app.use(
	"/api",
	rateLimit({
		windowMs: env.RATE_LIMIT_WINDOW_MS,
		max: env.RATE_LIMIT_MAX,
		standardHeaders: true,
		legacyHeaders: false,
	})
);

app.use("/api", (_req, res, next) => {
	res.setHeader("Cache-Control", env.CACHE_CONTROL);
	next();
});

app.get("/health", (_req, res) => {
	res.json({ status: "ok", uptime: process.uptime(), timestamp: Date.now() });
});

app.get("/api/tvl/global", async (_req, res) => {
	try {
		const data = await getGlobalTvlSnapshot();
		const changes = getTvlChanges(data.totalTvlUsd);
		res.json({
			...data,
			...changes,
		});
	} catch (error) {
		console.error("Failed to calculate global TVL:", error);
		res.status(500).json({
			totalTvlUsd: 0,
			tokens: [],
			generatedAt: new Date().toISOString(),
			change24hUsd: null,
			change24hPct: null,
			change7dUsd: null,
			change7dPct: null,
		});
	}
});

app.get("/api/tvl/history", async (req, res) => {
	try {
		const hoursParam = req.query.hours;
		const hours = typeof hoursParam === "string" ? Number(hoursParam) : 24;
		const points = await getHistoricalData(Number.isFinite(hours) ? hours : 24);
		res.json({
			points,
			generatedAt: new Date().toISOString(),
		});
	} catch (error) {
		console.error("Failed to fetch TVL history:", error);
		res.status(500).json({
			points: [],
			generatedAt: new Date().toISOString(),
		});
	}
});

app.get("/api/bridge/summary", async (_req, res) => {
	try {
		const data = await getBridgeSummary();
		res.json(data);
	} catch (error) {
		console.error("Failed to calculate bridge summary:", error);
		res.status(500).json({
			depositAmountCbtc: 0,
			depositCount: 0,
			failedDepositCount: 0,
			withdrawalCount: 0,
			totalDepositedCbtc: 0,
			totalWithdrawnCbtc: 0,
			currentSupplyCbtc: 0,
			vaults: [],
			generatedAt: new Date().toISOString(),
		});
	}
});

app.get("/api/bridge/timeseries", async (_req, res) => {
	try {
		const data = await getBridgeTimeseries();
		res.json(data);
	} catch (error) {
		console.error("Failed to calculate bridge timeseries:", error);
		res.status(500).json({
			points: [],
			generatedAt: new Date().toISOString(),
		});
	}
});

app.get("/api/gas/timeseries", async (_req, res) => {
	try {
		const data = await getGasTimeseries();
		res.json(data);
	} catch (error) {
		console.error("Failed to calculate gas timeseries:", error);
		res.status(500).json({
			points: [],
			generatedAt: new Date().toISOString(),
		});
	}
});

app.get("/api/gas/heatmap", async (_req, res) => {
	try {
		const data = await getGasHeatmap();
		res.json(data);
	} catch (error) {
		console.error("Failed to calculate gas heatmap:", error);
		res.status(500).json({
			rows: [],
			generatedAt: new Date().toISOString(),
		});
	}
});

app.get("/api/explorer/summary", async (_req, res) => {
	try {
		const data = await getExplorerSummary();
		res.json(data);
	} catch (error) {
		console.error("Failed to fetch explorer summary:", error);
		res.status(500).json({
			totalTransactions: null,
			totalAddresses: null,
			totalBlocks: null,
			txCount24h: null,
			txCount7d: null,
			generatedAt: new Date().toISOString(),
			source: null,
		});
	}
});

app.use((err: Error, req: express.Request, res: express.Response, _next: express.NextFunction) => {
	if (env.SENTRY_DSN) {
		Sentry.captureException(err);
	}
	if (req.log) {
		req.log.error({ err }, "Unhandled error");
	} else {
		console.error("Unhandled error", err);
	}
	res.status(500).json({ error: "Internal server error" });
});

const server = app.listen(env.PORT, () => {
	console.log(`Citrea TVL server running on port ${env.PORT}`);
	const stopAggregation = startAggregationJobs();
	getGlobalTvlSnapshot()
		.then(() => {
			console.log("Initial TVL calculation completed");
		})
		.catch((error) => {
			console.warn("Initial TVL calculation failed", error);
		});

	const shutdown = (signal: string) => {
		console.log(`Received ${signal}, shutting down...`);
		stopAggregation();
		server.close(() => {
			console.log("Server closed");
			process.exit(0);
		});
	};

	process.on("SIGINT", () => shutdown("SIGINT"));
	process.on("SIGTERM", () => shutdown("SIGTERM"));
});
