import { z } from "zod";

const numberFromEnv = (defaultValue: number) =>
	z.preprocess((value: unknown) => {
		if (typeof value === "string" && value.trim() !== "") {
			const parsed = Number(value);
			return Number.isFinite(parsed) ? parsed : value;
		}
		return value ?? defaultValue;
	}, z.number().default(defaultValue));

const envSchema = z.object({
	NODE_ENV: z
		.enum(["development", "test", "production"]) 
		.default("development"),
	PORT: numberFromEnv(3000),
	RPC_URL: z.string().url().default("https://rpc.mainnet.citrea.xyz"),
	COINGECKO_API_KEY: z.string().optional().default(""),
	EXPLORER_API_BASE: z
		.string()
		.url()
		.default("https://explorer.mainnet.citrea.xyz/api"),
	ALLOWED_ORIGINS: z.string().optional().default(""),
	RATE_LIMIT_WINDOW_MS: numberFromEnv(60_000),
	RATE_LIMIT_MAX: numberFromEnv(120),
	CACHE_CONTROL: z
		.string()
		.optional()
		.default("public, max-age=30, stale-while-revalidate=120"),
	SENTRY_DSN: z.string().optional().default(""),
	BRIDGE_TS_REFRESH_MS: numberFromEnv(10 * 60 * 1000),
	GAS_TS_REFRESH_MS: numberFromEnv(10 * 60 * 1000),
	SERIES_WINDOW_DAYS: numberFromEnv(7),
	TVL_REFRESH_MS: numberFromEnv(10 * 60 * 1000),
	TVL_HISTORY_DAYS: numberFromEnv(8),
	BRIDGE_LAUNCH_BLOCK: numberFromEnv(0),
	BRIDGE_FINALITY_LAG: numberFromEnv(10),
	BRIDGE_LOG_CHUNK_SIZE: numberFromEnv(1000),
	BRIDGE_SEED_BLOCK: numberFromEnv(0),
	BRIDGE_SEED_SUPPLY_WEI: z.string().optional().default(""),
	BRIDGE_SEED_DEPOSIT_COUNT: numberFromEnv(0),
	BRIDGE_SEED_FAILED_COUNT: numberFromEnv(0),
	BRIDGE_SEED_WITHDRAWAL_COUNT: numberFromEnv(0),
	SUPPLY_STATE_PATH: z.string().optional().default(""),
});

export const env = envSchema.parse({
	NODE_ENV: process.env.NODE_ENV,
	PORT: process.env.PORT,
	RPC_URL: process.env.RPC_URL,
	COINGECKO_API_KEY: process.env.COINGECKO_API_KEY,
	EXPLORER_API_BASE: process.env.EXPLORER_API_BASE,
	ALLOWED_ORIGINS: process.env.ALLOWED_ORIGINS,
	RATE_LIMIT_WINDOW_MS: process.env.RATE_LIMIT_WINDOW_MS,
	RATE_LIMIT_MAX: process.env.RATE_LIMIT_MAX,
	CACHE_CONTROL: process.env.CACHE_CONTROL,
	SENTRY_DSN: process.env.SENTRY_DSN,
	BRIDGE_TS_REFRESH_MS: process.env.BRIDGE_TS_REFRESH_MS,
	GAS_TS_REFRESH_MS: process.env.GAS_TS_REFRESH_MS,
	SERIES_WINDOW_DAYS: process.env.SERIES_WINDOW_DAYS,
	TVL_REFRESH_MS: process.env.TVL_REFRESH_MS,
	TVL_HISTORY_DAYS: process.env.TVL_HISTORY_DAYS,
	BRIDGE_LAUNCH_BLOCK: process.env.BRIDGE_LAUNCH_BLOCK,
	BRIDGE_FINALITY_LAG: process.env.BRIDGE_FINALITY_LAG,
	BRIDGE_LOG_CHUNK_SIZE: process.env.BRIDGE_LOG_CHUNK_SIZE,
	BRIDGE_SEED_BLOCK: process.env.BRIDGE_SEED_BLOCK,
	BRIDGE_SEED_SUPPLY_WEI: process.env.BRIDGE_SEED_SUPPLY_WEI,
	BRIDGE_SEED_DEPOSIT_COUNT: process.env.BRIDGE_SEED_DEPOSIT_COUNT,
	BRIDGE_SEED_FAILED_COUNT: process.env.BRIDGE_SEED_FAILED_COUNT,
	BRIDGE_SEED_WITHDRAWAL_COUNT: process.env.BRIDGE_SEED_WITHDRAWAL_COUNT,
	SUPPLY_STATE_PATH: process.env.SUPPLY_STATE_PATH,
});
