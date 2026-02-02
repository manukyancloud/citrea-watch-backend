import { JsonRpcProvider, Contract, Interface, formatUnits } from "ethers";
import Database from "better-sqlite3";
import { mkdir, readFile, writeFile } from "fs/promises";
import { mkdirSync } from "fs";
import { dirname, resolve } from "path";

import { TRACKED_TOKENS, TrackedToken } from "../config/tokens";
import { env } from "../config/env";
import { getTokenPriceUsd } from "./priceService";

const provider = new JsonRpcProvider(env.RPC_URL);

const ERC20_ABI = [
	"function totalSupply() view returns (uint256)",
	"function decimals() view returns (uint8)",
];

const BRIDGE_ADDRESS = "0x3100000000000000000000000000000000000002";
const WCBTC_ADDRESS = "0x3100000000000000000000000000000000000006";
const BASE_FEE_VAULT = "0x3100000000000000000000000000000000000003";
const L1_FEE_VAULT = "0x3100000000000000000000000000000000000004";
const PRIORITY_FEE_VAULT = "0x3100000000000000000000000000000000000005";
const FAILED_DEPOSIT_VAULT = "0x3100000000000000000000000000000000000007";
const BRIDGE_ABI = [
	"function depositAmount() view returns (uint256)",
	"event Deposit(bytes32 wtxId, bytes32 txId, address recipient, uint256 timestamp, uint256 depositId)",
	"event DepositTransferFailed(bytes32 wtxId, bytes32 txId, address recipient, uint256 timestamp, uint256 depositId)",
	"event Withdrawal((bytes32 txId, bytes4 outputId) utxo, uint256 index, uint256 timestamp)",
];

const BRIDGE_LAUNCH_BLOCK = env.BRIDGE_LAUNCH_BLOCK;
const BRIDGE_FINALITY_LAG = env.BRIDGE_FINALITY_LAG;
const BRIDGE_LOG_CHUNK_SIZE = env.BRIDGE_LOG_CHUNK_SIZE;
const BRIDGE_SEED_BLOCK = env.BRIDGE_SEED_BLOCK;
const BRIDGE_SEED_SUPPLY_WEI = env.BRIDGE_SEED_SUPPLY_WEI;
const BRIDGE_SEED_DEPOSIT_COUNT = env.BRIDGE_SEED_DEPOSIT_COUNT;
const BRIDGE_SEED_FAILED_COUNT = env.BRIDGE_SEED_FAILED_COUNT;
const BRIDGE_SEED_WITHDRAWAL_COUNT = env.BRIDGE_SEED_WITHDRAWAL_COUNT;
const SUPPLY_STATE_FILE = env.SUPPLY_STATE_PATH
	? env.SUPPLY_STATE_PATH
	: resolve(process.cwd(), "data", "supply-state.json");
const SUPPLY_STATE_VERSION = 3;
const CATCH_UP_PAUSE_EVERY = 5;
const CATCH_UP_PAUSE_MS = 500;
const LOG_REQUEST_DELAY_MS = 150;
const LOG_RETRY_MAX = 4;
const LOG_RETRY_BASE_MS = 400;
const CONCURRENCY_LIMIT = 5;
const MAX_LOG_CHUNK_SIZE = 1_000;
const HISTORY_DB_FILE = resolve(process.cwd(), "data", "history.db");
const HISTORY_SCHEMA_VERSION = 1;
const SNAPSHOT_BLOCK_INTERVAL = 10_000;
const BRIDGE_SUBGRAPH_URL = env.BRIDGE_SUBGRAPH_URL;
const SUBGRAPH_LAG_BLOCKS = env.SUBGRAPH_LAG_BLOCKS;
const SUBGRAPH_PAGE_SIZE = 1_000;
const SUBGRAPH_SCHEMA_TTL_MS = 5 * 60 * 1000;

interface BridgeVaultBalance {
	name: string;
	address: string;
	balanceCbtc: number;
}

export interface BridgeSummary {
	depositAmountCbtc: number;
	depositCount: number;
	failedDepositCount: number;
	withdrawalCount: number;
	totalDepositedCbtc: number;
	totalWithdrawnCbtc: number;
	currentSupplyCbtc: number;
	vaults: BridgeVaultBalance[];
	generatedAt: string;
}

export interface BridgeTimeseriesPoint {
	date: string;
	inflowCbtc: number;
	outflowCbtc: number;
}

export interface BridgeTimeseriesResult {
	points: BridgeTimeseriesPoint[];
	generatedAt: string;
}

export interface GasTimeseriesPoint {
	time: string;
	baseFee: number;
	l1Fee: number;
	priorityFee: number;
}

export interface GasTimeseriesResult {
	points: GasTimeseriesPoint[];
	generatedAt: string;
}

export interface GasHeatmapCell {
	hour: number;
	value: number;
}

export interface GasHeatmapRow {
	day: string;
	hours: GasHeatmapCell[];
}

export interface GasHeatmapResult {
	rows: GasHeatmapRow[];
	generatedAt: string;
}

const BRIDGE_CACHE_TTL_MS = 5 * 60 * 1000;
let bridgeCache: { data: BridgeSummary; updatedAt: number } | null = null;
const BRIDGE_TS_REFRESH_INTERVAL_MS = env.BRIDGE_TS_REFRESH_MS;
const GAS_REFRESH_INTERVAL_MS = env.GAS_TS_REFRESH_MS;
const SERIES_WINDOW_DAYS = env.SERIES_WINDOW_DAYS;
const TVL_REFRESH_INTERVAL_MS = env.TVL_REFRESH_MS;
const TVL_HISTORY_DAYS = env.TVL_HISTORY_DAYS;

type BridgeBucket = Record<string, { inflow: number; outflow: number }>;
let bridgeTsState: {
	lastScannedBlock: number;
	lastTimestamp: number;
	bucket: BridgeBucket;
	depositAmountCbtc: number;
	updatedAt: number;
} | null = null;
let bridgeTsRefreshInFlight = false;

let gasSeriesState: {
	points: { timestamp: number; baseFeeGwei: number }[];
	lastTimestamp: number;
	updatedAt: number;
} | null = null;
let gasRefreshInFlight = false;

interface SupplyState {
	version: number;
	lastScannedBlock: number;
	cbtcSupplyWei: string;
	lastUpdated: number;
	depositAmountWei?: string;
	depositCount?: number;
	failedDepositCount?: number;
	withdrawalCount?: number;
	vaultBalancesWei?: {
		baseFeeVault: string;
		l1FeeVault: string;
		priorityFeeVault: string;
		failedDepositVault: string;
	};
}

let supplyState: SupplyState | null = null;
let supplyRefreshInFlight = false;

let tvlHistory: { timestamp: number; totalTvlUsd: number }[] = [];
let tvlRefreshInFlight = false;
let globalTvlCache: { data: GlobalTvlResult; updatedAt: number } | null = null;
let globalTvlInFlight = false;
let historyDb: Database.Database | null = null;
let historyBackfillInFlight = false;

async function countBridgeLogs(topic: string): Promise<number> {
	const latestBlock = await provider.getBlockNumber();
	const step = 1_000;
	let count = 0;
	let scanned = 0;
	const totalRanges = Math.ceil((latestBlock + 1) / step);
	const logEvery = 100; // ~100k blocks per log

	for (let fromBlock = 0; fromBlock <= latestBlock; fromBlock += step) {
		const toBlock = Math.min(fromBlock + step - 1, latestBlock);
		const logs = await getLogsWithRetry({
			address: BRIDGE_ADDRESS,
			fromBlock,
			toBlock,
			topics: [topic],
		});
		count += logs.length;
		scanned += 1;
		if (scanned % logEvery === 0 || toBlock === latestBlock) {
			console.log("[cBTC] scan progress", {
				ranges: `${scanned}/${totalRanges}`,
				toBlock,
				count,
			});
		}
	}

	return count;
}

const sleep = (ms: number) =>
	new Promise((resolve) => {
		setTimeout(resolve, ms);
	});

const isRateLimitError = (error: unknown) => {
	const message = error instanceof Error ? error.message : String(error);
	return /rate limit|too many requests|429|timeout/i.test(message);
};

type SubgraphBridgeEvent = {
	id: string;
	type: "deposit" | "failed" | "withdrawal";
	blockNumber: number;
	timestamp: number;
};

type SubgraphCollections = {
	deposits: string;
	failed: string | null;
	withdrawals: string;
};

let subgraphCollectionsCache:
	| { data: SubgraphCollections; fetchedAt: number }
	| null = null;

const parseSubgraphNumber = (value: unknown): number => {
	if (typeof value === "number" && Number.isFinite(value)) {
		return value;
	}
	if (typeof value === "string" && value.trim() !== "") {
		const parsed = Number(value);
		return Number.isFinite(parsed) ? parsed : 0;
	}
	return 0;
};

const selectSubgraphTimestamp = (row: Record<string, unknown>): number => {
	const param = parseSubgraphNumber(row.timestampParam);
	if (param > 0) {
		return param;
	}
	return parseSubgraphNumber(row.timestamp_);
};

const resolveSubgraphCollections = async (): Promise<SubgraphCollections> => {
	const fallback: SubgraphCollections = {
		deposits: "deposits",
		failed: "depositTransferFaileds",
		withdrawals: "withdrawals",
	};
	if (!BRIDGE_SUBGRAPH_URL) {
		return fallback;
	}
	const now = Date.now();
	if (
		subgraphCollectionsCache &&
		now - subgraphCollectionsCache.fetchedAt < SUBGRAPH_SCHEMA_TTL_MS
	) {
		return subgraphCollectionsCache.data;
	}
	try {
		const response = await fetch(BRIDGE_SUBGRAPH_URL, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				query: "{ __type(name: \"Query\") { fields { name } } }",
			}),
		});
		if (!response.ok) {
			throw new Error(
				`Subgraph schema request failed: ${response.status} ${response.statusText}`
			);
		}
		const payload = (await response.json()) as {
			data?: { __type?: { fields?: Array<{ name?: string }> } };
		};
		const fields = payload.data?.__type?.fields ?? [];
		const names = fields
			.map((field) => field.name)
			.filter((name): name is string => typeof name === "string");
		const findField = (patterns: RegExp[]): string | null =>
			names.find((name) => patterns.some((pattern) => pattern.test(name))) ??
			null;

		const deposits =
			findField([/^deposits$/i, /^deposit$/i]) ?? fallback.deposits;
		const withdrawals =
			findField([/^withdrawals$/i, /^withdrawal$/i]) ?? fallback.withdrawals;
		const failed = findField([
			/deposittransferfaileds?/i,
			/deposit_transfer_faileds?/i,
			/^deposittransferfailed$/i,
		]);
		const resolved: SubgraphCollections = {
			deposits,
			failed,
			withdrawals,
		};
		subgraphCollectionsCache = { data: resolved, fetchedAt: now };
		return resolved;
	} catch (error) {
		console.warn("Failed to resolve subgraph collections", error);
		return fallback;
	}
};

const fetchSubgraphEvents = async (params: {
	entity: "Deposit" | "DepositTransferFailed" | "Withdrawal";
	collection: string;
	fromBlock: number;
	toBlock: number;
}): Promise<SubgraphBridgeEvent[]> => {
	if (!BRIDGE_SUBGRAPH_URL) {
		return [];
	}

	const events: SubgraphBridgeEvent[] = [];
	let cursor = "";
	while (true) {
		const query = `query($first: Int!, $from: BigInt!, $to: BigInt!, $idGt: ID!) {
			${params.collection}(first: $first, orderBy: id, orderDirection: asc, where: { block_number_gte: $from, block_number_lte: $to, id_gt: $idGt }) {
				id
				block_number
				timestamp_
				timestampParam
			}
		}`;
		const response = await fetch(BRIDGE_SUBGRAPH_URL, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				query,
				variables: {
					first: SUBGRAPH_PAGE_SIZE,
					from: params.fromBlock,
					to: params.toBlock,
					idGt: cursor,
				},
			}),
		});
		if (!response.ok) {
			throw new Error(
				`Subgraph request failed: ${response.status} ${response.statusText}`
			);
		}
		const payload = (await response.json()) as {
			data?: Record<string, Array<Record<string, unknown>>>;
			errors?: Array<{ message?: string }>;
		};
		if (payload.errors && payload.errors.length > 0) {
			throw new Error(payload.errors.map((err) => err.message).join("; "));
		}
		const rows = payload.data?.[params.collection] ?? [];
		if (!Array.isArray(rows) || rows.length === 0) {
			break;
		}
		for (const row of rows) {
			const blockNumber = parseSubgraphNumber(row.block_number);
			const timestamp = selectSubgraphTimestamp(row);
			events.push({
				id: String(row.id ?? ""),
				type:
					params.entity === "Deposit"
						? "deposit"
						: params.entity === "DepositTransferFailed"
							? "failed"
							: "withdrawal",
				blockNumber,
				timestamp,
			});
		}
		if (rows.length < SUBGRAPH_PAGE_SIZE) {
			break;
		}
		cursor = String(rows[rows.length - 1]?.id ?? "");
		if (!cursor) {
			break;
		}
	}

	return events;
};

const fetchBridgeEventsFromSubgraph = async (
	fromBlock: number,
	toBlock: number
): Promise<SubgraphBridgeEvent[]> => {
	if (!BRIDGE_SUBGRAPH_URL || toBlock < fromBlock) {
		return [];
	}
	try {
		const collections = await resolveSubgraphCollections();
		const [deposits, failed, withdrawals] = await Promise.all([
			fetchSubgraphEvents({
				entity: "Deposit",
				collection: collections.deposits,
				fromBlock,
				toBlock,
			}),
			collections.failed
				? fetchSubgraphEvents({
					entity: "DepositTransferFailed",
					collection: collections.failed,
					fromBlock,
					toBlock,
				})
				: Promise.resolve([] as SubgraphBridgeEvent[]),
			fetchSubgraphEvents({
				entity: "Withdrawal",
				collection: collections.withdrawals,
				fromBlock,
				toBlock,
			}),
		]);
		return [...deposits, ...failed, ...withdrawals];
	} catch (error) {
		console.warn("Subgraph bridge query failed", error);
		return [];
	}
};

const getLogsWithRetry = async (params: {
	address: string;
	fromBlock: number;
	toBlock: number;
	topics: Array<string | string[] | null>;
}): Promise<Awaited<ReturnType<JsonRpcProvider["getLogs"]>>> => {
	let attempt = 0;
	while (true) {
		try {
			const logs = await provider.getLogs(params);
			if (LOG_REQUEST_DELAY_MS > 0) {
				await sleep(LOG_REQUEST_DELAY_MS);
			}
			return logs;
		} catch (error) {
			attempt += 1;
			if (!isRateLimitError(error) || attempt > LOG_RETRY_MAX) {
				throw error;
			}
			const backoff = LOG_RETRY_BASE_MS * Math.pow(2, attempt - 1);
			await sleep(backoff + Math.floor(Math.random() * 200));
		}
	}
};

const getHistoryDb = (): Database.Database => {
	if (historyDb) {
		return historyDb;
	}
	mkdirSync(dirname(HISTORY_DB_FILE), { recursive: true });
	const db = new Database(HISTORY_DB_FILE);
	db.pragma("journal_mode = WAL");
	db.exec(
		"CREATE TABLE IF NOT EXISTS tvl_snapshots (timestamp INTEGER NOT NULL, blockNumber INTEGER NOT NULL, totalTvlUsd REAL NOT NULL, cbtcSupply REAL NOT NULL, ctUsdSupply REAL NOT NULL, syBtcSupply REAL NOT NULL, PRIMARY KEY (blockNumber));"
	);
	db.exec(
		"CREATE TABLE IF NOT EXISTS tvl_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);"
	);
	const row = db
		.prepare("SELECT value FROM tvl_meta WHERE key = ?")
		.get("schemaVersion") as { value?: string } | undefined;
	const currentVersion = row?.value ? Number(row.value) : null;
	if (currentVersion !== HISTORY_SCHEMA_VERSION) {
		db.exec("DELETE FROM tvl_snapshots;");
		db.prepare("INSERT OR REPLACE INTO tvl_meta (key, value) VALUES (?, ?)")
			.run("schemaVersion", String(HISTORY_SCHEMA_VERSION));
	}
	historyDb = db;
	return db;
};

interface TvlSnapshot {
	timestamp: number;
	blockNumber: number;
	totalTvlUsd: number;
	cbtcSupply: number;
	ctUsdSupply: number;
	syBtcSupply: number;
}

export interface TvlHistoryPoint {
	timestamp: number;
	blockNumber: number;
	totalTvlUsd: number;
	cbtcSupply: number;
}

const buildSnapshot = async (
	blockNumber: number,
	blockTimestampMs: number
): Promise<TvlSnapshot> => {
	const snapshot = await calculateGlobalTvl();
	const cbtc = snapshot.tokens.find((token) => token.symbol === "cBTC")?.supply ?? 0;
	const ctUsd = snapshot.tokens.find((token) => token.symbol === "ctUSD")?.supply ?? 0;
	const syBtc = snapshot.tokens.find((token) => token.symbol === "syBTC")?.supply ?? 0;
	return {
		timestamp: blockTimestampMs,
		blockNumber,
		totalTvlUsd: snapshot.totalTvlUsd,
		cbtcSupply: cbtc,
		ctUsdSupply: ctUsd,
		syBtcSupply: syBtc,
	};
};

const recordSnapshot = async (blockNumber: number): Promise<void> => {
	const db = getHistoryDb();
	const block = await provider.getBlock(blockNumber);
	const blockTimestampMs = block
		? Number(block.timestamp) * 1000
		: Date.now();
	const snapshot = await buildSnapshot(blockNumber, blockTimestampMs);
	const insert = db.prepare(
		"INSERT OR IGNORE INTO tvl_snapshots (timestamp, blockNumber, totalTvlUsd, cbtcSupply, ctUsdSupply, syBtcSupply) VALUES (?, ?, ?, ?, ?, ?)"
	);
	insert.run(
		snapshot.timestamp,
		snapshot.blockNumber,
		snapshot.totalTvlUsd,
		snapshot.cbtcSupply,
		snapshot.ctUsdSupply,
		snapshot.syBtcSupply
	);
};

export const getTvlHistorySnapshots = async (
	hoursBack = 24
): Promise<TvlHistoryPoint[]> => {
	const db = getHistoryDb();
	const safeHours = Math.max(1, hoursBack);
	const cutoff = Date.now() - safeHours * 60 * 60 * 1000;
	const rows = db
		.prepare(
			"SELECT timestamp, blockNumber, totalTvlUsd, cbtcSupply FROM tvl_snapshots WHERE timestamp >= ? ORDER BY timestamp ASC"
		)
		.all(cutoff) as TvlHistoryPoint[];
	return rows;
};

export const getHistoricalData = async (
	hours: number
): Promise<TvlHistoryPoint[]> => getTvlHistorySnapshots(hours);

const loadSupplyState = async (): Promise<SupplyState> => {
	if (supplyState) {
		return supplyState;
	}
	try {
		const raw = await readFile(SUPPLY_STATE_FILE, "utf-8");
		const parsed = JSON.parse(raw) as SupplyState;
		if (
			parsed.version === SUPPLY_STATE_VERSION &&
			typeof parsed.lastScannedBlock === "number" &&
			typeof parsed.cbtcSupplyWei === "string"
		) {
			const nextState: SupplyState = {
				version: SUPPLY_STATE_VERSION,
				lastScannedBlock: parsed.lastScannedBlock,
				cbtcSupplyWei: parsed.cbtcSupplyWei,
				lastUpdated: parsed.lastUpdated ?? 0,
				...(parsed.depositAmountWei ? { depositAmountWei: parsed.depositAmountWei } : {}),
				depositCount: parsed.depositCount ?? 0,
				failedDepositCount: parsed.failedDepositCount ?? 0,
				withdrawalCount: parsed.withdrawalCount ?? 0,
				...(parsed.vaultBalancesWei
					? { vaultBalancesWei: parsed.vaultBalancesWei }
					: {}),
			};
			supplyState = nextState;
			return nextState;
		}
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
			console.warn("Failed to load supply state", error);
		}
	}

	const useSeed = BRIDGE_SEED_BLOCK > 0 && BRIDGE_SEED_SUPPLY_WEI !== "";
	const initialState: SupplyState = {
		version: SUPPLY_STATE_VERSION,
		lastScannedBlock: useSeed ? BRIDGE_SEED_BLOCK : BRIDGE_LAUNCH_BLOCK,
		cbtcSupplyWei: useSeed ? BRIDGE_SEED_SUPPLY_WEI : "0",
		lastUpdated: 0,
		depositCount: useSeed ? BRIDGE_SEED_DEPOSIT_COUNT : 0,
		failedDepositCount: useSeed ? BRIDGE_SEED_FAILED_COUNT : 0,
		withdrawalCount: useSeed ? BRIDGE_SEED_WITHDRAWAL_COUNT : 0,
	};
	supplyState = initialState;
	return initialState;
};

const persistSupplyState = async (state: SupplyState): Promise<void> => {
	await mkdir(dirname(SUPPLY_STATE_FILE), { recursive: true });
	await writeFile(SUPPLY_STATE_FILE, JSON.stringify(state, null, 2));
};

const refreshCbtcSupplyState = async (): Promise<void> => {
	if (supplyRefreshInFlight) {
		return;
	}
	supplyRefreshInFlight = true;
	try {
		const state = await loadSupplyState();
		const latestBlock = await provider.getBlockNumber();
		const targetBlock = Math.max(
			BRIDGE_LAUNCH_BLOCK,
			latestBlock - BRIDGE_FINALITY_LAG
		);

		const [baseFeeVault, l1FeeVault, priorityFeeVault, failedDepositVault] =
			await Promise.all([
				provider.getBalance(BASE_FEE_VAULT),
				provider.getBalance(L1_FEE_VAULT),
				provider.getBalance(PRIORITY_FEE_VAULT),
				provider.getBalance(FAILED_DEPOSIT_VAULT),
			]);

		state.vaultBalancesWei = {
			baseFeeVault: baseFeeVault.toString(),
			l1FeeVault: l1FeeVault.toString(),
			priorityFeeVault: priorityFeeVault.toString(),
			failedDepositVault: failedDepositVault.toString(),
		};

		const bridge = new Contract(BRIDGE_ADDRESS, BRIDGE_ABI, provider) as unknown as {
			depositAmount: () => Promise<bigint>;
		};
		const iface = new Interface(BRIDGE_ABI);
		const depositEvent = iface.getEvent("Deposit");
		const failedDepositEvent = iface.getEvent("DepositTransferFailed");
		const withdrawalEvent = iface.getEvent("Withdrawal");
		if (!depositEvent || !failedDepositEvent || !withdrawalEvent) {
			throw new Error("Bridge ABI events not found");
		}
		const depositTopic = depositEvent.topicHash;
		const failedDepositTopic = failedDepositEvent.topicHash;
		const withdrawalTopic = withdrawalEvent.topicHash;
		const depositAmount = await bridge.depositAmount();
		state.depositAmountWei = depositAmount.toString();

		if (targetBlock <= state.lastScannedBlock) {
			state.lastUpdated = Date.now();
			supplyState = state;
			await persistSupplyState(state);
			return;
		}
		const chunkSize = Math.min(BRIDGE_LOG_CHUNK_SIZE, MAX_LOG_CHUNK_SIZE);
		const initialLag = targetBlock - state.lastScannedBlock;
		const catchUpMode = initialLag > chunkSize;
		let currentWei = BigInt(state.cbtcSupplyWei || "0");
		let totalDepositCount = state.depositCount ?? 0;
		let totalFailedCount = state.failedDepositCount ?? 0;
		let totalWithdrawalCount = state.withdrawalCount ?? 0;
		let chunkCounter = 0;

		const subgraphCutoff = BRIDGE_SUBGRAPH_URL
			? Math.max(BRIDGE_LAUNCH_BLOCK, targetBlock - SUBGRAPH_LAG_BLOCKS)
			: -1;
		if (subgraphCutoff >= state.lastScannedBlock + 1) {
			const subgraphFrom = state.lastScannedBlock + 1;
			const subgraphEvents = await fetchBridgeEventsFromSubgraph(
				subgraphFrom,
				subgraphCutoff
			);
			let depositCount = 0;
			let failedDepositCount = 0;
			let withdrawalCount = 0;
			for (const event of subgraphEvents) {
				if (event.type === "deposit") {
					depositCount += 1;
				} else if (event.type === "failed") {
					failedDepositCount += 1;
				} else if (event.type === "withdrawal") {
					withdrawalCount += 1;
				}
			}
			const deltaCount = depositCount + failedDepositCount - withdrawalCount;
			const deltaWei = BigInt(deltaCount) * depositAmount;
			currentWei = currentWei + deltaWei;
			totalDepositCount += depositCount;
			totalFailedCount += failedDepositCount;
			totalWithdrawalCount += withdrawalCount;
			state.lastScannedBlock = subgraphCutoff;
			state.cbtcSupplyWei = (currentWei > 0n ? currentWei : 0n).toString();
			state.depositCount = totalDepositCount;
			state.failedDepositCount = totalFailedCount;
			state.withdrawalCount = totalWithdrawalCount;
			state.lastUpdated = Date.now();
			supplyState = state;
			await persistSupplyState(state);
			if (catchUpMode) {
				await recordSnapshot(subgraphCutoff);
			}
		}

		for (
			let batchStart = state.lastScannedBlock + 1;
			batchStart <= targetBlock;
			batchStart += chunkSize * CONCURRENCY_LIMIT
		) {
			const ranges: Array<{ start: number; end: number }> = [];
			for (
				let start = batchStart;
				start <= targetBlock && ranges.length < CONCURRENCY_LIMIT;
				start += chunkSize
			) {
				const end = Math.min(start + chunkSize - 1, targetBlock);
				ranges.push({ start, end });
			}

			const batchLogs = await Promise.all(
				ranges.map((range) =>
					getLogsWithRetry({
						address: BRIDGE_ADDRESS,
						fromBlock: range.start,
						toBlock: range.end,
						topics: [[depositTopic, failedDepositTopic, withdrawalTopic]],
					})
				)
			);

			let depositCount = 0;
			let failedDepositCount = 0;
			let withdrawalCount = 0;
			for (const logs of batchLogs) {
				for (const log of logs) {
					const topic = log.topics[0];
					if (topic === depositTopic) {
						depositCount += 1;
					} else if (topic === failedDepositTopic) {
						failedDepositCount += 1;
					} else if (topic === withdrawalTopic) {
						withdrawalCount += 1;
					}
				}
			}

			const deltaCount = depositCount + failedDepositCount - withdrawalCount;
			const deltaWei = BigInt(deltaCount) * depositAmount;
			currentWei = currentWei + deltaWei;
			totalDepositCount += depositCount;
			totalFailedCount += failedDepositCount;
			totalWithdrawalCount += withdrawalCount;
			const batchEnd = ranges[ranges.length - 1]?.end ?? state.lastScannedBlock;
			state.lastScannedBlock = batchEnd;
			state.cbtcSupplyWei = (currentWei > 0n ? currentWei : 0n).toString();
			state.depositCount = totalDepositCount;
			state.failedDepositCount = totalFailedCount;
			state.withdrawalCount = totalWithdrawalCount;
			state.lastUpdated = Date.now();
			supplyState = state;
			await persistSupplyState(state);

			if (catchUpMode) {
				const prevBucket = Math.floor((batchEnd - chunkSize) / SNAPSHOT_BLOCK_INTERVAL);
				const currentBucket = Math.floor(batchEnd / SNAPSHOT_BLOCK_INTERVAL);
				if (currentBucket > prevBucket) {
					await recordSnapshot(batchEnd);
				}
			}
			chunkCounter += ranges.length;
			if (catchUpMode && chunkCounter % CATCH_UP_PAUSE_EVERY === 0) {
				await sleep(CATCH_UP_PAUSE_MS);
			}
		}
	} finally {
		supplyRefreshInFlight = false;
	}
};

async function estimateBlockTimeSeconds(sampleSize = 500): Promise<number> {
	const latestBlock = await provider.getBlockNumber();
	const safeSample = Math.min(sampleSize, latestBlock);
	if (safeSample <= 0) {
		return 2;
	}
	const [latest, older] = await Promise.all([
		provider.getBlock(latestBlock),
		provider.getBlock(latestBlock - safeSample),
	]);
	if (!latest || !older) {
		return 2;
	}
	const delta = Number(latest.timestamp - older.timestamp);
	return delta > 0 ? delta / safeSample : 2;
}

const backfillTvlHistoryIfNeeded = async (): Promise<void> => {
	if (historyBackfillInFlight) {
		return;
	}
	historyBackfillInFlight = true;
	try {
		const db = getHistoryDb();
		const row = db
			.prepare("SELECT COUNT(*) as count FROM tvl_snapshots")
			.get() as { count?: number } | undefined;
		if (row && typeof row.count === "number" && row.count >= 2) {
			return;
		}
		const [latestBlock, blockTimeSec] = await Promise.all([
			provider.getBlockNumber(),
			estimateBlockTimeSeconds(),
		]);
		const blocksBack = Math.ceil(
			(TVL_HISTORY_DAYS * 24 * 3600) / Math.max(1, blockTimeSec)
		);
		const startBlock = Math.max(
			BRIDGE_LAUNCH_BLOCK,
			latestBlock - blocksBack
		);
		const step = Math.max(1, SNAPSHOT_BLOCK_INTERVAL);
		for (let block = startBlock; block <= latestBlock; block += step) {
			await recordSnapshot(block);
		}
	} finally {
		historyBackfillInFlight = false;
	}
};

const formatDayLabel = (timestampSec: number) =>
	new Date(timestampSec * 1000).toLocaleDateString("en-US", {
		weekday: "short",
	});

const formatDayKey = (timestampSec: number) =>
	new Date(timestampSec * 1000).toISOString().slice(0, 10);

const formatHourLabel = (timestampSec: number) => {
	const hour = new Date(timestampSec * 1000).getUTCHours();
	return `${hour.toString().padStart(2, "0")}:00`;
};

const buildEmptyBridgePoints = (nowSec: number): BridgeTimeseriesPoint[] => {
	const points: BridgeTimeseriesPoint[] = [];
	for (let offset = SERIES_WINDOW_DAYS - 1; offset >= 0; offset -= 1) {
		const dayTimestamp = nowSec - offset * 24 * 3600;
		points.push({
			date: formatDayLabel(dayTimestamp),
			inflowCbtc: 0,
			outflowCbtc: 0,
		});
	}
	return points;
};

const buildEmptyGasPoints = (nowSec: number): GasTimeseriesPoint[] => {
	const points: GasTimeseriesPoint[] = [];
	const currentHour = Math.floor(nowSec / 3600) * 3600;
	for (let offset = 23; offset >= 0; offset -= 1) {
		const ts = currentHour - offset * 3600;
		points.push({
			time: formatHourLabel(ts),
			baseFee: 0,
			l1Fee: 0,
			priorityFee: 0,
		});
	}
	return points;
};

const recordTvlPoint = (totalTvlUsd: number, timestampSec: number) => {
	tvlHistory.push({ timestamp: timestampSec, totalTvlUsd });
	const cutoff = timestampSec - TVL_HISTORY_DAYS * 24 * 3600;
	tvlHistory = tvlHistory.filter((point) => point.timestamp >= cutoff);
};

const getTvlChange = (days: number, currentTotal: number, nowSec: number) => {
	const cutoff = nowSec - days * 24 * 3600;
	const candidates = tvlHistory
		.filter((point) => point.timestamp <= cutoff)
		.sort((a, b) => b.timestamp - a.timestamp);
	const latestCandidate = candidates[0];
	if (!latestCandidate) {
		return { deltaUsd: null, deltaPct: null };
	}
	const past = latestCandidate.totalTvlUsd;
	const deltaUsd = currentTotal - past;
	const deltaPct = past > 0 ? (deltaUsd / past) * 100 : null;
	return { deltaUsd, deltaPct };
};

type TvlChangeResult = {
	deltaUsd: number | null;
	deltaPct: number | null;
	period: "range" | "since_inception" | null;
};

const getTvlChangeFromHistoryDb = (
	hours: number,
	currentTotal: number
): TvlChangeResult => {
	const db = getHistoryDb();
	const cutoff = Date.now() - hours * 60 * 60 * 1000;
	const row = db
		.prepare(
			"SELECT totalTvlUsd FROM tvl_snapshots WHERE timestamp <= ? ORDER BY timestamp DESC LIMIT 1"
		)
		.get(cutoff) as { totalTvlUsd?: number } | undefined;
	if (!row || typeof row.totalTvlUsd !== "number") {
		const oldest = db
			.prepare(
				"SELECT totalTvlUsd FROM tvl_snapshots ORDER BY timestamp ASC LIMIT 1"
			)
			.get() as { totalTvlUsd?: number } | undefined;
		if (!oldest || typeof oldest.totalTvlUsd !== "number") {
			return { deltaUsd: null, deltaPct: null, period: null };
		}
		const past = oldest.totalTvlUsd;
		const deltaUsd = currentTotal - past;
		const deltaPct = past > 0 ? (deltaUsd / past) * 100 : null;
		return { deltaUsd, deltaPct, period: "since_inception" };
	}
	const past = row.totalTvlUsd;
	const deltaUsd = currentTotal - past;
	const deltaPct = past > 0 ? (deltaUsd / past) * 100 : null;
	return { deltaUsd, deltaPct, period: "range" };
};

const pruneBucket = (bucket: BridgeBucket, latestTimestamp: number) => {
	const cutoffMs =
		(latestTimestamp - SERIES_WINDOW_DAYS * 24 * 3600) * 1000;
	Object.keys(bucket).forEach((dayKey) => {
		const dayMs = Date.parse(`${dayKey}T00:00:00Z`);
		if (Number.isFinite(dayMs) && dayMs < cutoffMs) {
			delete bucket[dayKey];
		}
	});
};

const buildBridgePoints = (
	latestTimestamp: number,
	bucket: BridgeBucket
): BridgeTimeseriesPoint[] => {
	const points: BridgeTimeseriesPoint[] = [];
	for (let offset = SERIES_WINDOW_DAYS - 1; offset >= 0; offset -= 1) {
		const dayTimestamp =
			latestTimestamp - offset * 24 * 3600;
		const dayKey = formatDayKey(dayTimestamp);
		const value = bucket[dayKey] ?? { inflow: 0, outflow: 0 };
		points.push({
			date: formatDayLabel(dayTimestamp),
			inflowCbtc: value.inflow,
			outflowCbtc: value.outflow,
		});
	}
	return points;
};

async function fetchBridgeSummary(): Promise<BridgeSummary> {
	const now = Date.now();
	if (bridgeCache && now - bridgeCache.updatedAt < BRIDGE_CACHE_TTL_MS) {
		return bridgeCache.data;
	}

	const state = await loadSupplyState();
	const depositAmountWei = state.depositAmountWei ?? "0";
	const depositCount = state.depositCount ?? 0;
	const failedDepositCount = state.failedDepositCount ?? 0;
	const withdrawalCount = state.withdrawalCount ?? 0;
	const totalDeposits =
		BigInt(depositCount + failedDepositCount) * BigInt(depositAmountWei || "0");
	const totalWithdrawals =
		BigInt(withdrawalCount) * BigInt(depositAmountWei || "0");
	const depositAmountCbtc = Number(formatUnits(depositAmountWei, 18));
	const totalDepositedCbtc = Number(formatUnits(totalDeposits, 18));
	const totalWithdrawnCbtc = Number(formatUnits(totalWithdrawals, 18));
	const supplyNormalized = Number(formatUnits(state.cbtcSupplyWei || "0", 18));
	const vaultBalances = state.vaultBalancesWei ?? {
		baseFeeVault: "0",
		l1FeeVault: "0",
		priorityFeeVault: "0",
		failedDepositVault: "0",
	};
	const vaults: BridgeVaultBalance[] = [
		{
			name: "BaseFeeVault",
			address: BASE_FEE_VAULT,
			balanceCbtc: Number(formatUnits(vaultBalances.baseFeeVault, 18)),
		},
		{
			name: "L1FeeVault",
			address: L1_FEE_VAULT,
			balanceCbtc: Number(formatUnits(vaultBalances.l1FeeVault, 18)),
		},
		{
			name: "PriorityFeeVault",
			address: PRIORITY_FEE_VAULT,
			balanceCbtc: Number(formatUnits(vaultBalances.priorityFeeVault, 18)),
		},
		{
			name: "FailedDepositVault",
			address: FAILED_DEPOSIT_VAULT,
			balanceCbtc: Number(formatUnits(vaultBalances.failedDepositVault, 18)),
		},
	];

	const summary: BridgeSummary = {
		depositAmountCbtc,
		depositCount,
		failedDepositCount,
		withdrawalCount,
		totalDepositedCbtc,
		totalWithdrawnCbtc,
		currentSupplyCbtc: supplyNormalized,
		vaults,
		generatedAt: new Date().toISOString(),
	};

	bridgeCache = { data: summary, updatedAt: now };
	return summary;
}

async function refreshBridgeTimeseries(forceFull = false): Promise<void> {
	if (bridgeTsRefreshInFlight) {
		return;
	}
	bridgeTsRefreshInFlight = true;
	try {
		const [latestBlock, blockTimeSec] = await Promise.all([
			provider.getBlockNumber(),
			estimateBlockTimeSeconds(),
		]);
		const bridge = new Contract(BRIDGE_ADDRESS, BRIDGE_ABI, provider) as unknown as {
			depositAmount: () => Promise<bigint>;
		};
		const iface = new Interface(BRIDGE_ABI);
		const depositEvent = iface.getEvent("Deposit");
		const failedDepositEvent = iface.getEvent("DepositTransferFailed");
		const withdrawalEvent = iface.getEvent("Withdrawal");
		if (!depositEvent || !failedDepositEvent || !withdrawalEvent) {
			throw new Error("Bridge ABI events not found");
		}
		const depositTopic = depositEvent.topicHash;
		const failedDepositTopic = failedDepositEvent.topicHash;
		const withdrawalTopic = withdrawalEvent.topicHash;
		const topicMap: Record<string, "deposit" | "failed" | "withdrawal"> = {
			[depositTopic]: "deposit",
			[failedDepositTopic]: "failed",
			[withdrawalTopic]: "withdrawal",
		};

		const depositAmount = await bridge.depositAmount();
		const depositAmountCbtc = Number(formatUnits(depositAmount, 18));

		const isFull = forceFull || !bridgeTsState;
		const bucket: BridgeBucket = isFull
			? {}
			: { ...bridgeTsState!.bucket };
		const fromBlock = isFull
			? Math.max(
					0,
					latestBlock -
						Math.ceil((SERIES_WINDOW_DAYS * 24 * 3600) / blockTimeSec)
				)
			: Math.min(latestBlock, bridgeTsState!.lastScannedBlock + 1);

		const subgraphCutoff = BRIDGE_SUBGRAPH_URL
			? Math.max(0, latestBlock - SUBGRAPH_LAG_BLOCKS)
			: -1;
		if (subgraphCutoff >= fromBlock) {
			const subgraphEvents = await fetchBridgeEventsFromSubgraph(
				fromBlock,
				subgraphCutoff
			);
			for (const event of subgraphEvents) {
				const timestamp = event.timestamp;
				if (!timestamp) {
					continue;
				}
				const dayKey = formatDayKey(timestamp);
				if (!bucket[dayKey]) {
					bucket[dayKey] = { inflow: 0, outflow: 0 };
				}
				if (event.type === "withdrawal") {
					bucket[dayKey].outflow += depositAmountCbtc;
				} else {
					bucket[dayKey].inflow += depositAmountCbtc;
				}
			}
		}

		const rpcFromBlock = Math.max(fromBlock, subgraphCutoff + 1);
		if (rpcFromBlock <= latestBlock) {
			const step = Math.min(BRIDGE_LOG_CHUNK_SIZE, MAX_LOG_CHUNK_SIZE);
			for (
				let batchStart = rpcFromBlock;
				batchStart <= latestBlock;
				batchStart += step * CONCURRENCY_LIMIT
			) {
				const ranges: Array<{ start: number; end: number }> = [];
				for (
					let start = batchStart;
					start <= latestBlock && ranges.length < CONCURRENCY_LIMIT;
					start += step
				) {
					const end = Math.min(start + step - 1, latestBlock);
					ranges.push({ start, end });
				}

				const batchLogs = await Promise.all(
					ranges.map((range) =>
						getLogsWithRetry({
							address: BRIDGE_ADDRESS,
							fromBlock: range.start,
							toBlock: range.end,
							topics: [[depositTopic, failedDepositTopic, withdrawalTopic]],
						})
					)
				);

				for (const logs of batchLogs) {
					for (const log of logs) {
						const topic = log.topics[0];
						if (!topic) {
							continue;
						}
						const logType = topicMap[topic];
						if (!logType) {
							continue;
						}
						const parsed = iface.parseLog(log);
						if (!parsed) {
							continue;
						}
						const timestamp = Number(parsed.args.timestamp);
						const dayKey = formatDayKey(timestamp);
						if (!bucket[dayKey]) {
							bucket[dayKey] = { inflow: 0, outflow: 0 };
						}
						if (logType === "withdrawal") {
							bucket[dayKey].outflow += depositAmountCbtc;
						} else {
							bucket[dayKey].inflow += depositAmountCbtc;
						}
					}
				}
			}
		}

		const latest = await provider.getBlock(latestBlock);
		const latestTimestamp = latest
			? Number(latest.timestamp)
			: Math.floor(Date.now() / 1000);
		pruneBucket(bucket, latestTimestamp);

		bridgeTsState = {
			lastScannedBlock: latestBlock,
			lastTimestamp: latestTimestamp,
			bucket,
			depositAmountCbtc,
			updatedAt: Date.now(),
		};
	} finally {
		bridgeTsRefreshInFlight = false;
	}
}

export async function getBridgeTimeseries(): Promise<BridgeTimeseriesResult> {
	if (!bridgeTsState) {
		try {
			await refreshBridgeTimeseries(true);
		} catch (error) {
			console.warn("Bridge timeseries refresh failed", error);
		}
	}
	if (!bridgeTsState) {
		const nowSec = Math.floor(Date.now() / 1000);
		return {
			points: buildEmptyBridgePoints(nowSec),
			generatedAt: new Date().toISOString(),
		};
	}
	const points = buildBridgePoints(
		bridgeTsState.lastTimestamp,
		bridgeTsState.bucket
	);
	return {
		points,
		generatedAt: new Date(bridgeTsState.updatedAt).toISOString(),
	};
}

async function refreshGasTimeseries(forceFull = false): Promise<void> {
	if (gasRefreshInFlight) {
		return;
	}
	gasRefreshInFlight = true;
	try {
		const [latestBlock, blockTimeSec] = await Promise.all([
			provider.getBlockNumber(),
			estimateBlockTimeSeconds(),
		]);
		const latest = await provider.getBlock(latestBlock);
		const latestTimestamp = latest
			? Number(latest.timestamp)
			: Math.floor(Date.now() / 1000);
		const currentHour = Math.floor(latestTimestamp / 3600) * 3600;
		const hoursBack = SERIES_WINDOW_DAYS * 24;

		const needsFull = forceFull || !gasSeriesState;
		const points: { timestamp: number; baseFeeGwei: number }[] = needsFull
			? []
			: [...gasSeriesState!.points];
		const lastTimestamp = needsFull
			? currentHour - (hoursBack - 1) * 3600
			: gasSeriesState!.lastTimestamp;
		const startHour = needsFull ? lastTimestamp : lastTimestamp + 3600;

		for (let ts = startHour; ts <= currentHour; ts += 3600) {
			const blocksBack = Math.round((latestTimestamp - ts) / blockTimeSec);
			const blockNumber = Math.max(0, latestBlock - blocksBack);
			const block = await provider.getBlock(blockNumber);
			const baseFee = block?.baseFeePerGas ?? 0n;
			points.push({
				timestamp: ts,
				baseFeeGwei: Number(formatUnits(baseFee, "gwei")),
			});
		}

		while (points.length > hoursBack) {
			points.shift();
		}

		gasSeriesState = {
			points,
			lastTimestamp: currentHour,
			updatedAt: Date.now(),
		};
	} finally {
		gasRefreshInFlight = false;
	}
}

export async function getGasTimeseries(): Promise<GasTimeseriesResult> {
	if (!gasSeriesState) {
		try {
			await refreshGasTimeseries(true);
		} catch (error) {
			console.warn("Gas timeseries refresh failed", error);
		}
	}
	if (!gasSeriesState) {
		const nowSec = Math.floor(Date.now() / 1000);
		return {
			points: buildEmptyGasPoints(nowSec),
			generatedAt: new Date().toISOString(),
		};
	}
	const hourly = gasSeriesState?.points ?? [];
	const last24 = hourly.slice(-24);
	const points: GasTimeseriesPoint[] = last24.map((entry) => ({
		time: formatHourLabel(entry.timestamp),
		baseFee: entry.baseFeeGwei,
		l1Fee: 0,
		priorityFee: 0,
	}));

	return {
		points,
		generatedAt: new Date().toISOString(),
	};
}

export async function getGasHeatmap(): Promise<GasHeatmapResult> {
	if (!gasSeriesState) {
		await refreshGasTimeseries(true);
	}
	const hourly = gasSeriesState?.points ?? [];
	const rows: GasHeatmapRow[] = [];
	for (let dayIndex = 0; dayIndex < SERIES_WINDOW_DAYS; dayIndex += 1) {
		const startIndex = dayIndex * 24;
		const dayHours = hourly.slice(startIndex, startIndex + 24);
		const dayLabel = dayHours.length && dayHours[0]
			? formatDayLabel(dayHours[0].timestamp)
			: "";
		rows.push({
			day: dayLabel,
			hours: dayHours.map((entry, hourIndex) => ({
				hour: hourIndex,
				value: entry.baseFeeGwei,
			})),
		});
	}

	const result: GasHeatmapResult = {
		rows,
		generatedAt: new Date().toISOString(),
	};
	return result;
}

async function refreshGlobalTvlCache(): Promise<void> {
	if (globalTvlInFlight) {
		return;
	}
	globalTvlInFlight = true;
	try {
		const snapshot = await calculateGlobalTvl();
		globalTvlCache = { data: snapshot, updatedAt: Date.now() };
		const timestampSec = Math.floor(Date.now() / 1000);
		recordTvlPoint(snapshot.totalTvlUsd, timestampSec);
	} finally {
		globalTvlInFlight = false;
	}
}

export async function getGlobalTvlSnapshot(): Promise<GlobalTvlResult> {
	const now = Date.now();
	if (globalTvlCache && now - globalTvlCache.updatedAt < TVL_REFRESH_INTERVAL_MS) {
		return globalTvlCache.data;
	}
	await refreshGlobalTvlCache();
	if (globalTvlCache) {
		return globalTvlCache.data;
	}
	return calculateGlobalTvl();
}

async function refreshTvlHistory(): Promise<void> {
	if (tvlRefreshInFlight) {
		return;
	}
	tvlRefreshInFlight = true;
	try {
		await refreshGlobalTvlCache();
	} finally {
		tvlRefreshInFlight = false;
	}
}

export function getTvlChanges(currentTotal: number): {
	change24hUsd: number | null;
	change24hPct: number | null;
	change7dUsd: number | null;
	change7dPct: number | null;
	change24hPeriod?: string | null;
	change7dPeriod?: string | null;
} {
	const change24hDb = getTvlChangeFromHistoryDb(24, currentTotal);
	const change7dDb = getTvlChangeFromHistoryDb(24 * 7, currentTotal);
	const nowSec = Math.floor(Date.now() / 1000);
	const change24h =
		change24hDb.deltaUsd === null && change24hDb.deltaPct === null
			? getTvlChange(1, currentTotal, nowSec)
			: change24hDb;
	const change7d =
		change7dDb.deltaUsd === null && change7dDb.deltaPct === null
			? getTvlChange(7, currentTotal, nowSec)
			: change7dDb;
	return {
		change24hUsd: change24h.deltaUsd,
		change24hPct: change24h.deltaPct,
		change7dUsd: change7d.deltaUsd,
		change7dPct: change7d.deltaPct,
		change24hPeriod: change24hDb.period ?? null,
		change7dPeriod: change7dDb.period ?? null,
	};
}

let aggregationStarted = false;
let aggregationTimers: NodeJS.Timeout[] = [];

export function startAggregationJobs(): () => void {
	if (aggregationStarted) {
		return () => undefined;
	}
	aggregationStarted = true;

	refreshBridgeTimeseries(true).catch((error) => {
		console.warn("Bridge timeseries warmup failed", error);
	});
	refreshGasTimeseries(true).catch((error) => {
		console.warn("Gas timeseries warmup failed", error);
	});
	refreshCbtcSupplyState().catch((error) => {
		console.warn("cBTC supply warmup failed", error);
	});
	refreshTvlHistory().catch((error) => {
		console.warn("TVL history warmup failed", error);
	});
	backfillTvlHistoryIfNeeded().catch((error) => {
		console.warn("TVL history backfill failed", error);
	});

	const bridgeTimer = setInterval(() => {
		refreshBridgeTimeseries(false).catch((error) => {
			console.warn("Bridge timeseries refresh failed", error);
		});
	}, BRIDGE_TS_REFRESH_INTERVAL_MS);

	const gasTimer = setInterval(() => {
		refreshGasTimeseries(false).catch((error) => {
			console.warn("Gas timeseries refresh failed", error);
		});
	}, GAS_REFRESH_INTERVAL_MS);

	const supplyTimer = setInterval(() => {
		refreshCbtcSupplyState().catch((error) => {
			console.warn("cBTC supply refresh failed", error);
		});
	}, TVL_REFRESH_INTERVAL_MS);

	const tvlTimer = setInterval(() => {
		refreshTvlHistory().catch((error) => {
			console.warn("TVL history refresh failed", error);
		});
	}, TVL_REFRESH_INTERVAL_MS);

	const snapshotTimer = setInterval(() => {
		provider
			.getBlockNumber()
			.then((blockNumber) => recordSnapshot(blockNumber))
			.catch((error) => {
				console.warn("Hourly snapshot failed", error);
			});
	}, 60 * 60 * 1000);

	aggregationTimers = [bridgeTimer, gasTimer, supplyTimer, tvlTimer, snapshotTimer];

	return () => {
		aggregationTimers.forEach((timer) => clearInterval(timer));
		aggregationTimers = [];
		aggregationStarted = false;
	};
}

export function stopAggregationJobs(): void {
	if (!aggregationStarted) {
		return;
	}
	aggregationTimers.forEach((timer) => clearInterval(timer));
	aggregationTimers = [];
	aggregationStarted = false;
}

async function fetchNativeCbtcSupply(): Promise<number> {
	const state = await loadSupplyState();
	if (!state.lastUpdated) {
		await refreshCbtcSupplyState();
	}
	const current = supplyState ?? state;
	return Number(formatUnits(current.cbtcSupplyWei || "0", 18));
}

export interface TokenTvlResult {
	symbol: string;
	supply: number;
	priceUsd: number | null;
	tvlUsd: number;
	priceSource: string;
}

export interface GlobalTvlResult {
	totalTvlUsd: number;
	tokens: TokenTvlResult[];
	generatedAt: string;
	lastUpdated: number;
	change24hUsd?: number | null;
	change24hPct?: number | null;
	change7dUsd?: number | null;
	change7dPct?: number | null;
	change24hPeriod?: string | null;
	change7dPeriod?: string | null;
}

export async function getBridgeSummary(): Promise<BridgeSummary> {
	return fetchBridgeSummary();
}

async function fetchTokenSupply(token: TrackedToken): Promise<number> {
	if (token.type === "NATIVE" && token.symbol === "cBTC") {
		return fetchNativeCbtcSupply();
	}

	const contract = new Contract(token.address, ERC20_ABI, provider) as unknown as {
		totalSupply: () => Promise<bigint>;
		decimals: () => Promise<number>;
	};
	const [supplyRaw, decimalsOnchain] = await Promise.all([
		contract.totalSupply(),
		contract.decimals().catch(() => token.decimals),
	]);

	const decimals =
		typeof decimalsOnchain === "number" ? decimalsOnchain : token.decimals;

	return Number(formatUnits(supplyRaw, decimals));
}

export async function calculateGlobalTvl(): Promise<GlobalTvlResult> {
	const results: TokenTvlResult[] = [];

	for (const token of TRACKED_TOKENS) {
		if (token.type !== "ERC20" && token.type !== "NATIVE") {
			continue;
		}
		if (token.address.toLowerCase() === WCBTC_ADDRESS.toLowerCase()) {
			continue;
		}

		try {
			const supply = await fetchTokenSupply(token);
			const dexConfig = (token as {
				dexConfig?: { poolAddress: string; baseTokenSymbol: string };
			}).dexConfig;
			const pricingAddress =
				token.type === "NATIVE" && token.symbol === "cBTC"
					? WCBTC_ADDRESS
					: token.address;

			const price = await getTokenPriceUsd({
				tokenAddress: pricingAddress,
				symbol: token.symbol,
				primaryPricingStrategy: token.primaryPricingStrategy,
				...(token.coingeckoId ? { coingeckoId: token.coingeckoId } : {}),
				...(token.peggedTo ? { peggedTo: token.peggedTo } : {}),
				...(dexConfig ? { dexConfig } : {}),
			});

			const priceUsd = price.priceUsd;
			const tvlUsd = priceUsd === null ? 0 : supply * priceUsd;

			results.push({
				symbol: token.symbol,
				supply,
				priceUsd,
				tvlUsd,
				priceSource: price.priceSource,
			});
		} catch (error) {
			console.error(`Failed to process token ${token.symbol}:`, error);
			results.push({
				symbol: token.symbol,
				supply: 0,
				priceUsd: null,
				tvlUsd: 0,
				priceSource: "error",
			});
		}
	}

	const totalTvlUsd = results.reduce((sum, token) => sum + token.tvlUsd, 0);

	return {
		totalTvlUsd,
		tokens: results,
		generatedAt: new Date().toISOString(),
		lastUpdated: Date.now(),
	};
}
