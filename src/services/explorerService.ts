import axios from "axios";

import { env } from "../config/env";

const EXPLORER_API_BASE = env.EXPLORER_API_BASE;

const EXPLORER_CACHE_TTL_MS = 5 * 1000;
let explorerCache:
  | { data: ExplorerSummary; updatedAt: number }
  | null = null;

interface ExplorerChartPoint {
  timestamp: number;
  value: number;
}

export interface ExplorerSummary {
  totalTransactions: number | null;
  totalAddresses: number | null;
  totalBlocks: number | null;
  txCount24h: number | null;
  txCount7d: number | null;
  tps: number | null;
  averageBlockTimeSec: number | null;
  totalTokens: number | null;
  txFees24hCbtc: number | null;
  gasPrices: {
    slow: number | null;
    average: number | null;
    fast: number | null;
  } | null;
  gasPriceUpdatedAt: string | null;
  generatedAt: string;
  source: string;
}

const parseNumber = (value: unknown): number | null => {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

const normalizeTimestamp = (value: number): number => {
  if (value > 1e12) {
    return Math.floor(value / 1000);
  }
  return value;
};

const normalizeBlockTimeSeconds = (value: number | null): number | null => {
  if (value === null) {
    return null;
  }
  if (value >= 1000) {
    return value / 1000;
  }
  return value;
};

const parseChartPoints = (data: unknown): ExplorerChartPoint[] => {
  if (!data || typeof data !== "object") {
    return [];
  }

  const root = data as Record<string, unknown>;
  const nested =
    (root.data as Record<string, unknown> | undefined) ??
    (root.result as Record<string, unknown> | undefined) ??
    root;
  const items =
    (nested.items as unknown[]) ||
    (nested.chart as unknown[]) ||
    (nested.points as unknown[]) ||
    (nested.series as unknown[]) ||
    [];

  if (!Array.isArray(items)) {
    return [];
  }

  return items
    .map((item) => {
      if (!item || typeof item !== "object") {
        return null;
      }
      const row = item as Record<string, unknown>;
      let timestampValue =
        parseNumber(row.timestamp) ??
        parseNumber(row.date) ??
        parseNumber(row.time) ??
        null;
      if (timestampValue === null && typeof row.date === "string") {
        const parsedDate = Date.parse(row.date);
        if (Number.isFinite(parsedDate)) {
          timestampValue = Math.floor(parsedDate / 1000);
        }
      }
      const value =
        parseNumber(row.value) ??
        parseNumber(row.count) ??
        parseNumber(row.transactions) ??
        parseNumber(row.tx_count) ??
        null;

      if (timestampValue === null || value === null) {
        return null;
      }

      return {
        timestamp: normalizeTimestamp(timestampValue),
        value,
      } as ExplorerChartPoint;
    })
    .filter((point): point is ExplorerChartPoint => point !== null);
};

const resolveStats = async (): Promise<Record<string, unknown> | null> => {
  const candidates = [
    `${EXPLORER_API_BASE}/v2/stats`,
    `${EXPLORER_API_BASE}/v1/counters`,
    `${EXPLORER_API_BASE}/stats`,
  ];

  for (const url of candidates) {
    try {
      const response = await axios.get(url, { timeout: 10_000 });
      if (response.status >= 200 && response.status < 300) {
        return response.data as Record<string, unknown>;
      }
    } catch (error) {
      continue;
    }
  }

  return null;
};

const resolveTransactionsChart = async (): Promise<ExplorerChartPoint[]> => {
  const candidates = [
    `${EXPLORER_API_BASE}/v2/stats/charts/transactions`,
    `${EXPLORER_API_BASE}/v1/stats/charts/transactions`,
    `${EXPLORER_API_BASE}/stats/charts/transactions`,
  ];

  for (const url of candidates) {
    try {
      const response = await axios.get(url, { timeout: 10_000 });
      if (response.status >= 200 && response.status < 300) {
        return parseChartPoints(response.data);
      }
    } catch (error) {
      continue;
    }
  }

  return [];
};

const resolveTokenCount = async (): Promise<number | null> => {
  const baseUrl = `${EXPLORER_API_BASE}/v2/tokens`;
  let total = 0;
  let params: Record<string, unknown> = { limit: 50 };
  const maxPages = 20;

  for (let page = 0; page < maxPages; page += 1) {
    try {
      const response = await axios.get(baseUrl, {
        params,
        timeout: 10_000,
      });
      if (response.status < 200 || response.status >= 300) {
        break;
      }
      const payload = response.data as Record<string, unknown>;
      const totalItems = parseNumber(payload.total) ?? parseNumber(payload.total_items);
      if (typeof totalItems === "number") {
        return totalItems;
      }
      const items = (payload.items as unknown[]) ?? [];
      if (!Array.isArray(items)) {
        break;
      }
      total += items.length;
      const nextPage = payload.next_page_params as
        | Record<string, unknown>
        | null
        | undefined;
      if (!nextPage || Object.keys(nextPage).length === 0) {
        break;
      }
      params = { ...nextPage, limit: 50 };
    } catch (error) {
      break;
    }
  }

  return total > 0 ? total : null;
};

const computeTxWindow = (
  points: ExplorerChartPoint[],
  days: number
): number | null => {
  if (!points.length) {
    return null;
  }

  const sorted = [...points].sort((a, b) => a.timestamp - b.timestamp);
  const latest = sorted[sorted.length - 1];
  if (!latest) {
    return null;
  }
  const cutoff = latest.timestamp - days * 24 * 3600;
  const windowPoints = sorted.filter((point) => point.timestamp >= cutoff);
  if (!windowPoints.length) {
    return null;
  }

  const values = windowPoints.map((point) => point.value);
  const isNonDecreasing = values.every(
    (value, index) => index === 0 || value >= (values[index - 1] ?? value)
  );
  if (isNonDecreasing) {
    const first = values[0] ?? 0;
    const last = values[values.length - 1] ?? 0;
    const delta = last - first;
    return delta >= 0 ? delta : null;
  }

  return windowPoints.reduce((sum, point) => sum + point.value, 0);
};

const computeTpsFromWindow = (
  windowTxCount: number | null,
  windowSeconds: number
): number | null => {
  if (!windowTxCount || windowSeconds <= 0) {
    return null;
  }
  return windowTxCount / windowSeconds;
};

export async function getExplorerSummary(): Promise<ExplorerSummary> {
  const now = Date.now();
  if (explorerCache && now - explorerCache.updatedAt < EXPLORER_CACHE_TTL_MS) {
    return explorerCache.data;
  }

  const [stats, txChart] = await Promise.all([
    resolveStats(),
    resolveTransactionsChart(),
  ]);
  const totalTokens = await resolveTokenCount();

  const statsObj = stats ?? {};
  const statsRoot =
    (statsObj.data as Record<string, unknown> | undefined) ??
    (statsObj.result as Record<string, unknown> | undefined) ??
    statsObj;
  const counters =
    (statsRoot.counters as Record<string, unknown> | undefined) ?? statsRoot;
  const totalTransactions =
    parseNumber(counters.total_transactions) ??
    parseNumber(counters.transactions) ??
    parseNumber(counters.tx_count) ??
    null;
  const totalAddresses =
    parseNumber(counters.total_addresses) ??
    parseNumber(counters.addresses) ??
    parseNumber(counters.address_count) ??
    null;
  const totalBlocks =
    parseNumber(counters.total_blocks) ??
    parseNumber(counters.blocks) ??
    parseNumber(counters.block_count) ??
    parseNumber(statsRoot.total_blocks) ??
    parseNumber(statsRoot.blocks) ??
    parseNumber(statsRoot.block_height) ??
    parseNumber(statsRoot.latest_block) ??
    null;
  const averageBlockTimeRaw = parseNumber(statsRoot.average_block_time) ?? null;
  const averageBlockTimeSec = normalizeBlockTimeSeconds(averageBlockTimeRaw);
  const transactionsToday =
    parseNumber(statsRoot.transactions_today) ?? null;
  const gasUsedToday = parseNumber(statsRoot.gas_used_today) ?? null;

  const gasPricesRaw = statsRoot.gas_prices as
    | { slow?: unknown; average?: unknown; fast?: unknown }
    | undefined;
  const gasPrices = gasPricesRaw
    ? {
      slow: parseNumber(gasPricesRaw.slow),
      average: parseNumber(gasPricesRaw.average),
      fast: parseNumber(gasPricesRaw.fast),
    }
    : null;
  const gasPriceUpdatedAtRaw =
    (statsRoot.gas_price_updated_at as string | undefined) ?? null;

  const txCount24h = computeTxWindow(txChart, 1);
  const txCount7d = computeTxWindow(txChart, 7);
  const tpsFrom24h = computeTpsFromWindow(txCount24h, 24 * 3600);
  const tpsFromToday = (() => {
    if (!transactionsToday) {
      return null;
    }
    const now = new Date();
    const startOfDay = new Date(
      Date.UTC(
        now.getUTCFullYear(),
        now.getUTCMonth(),
        now.getUTCDate(),
        0,
        0,
        0,
        0
      )
    );
    const secondsSinceStart = Math.max(
      1,
      (now.getTime() - startOfDay.getTime()) / 1000
    );
    return transactionsToday / secondsSinceStart;
  })();
  const tpsFromStats =
    parseNumber(statsRoot.tps) ??
    parseNumber(statsRoot.average_tps) ??
    parseNumber(statsRoot.avg_tps) ??
    null;
  const tps = tpsFromStats ?? tpsFrom24h ?? tpsFromToday ?? null;
  const txFees24hCbtc =
    typeof gasUsedToday === "number" &&
    gasPrices?.average !== null &&
    typeof gasPrices?.average === "number"
      ? (gasUsedToday * gasPrices.average) / 1e9
      : null;

  const summary: ExplorerSummary = {
    totalTransactions,
    totalAddresses,
    totalBlocks,
    txCount24h,
    txCount7d,
    tps,
    averageBlockTimeSec,
    totalTokens,
    txFees24hCbtc,
    gasPrices,
    gasPriceUpdatedAt: gasPriceUpdatedAtRaw,
    generatedAt: new Date().toISOString(),
    source: EXPLORER_API_BASE,
  };

  explorerCache = { data: summary, updatedAt: now };
  return summary;
}
