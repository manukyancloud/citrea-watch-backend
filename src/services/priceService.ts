import { Contract, JsonRpcProvider } from "ethers";

import { TRACKED_TOKENS } from "../config/tokens";
import { env } from "../config/env";

export interface PriceResult {
	priceUsd: number | null;
	priceSource: string;
	updatedAt: string;
}

interface PriceCacheEntry {
	priceUsd: number | null;
	priceSource: string;
	fetchedAt: number;
}

const CACHE_TTL_MS = 60_000;
const priceCache = new Map<string, PriceCacheEntry>();
const coingeckoIdCache = new Map<string, PriceCacheEntry>();

const provider = new JsonRpcProvider(env.RPC_URL);

const COINGECKO_ENDPOINT = "https://api.coingecko.com/api/v3/simple/price";
const WCBTC_ADDRESS = "0x3100000000000000000000000000000000000006";

const isDev = env.NODE_ENV !== "production";
const dexLog = (...args: unknown[]) => {
	if (isDev) {
		console.log(...args);
	}
};
const dexError = (...args: unknown[]) => {
	if (isDev) {
		console.error(...args);
	}
};

function getCacheKey(symbol: string, strategy: string): string {
	return `${symbol.toLowerCase()}::${strategy}`;
}

async function fetchCoingeckoPriceUsd(coingeckoId: string): Promise<number> {
	const cached = coingeckoIdCache.get(coingeckoId);
	const now = Date.now();

	if (cached && now - cached.fetchedAt < CACHE_TTL_MS) {
		if (typeof cached.priceUsd === "number") {
			return cached.priceUsd;
		}
	}

	const url = `${COINGECKO_ENDPOINT}?ids=${encodeURIComponent(
		coingeckoId
	)}&vs_currencies=usd`;

	try {
		const response = await fetch(url, {
			method: "GET",
			headers: {
				Accept: "application/json",
			},
		});

		if (response.status === 429) {
			if (cached && typeof cached.priceUsd === "number") {
				console.warn(
					`Coingecko rate limited for ${coingeckoId}; using stale cache.`
				);
				return cached.priceUsd;
			}
			throw new Error("Coingecko rate limited (429)");
		}

		if (!response.ok) {
			throw new Error(`Coingecko request failed: ${response.status}`);
		}

		const data = (await response.json()) as Record<string, { usd?: number }>;
		const price = data?.[coingeckoId]?.usd;

		if (typeof price !== "number") {
			throw new Error("Coingecko response missing USD price");
		}

		coingeckoIdCache.set(coingeckoId, {
			priceUsd: price,
			priceSource: "coingecko",
			fetchedAt: now,
		});

		return price;
	} catch (error) {
		if (cached && typeof cached.priceUsd === "number") {
			console.warn(
				`Coingecko error for ${coingeckoId}; using stale cache.`,
				error
			);
			return cached.priceUsd;
		}
		throw error;
	}
}

function resolveCoingeckoIdFromSymbol(symbol?: string): string | null {
	if (!symbol) {
		return null;
	}
	switch (symbol.toLowerCase()) {
		case "btc":
		case "cbtc":
		case "bitcoin":
			return "bitcoin";
		case "usdc":
		case "usd-coin":
			return "usd-coin";
		case "usdt":
		case "tether":
			return "tether";
		case "eth":
		case "ethereum":
			return "ethereum";
		default:
			return null;
	}
}

export async function fetchPriceFromAlgebraPool(
	tokenAddress: string,
	poolAddress: string,
	baseTokenPriceUsd: number,
	baseTokenAddress: string
): Promise<number> {
	dexLog("[DEX] fetchPriceFromAlgebraPool input", {
		tokenAddress,
		poolAddress,
		baseTokenPriceUsd,
	});
	const slot0Selector = "0x3850c7bd";
	dexLog("[DEX] slot0 selector", {
		poolAddress,
		selector: slot0Selector,
	});
	const code = await provider.getCode(poolAddress);
	dexLog("[DEX] pool bytecode size", {
		poolAddress,
		bytecodeSize: code === "0x" ? 0 : (code.length - 2) / 2,
	});

	const V3_POOL_ABI = [
		"function slot0() view returns (uint160 sqrtPriceX96,int24 tick,uint16 observationIndex,uint16 observationCardinality,uint16 observationCardinalityNext,uint8 feeProtocol,bool unlocked)",
		"function token0() view returns (address)",
		"function token1() view returns (address)",
	];
	const V3_SLOT0_SIMPLE_ABI = [
		"function slot0() view returns (uint160 sqrtPriceX96)",
	];
	const ALGEBRA_GLOBAL_STATE_ABI = [
		"function globalState() view returns (uint160 price, int24 tick, uint16 lastFee, uint8 pluginConfig, uint16 communityFee, bool unlocked)",
	];
	const ERC20_META_ABI = [
		"function decimals() view returns (uint8)",
		"function symbol() view returns (string)",
	];

	try {
		const pool = new Contract(poolAddress, V3_POOL_ABI, provider) as unknown as {
			slot0: () => Promise<[bigint]>;
			token0: () => Promise<string>;
			token1: () => Promise<string>;
		};

		let token0Address: string;
		let token1Address: string;
		let sqrtPriceX96: bigint | null = null;
		let poolType: "V3" | "ALGEBRA_V2" = "V3";

		try {
			const algebraPool = new Contract(
				poolAddress,
				ALGEBRA_GLOBAL_STATE_ABI,
				provider
			) as unknown as { globalState: () => Promise<[bigint]> };
			const globalState = await algebraPool.globalState();
			sqrtPriceX96 = globalState[0];
			poolType = "ALGEBRA_V2";
			dexLog("[DEX] globalState method used", {
				poolAddress,
				sqrtPriceX96: sqrtPriceX96.toString(),
			});
			[token0Address, token1Address] = await Promise.all([
				pool.token0(),
				pool.token1(),
			]);
		} catch {
			try {
				const [token0, token1, slot0] = await Promise.all([
					pool.token0(),
					pool.token1(),
					pool.slot0(),
				]);
				token0Address = token0;
				token1Address = token1;
				sqrtPriceX96 = slot0[0];
				dexLog("[DEX] pool data", {
					poolType: "V3",
					token0Address,
					token1Address,
					sqrtPriceX96: sqrtPriceX96.toString(),
				});
				dexLog("[DEX] slot0 method used", { poolAddress });
			} catch {
				try {
					const simplePool = new Contract(
						poolAddress,
						V3_SLOT0_SIMPLE_ABI,
						provider
					) as unknown as { slot0: () => Promise<[bigint]> };
					const slot0Simple = await simplePool.slot0();
					sqrtPriceX96 = slot0Simple[0];
					dexLog("[DEX] slot0 simple ABI ok", {
						sqrtPriceX96: sqrtPriceX96.toString(),
					});
					[token0Address, token1Address] = await Promise.all([
						pool.token0(),
						pool.token1(),
					]);
					dexLog("[DEX] slot0 simple method used", { poolAddress });
				} catch {
					try {
						[token0Address, token1Address] = await Promise.all([
							pool.token0(),
							pool.token1(),
						]);
						console.warn(
							"[DEX] pool exists but slot0/globalState not accessible; returning 0.",
							{
								poolAddress,
								token0Address,
								token1Address,
							}
						);
						return 0;
					} catch (tokenError) {
						console.error(
							"[DEX] token0/token1 failed; invalid pool.",
							tokenError
						);
						throw tokenError;
					}
				}
			}
		}

		const token0 = new Contract(token0Address, ERC20_META_ABI, provider) as unknown as {
			decimals: () => Promise<number>;
			symbol: () => Promise<string>;
		};
		const token1 = new Contract(token1Address, ERC20_META_ABI, provider) as unknown as {
			decimals: () => Promise<number>;
			symbol: () => Promise<string>;
		};

		const [decimals0, decimals1, symbol0, symbol1] = await Promise.all([
			token0.decimals().catch(() => 18),
			token1.decimals().catch(() => 18),
			token0.symbol().catch(() => ""),
			token1.symbol().catch(() => ""),
		]);

		if (!sqrtPriceX96) {
			throw new Error("Missing sqrtPriceX96 for V3 pool");
		}
		const dec0 = 10n ** BigInt(decimals0);
		const dec1 = 10n ** BigInt(decimals1);
		const SCALE = 10n ** 18n;
		let priceRatio = (sqrtPriceX96 * sqrtPriceX96 * SCALE) >> 192n;
		priceRatio = (priceRatio * dec0) / dec1;
		const priceToken1PerToken0 = Number(priceRatio) / 1e18;
		dexLog("[DEX] price math", {
			poolType,
			priceRatio: priceRatio.toString(),
			priceToken1PerToken0,
			decimals0,
			decimals1,
			symbol0,
			symbol1,
		});

		const tokenAddressLower = tokenAddress.toLowerCase();
		const token0Lower = token0Address.toLowerCase();
		const token1Lower = token1Address.toLowerCase();
		const baseTokenLower = baseTokenAddress.toLowerCase();

		const baseIsToken0 = baseTokenLower === token0Lower;
		const baseIsToken1 = baseTokenLower === token1Lower;
		if (!baseIsToken0 && !baseIsToken1) {
			throw new Error(
				`Base token ${baseTokenAddress} not found in pool (${symbol0}/${symbol1})`
			);
		}

		const targetIsToken0 = tokenAddressLower === token0Lower;
		const targetIsToken1 = tokenAddressLower === token1Lower;
		if (!targetIsToken0 && !targetIsToken1) {
			throw new Error(
				`Token ${tokenAddress} not found in pool (${symbol0}/${symbol1})`
			);
		}

		if ((baseIsToken0 && targetIsToken0) || (baseIsToken1 && targetIsToken1)) {
			dexLog("[DEX] base token equals target; using base price", {
				poolType,
				baseTokenPriceUsd,
			});
			return baseTokenPriceUsd;
		}

		if (baseIsToken1 && targetIsToken0) {
			const finalPrice = priceToken1PerToken0 * baseTokenPriceUsd;
			dexLog("[DEX] base token1 -> target token0", {
				finalPrice,
				poolType,
			});
			return finalPrice;
		}

		if (baseIsToken0 && targetIsToken1) {
			if (priceToken1PerToken0 === 0) {
				dexLog("[DEX] base token0 -> target token1 but price is zero", {
					poolType,
				});
				return 0;
			}
			const finalPrice = baseTokenPriceUsd / priceToken1PerToken0;
			dexLog("[DEX] base token0 -> target token1", {
				finalPrice,
				poolType,
			});
			return finalPrice;
		}

		throw new Error("Unable to derive price from pool");
	} catch (error) {
		dexError("[DEX] fetchPriceFromAlgebraPool error", error);
		throw error;
	}
}

export async function getTokenPriceUsd(params: {
	tokenAddress: string;
	symbol: string;
	primaryPricingStrategy: "coingecko" | "dex" | "pegged" | "none";
	coingeckoId?: string;
	peggedTo?: string;
	dexConfig?: {
		poolAddress: string;
		baseTokenSymbol: string;
	};
}): Promise<PriceResult> {
	const cacheKey = getCacheKey(params.symbol, params.primaryPricingStrategy);
	const cached = priceCache.get(cacheKey);
	const now = Date.now();

	if (cached && now - cached.fetchedAt < CACHE_TTL_MS) {
		return {
			priceUsd: cached.priceUsd,
			priceSource: cached.priceSource,
			updatedAt: new Date(cached.fetchedAt).toISOString(),
		};
	}

	let priceUsd: number | null = null;
	let priceSource: string = params.primaryPricingStrategy;

	switch (params.primaryPricingStrategy) {
		case "coingecko": {
			if (!params.coingeckoId) {
				throw new Error("Missing coingeckoId for coingecko pricing");
			}
			priceUsd = await fetchCoingeckoPriceUsd(params.coingeckoId);
			priceSource = "coingecko";
			break;
		}
		case "pegged": {
			if (params.peggedTo === "bitcoin") {
				priceUsd = await fetchCoingeckoPriceUsd("bitcoin");
			} else if (params.peggedTo === "usd-coin") {
				priceUsd = 1.0;
			} else {
				priceUsd = 1.0;
			}
			priceSource = params.peggedTo ? `pegged:${params.peggedTo}` : "pegged";
			break;
		}
		case "dex": {
			try {
				if (!params.dexConfig?.poolAddress) {
					throw new Error("Missing dexConfig.poolAddress for DEX pricing");
				}
				const baseToken = TRACKED_TOKENS.find(
					(token) =>
						token.symbol.toLowerCase() ===
						params.dexConfig?.baseTokenSymbol.toLowerCase()
				);
				if (!baseToken) {
					throw new Error(
						`Base token not found: ${params.dexConfig.baseTokenSymbol}`
					);
				}

				const baseTokenAddress =
					baseToken.type === "NATIVE" && baseToken.symbol === "cBTC"
						? WCBTC_ADDRESS
						: baseToken.address;

				const baseTokenPrice = await getTokenPriceUsd({
					tokenAddress: baseTokenAddress,
					symbol: baseToken.symbol,
					primaryPricingStrategy: baseToken.primaryPricingStrategy,
					...(baseToken.coingeckoId
						? { coingeckoId: baseToken.coingeckoId }
						: {}),
					...(baseToken.peggedTo ? { peggedTo: baseToken.peggedTo } : {}),
					...(baseToken.dexConfig ? { dexConfig: baseToken.dexConfig } : {}),
				});

				const baseTokenPriceUsd = baseTokenPrice.priceUsd;
				if (!baseTokenPriceUsd || baseTokenPriceUsd <= 0) {
					throw new Error(
						`Invalid base token price for ${baseToken.symbol}`
					);
				}
				priceUsd = await fetchPriceFromAlgebraPool(
					params.tokenAddress,
					params.dexConfig.poolAddress,
					baseTokenPriceUsd,
					baseTokenAddress
				);
				priceSource = "dex";
			} catch (error) {
				console.warn("DEX pricing failed; using 0.", error);
				priceUsd = 0;
				priceSource = "dex";
			}
			break;
		}
		case "none": {
			priceUsd = null;
			priceSource = "none";
			break;
		}
		default: {
			priceUsd = null;
			priceSource = "none";
			break;
		}
	}

	priceCache.set(cacheKey, {
		priceUsd,
		priceSource,
		fetchedAt: now,
	});

	return {
		priceUsd,
		priceSource,
		updatedAt: new Date(now).toISOString(),
	};
}
