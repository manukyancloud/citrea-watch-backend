export interface TrackedToken {
  symbol: string;
  address: string;
  decimals: number;
  type: 'ERC20' | 'ERC721' | 'ERC1155' | 'NATIVE';
  coingeckoId?: string;
  primaryPricingStrategy: 'coingecko' | 'dex' | 'pegged' | 'none';
  peggedTo?: string;
  dexConfig?: {
    poolAddress: string;
    baseTokenSymbol: string;
  };
}

export const TRACKED_TOKENS: TrackedToken[] = [
  // ==== STABLECOINS & PEGGED TOKENS ====
  {
    symbol: 'cBTC',
    address: '0x0000000000000000000000000000000000000000',
    decimals: 18,
    type: 'NATIVE',
    primaryPricingStrategy: 'dex',
    dexConfig: {
        poolAddress: '0x5d4b518984ae9778479ee2ea782b9925bbf17080',
        baseTokenSymbol: 'ctUSD'
    }
  },
  {
    symbol: 'ctUSD',
    address: '0x8D82c4E3c936C7B5724A382a9c5a4E6Eb7aB6d5D',
    decimals: 6,
    type: 'ERC20',
    primaryPricingStrategy: 'dex',
    dexConfig: {
        poolAddress: '0x172d2ab563afdaace7247a6592ee1be62e791165',
        baseTokenSymbol: 'USDC.e'
    }
  },
  {
    symbol: 'USDC.e',
    address: '0xE045e6c36cF77FAA2CfB54466D71A3aEF7bbE839',
    decimals: 6,
    type: 'ERC20',
    primaryPricingStrategy: 'pegged', // 1.0
    peggedTo: 'usd-coin'
  },
  {
    symbol: 'USDT.e',
    address: '0x9f3096Bac87e7F03DC09b0B416eB0DF837304dc4',
    decimals: 6,
    type: 'ERC20',
    primaryPricingStrategy: 'pegged',
    peggedTo: 'usd-coin'
  },
  {
    symbol: 'JUSD',
    address: '0x0987D3720D38847ac6dBB9D025B9dE892a3CA35C',
    decimals: 18,
    type: 'ERC20',
    primaryPricingStrategy: 'pegged',
    peggedTo: 'usd-coin'
  },
  {
    symbol: 'GUSD',
    address: '0xAC8c1AEB584765DB16ac3e08D4736CFcE198589B',
    decimals: 18,
    type: 'ERC20',
    primaryPricingStrategy: 'dex',
    dexConfig: {
      poolAddress: '0xb22325fe6e033c6b7cefb7bc69c9650ffdc691f9',
      baseTokenSymbol: 'ctUSD'
    }
  },

  // ==== BITCOIN-LINKED ASSETS ====
  {
    symbol: 'wcBTC',
    address: '0x3100000000000000000000000000000000000006',
    decimals: 18,
    type: 'ERC20',
    primaryPricingStrategy: 'pegged',
    peggedTo: 'bitcoin'
  },
  {
    symbol: 'syBTC',
    address: '0x384157027B1CDEAc4e26e3709667BB28735379Bb',
    decimals: 8,
    type: 'ERC20',
    primaryPricingStrategy: 'pegged',
    peggedTo: 'bitcoin'
  },
  {
    symbol: 'WBTC.e',
    address: '0xDF240DC08B0FdaD1d93b74d5048871232f6BEA3d',
    decimals: 8,
    type: 'ERC20',
    primaryPricingStrategy: 'dex',
    dexConfig: {
        poolAddress: '0xaea5cf09209631b6a3a69d5798034e2efdbe2cc8',
        baseTokenSymbol: 'cBTC'
    }
  },
];