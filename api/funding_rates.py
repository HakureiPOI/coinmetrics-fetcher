"""
CoinMetrics 永续合约资金费率数据获取模块
"""

import logging
from typing import Optional

import pandas as pd

from api.base_fetcher import BaseFetcher
from utils import validate_time_range

logger = logging.getLogger(__name__)


class FundingRateFetcher(BaseFetcher):
    """资金费率数据获取器"""

    def _fetch_perpetual_markets(self, exchange: str, base: str) -> list[str]:
        """获取永续合约市场列表"""
        df = self.ref_api.get_markets(
            exchange=exchange, market_type="future", base=base, verbose=False
        )
        perpetuals = df[df["symbol"].str.contains("PERPETUAL", case=True, na=False)]
        return perpetuals["market"].tolist()

    def _fetch_funding_rates_batch(self, markets: list[str], start_time: str, end_time: str) -> pd.DataFrame:
        """批量获取资金费率数据"""
        return self.ts_api.get_market_funding_rates(
            markets=",".join(markets),
            start_time=start_time, end_time=end_time,
            page_size=10000, verbose=False,
        )

    def _fetch_predicted_rates_batch(self, markets: list[str], start_time: str, end_time: str) -> pd.DataFrame:
        """批量获取预计资金费率数据"""
        return self.ts_api.get_market_funding_rates_predicted(
            markets=",".join(markets),
            start_time=start_time, end_time=end_time,
            page_size=10000, verbose=False,
        )

    def _get_market_metadata(self, exchange: str, base: str) -> pd.DataFrame:
        """获取市场元数据"""
        return self.ref_api.get_markets(
            exchange=exchange, market_type="future", base=base, verbose=False
        )[["market", "symbol", "pair"]]

    def get_funding_rates(
        self,
        exchange: str,
        base: str,
        start_time: str,
        end_time: str,
        batch_size: int = 50,
        max_workers: int = 4,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """获取永续合约资金费率数据"""
        validate_time_range(start_time, end_time)

        markets = self._fetch_perpetual_markets(exchange, base)
        if verbose:
            logger.info(f"[资金费率] {exchange.upper()} {base.upper()} | {len(markets)} 个市场")

        if not markets:
            return pd.DataFrame()

        df = self._fetch_all_concurrent(
            markets, start_time, end_time, batch_size, max_workers,
            self._fetch_funding_rates_batch, "资金费率", verbose
        )

        if len(df) > 0:
            df = pd.merge(df, self._get_market_metadata(exchange, base), on="market", how="left")
            df = df.sort_values(["market", "time"]).reset_index(drop=True)

        if verbose:
            logger.info(f"[资金费率] 完成: {len(df)} 条")
        return df

    def get_predicted_funding_rates(
        self,
        exchange: str,
        base: str,
        start_time: str,
        end_time: str,
        batch_size: int = 50,
        max_workers: int = 4,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """获取永续合约预计资金费率数据"""
        validate_time_range(start_time, end_time)

        markets = self._fetch_perpetual_markets(exchange, base)
        if verbose:
            logger.info(f"[预计资金费率] {exchange.upper()} {base.upper()} | {len(markets)} 个市场")

        if not markets:
            return pd.DataFrame()

        df = self._fetch_all_concurrent(
            markets, start_time, end_time, batch_size, max_workers,
            self._fetch_predicted_rates_batch, "预计资金费率", verbose
        )

        if len(df) > 0:
            df = pd.merge(df, self._get_market_metadata(exchange, base), on="market", how="left")
            df = df.sort_values(["market", "time"]).reset_index(drop=True)

        if verbose:
            logger.info(f"[预计资金费率] 完成: {len(df)} 条")
        return df