"""
CoinMetrics 期货数据获取模块
"""

import logging
from typing import Optional

import pandas as pd

from api.base_fetcher import BaseFetcher
from utils import validate_time_range

logger = logging.getLogger(__name__)


class FuturesDataFetcher(BaseFetcher):
    """期货数据获取器"""

    def _fetch_futures_markets(self, exchange: str, base: str) -> list[str]:
        """获取所有期货市场列表（包括永续和交割）"""
        df = self.ref_api.get_markets(
            exchange=exchange, market_type="future", base=base, verbose=False
        )
        return df["market"].tolist()

    def _fetch_candles_batch(self, markets: list[str], start_time: str, end_time: str) -> pd.DataFrame:
        """批量获取 K 线数据"""
        return self.ts_api.get_market_candles(
            markets=",".join(markets),
            start_time=start_time, end_time=end_time,
            page_size=10000, verbose=False,
        )

    def _get_market_metadata(self, exchange: str, base: str) -> pd.DataFrame:
        """获取市场元数据"""
        return self.ref_api.get_markets(
            exchange=exchange, market_type="future", base=base, verbose=False
        )[["market", "symbol", "pair"]]

    def get_candles(
        self,
        exchange: str,
        base: str,
        start_time: str,
        end_time: str,
        batch_size: int = 50,
        max_workers: int = 4,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """获取所有期货的分钟级 K 线数据"""
        validate_time_range(start_time, end_time)

        markets = self._fetch_futures_markets(exchange, base)
        if verbose:
            logger.info(f"[K线] {exchange.upper()} {base.upper()} | {len(markets)} 个市场")

        if not markets:
            return pd.DataFrame()

        df = self._fetch_all_concurrent(
            markets, start_time, end_time, batch_size, max_workers,
            self._fetch_candles_batch, "K线", verbose
        )

        if len(df) > 0:
            df = pd.merge(df, self._get_market_metadata(exchange, base), on="market", how="left")

        if verbose:
            logger.info(f"[K线] 完成: {len(df)} 条")
        return df