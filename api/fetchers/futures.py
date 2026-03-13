"""
CoinMetrics 期货数据获取模块
"""

import logging
from typing import Optional

import pandas as pd

from ..base_fetcher import BaseFetcher
from utils import validate_time_range, ValidationError

logger = logging.getLogger(__name__)


class FuturesDataFetcher(BaseFetcher):
    """期货数据获取器"""

    def _fetch_candles_batch(self, markets: list[str], start_time: str, end_time: str, frequency: str) -> pd.DataFrame:
        """批量获取 K 线数据"""
        return self.ts_api.get_market_candles(
            markets=",".join(markets),
            start_time=start_time, end_time=end_time,
            frequency=frequency,
            page_size=10000, verbose=False,
        )

    def get_candles(
        self,
        exchange: str,
        base: str,
        start_time: str,
        end_time: str,
        frequency: str = "1m",
        batch_size: int = 50,
        max_workers: int = 4,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取所有期货的 K 线数据

        Args:
            exchange: 交易所名称 (如 deribit, binance)
            base: 基础资产 (如 btc, eth)
            start_time: 开始时间 (ISO 8601)
            end_time: 结束时间 (ISO 8601)
            frequency: K 线频率 (1m/5m/10m/15m/30m/1h/4h/1d)，默认 1m
            batch_size: 每批请求的市场数量
            max_workers: 最大并发数
            verbose: 是否打印进度

        Returns:
            K 线数据 DataFrame
        """
        validate_time_range(start_time, end_time)

        valid_frequencies = {"1m", "5m", "10m", "15m", "30m", "1h", "4h", "1d"}
        if frequency not in valid_frequencies:
            raise ValidationError(f"frequency 必须是 {valid_frequencies} 之一")

        markets = self._fetch_markets(exchange, base, "future", verbose=False)
        if verbose:
            logger.info(f"[K 线] {exchange.upper()} {base.upper()} | {len(markets)} 个市场 | {frequency}")

        if not markets:
            return pd.DataFrame()

        df = self._fetch_all_concurrent(
            markets, start_time, end_time, batch_size, max_workers,
            self._fetch_candles_batch, "K 线", verbose, frequency=frequency
        )

        if not df.empty:
            metadata = self._get_market_metadata(exchange, base, "future")
            if not metadata.empty:
                df = pd.merge(df, metadata, on="market", how="left")
            df = df.sort_values(["market", "time"]).reset_index(drop=True)

        if verbose:
            logger.info(f"[K 线] 完成：{len(df)} 条")
        return df
