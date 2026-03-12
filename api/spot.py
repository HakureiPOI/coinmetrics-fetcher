"""
CoinMetrics 现货数据获取模块
"""

import logging
from typing import Optional

import pandas as pd

from api.base_fetcher import BaseFetcher
from utils import validate_time_range

logger = logging.getLogger(__name__)


class SpotDataFetcher(BaseFetcher):
    """现货数据获取器"""

    def _fetch_spot_markets(self, exchange: str, base: str) -> list[str]:
        """获取现货市场列表"""
        df = self.ref_api.get_markets(
            exchange=exchange, market_type="spot", base=base, verbose=False
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
            exchange=exchange, market_type="spot", base=base, verbose=False
        )[["market", "symbol", "pair"]]

    def get_candles(
        self,
        exchange: str,
        base: str,
        start_time: str,
        end_time: str,
        quote: Optional[str] = None,
        batch_size: int = 50,
        max_workers: int = 4,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取现货分钟级 K 线数据

        Args:
            exchange: 交易所名称 (如 deribit, binance)
            base: 基础资产 (如 btc, eth)
            start_time: 开始时间 (ISO 8601)
            end_time: 结束时间 (ISO 8601)
            quote: 计价货币 (如 usd, usdt)，None 表示全部
            batch_size: 每批请求的市场数量
            max_workers: 最大并发数
            verbose: 是否打印进度

        Returns:
            K 线数据 DataFrame，包含 market, time, open, high, low, close, volume, symbol, pair
        """
        validate_time_range(start_time, end_time)

        # 获取现货市场列表
        df = self.ref_api.get_markets(
            exchange=exchange, market_type="spot", base=base, quote=quote, verbose=False
        )
        markets = df["market"].tolist()

        if verbose:
            logger.info(f"[现货K线] {exchange.upper()} {base.upper()} | {len(markets)} 个市场")

        if not markets:
            return pd.DataFrame()

        df = self._fetch_all_concurrent(
            markets, start_time, end_time, batch_size, max_workers,
            self._fetch_candles_batch, "K线", verbose
        )

        if len(df) > 0:
            df = pd.merge(df, self._get_market_metadata(exchange, base), on="market", how="left")
            df = df.sort_values(["market", "time"]).reset_index(drop=True)

        if verbose:
            logger.info(f"[现货K线] 完成: {len(df)} 条")
        return df