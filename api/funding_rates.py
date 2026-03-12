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
    """
    资金费率数据获取器

    提供获取永续合约资金费率和预计资金费率的高级接口。
    """

    def _fetch_perpetual_markets(self, exchange: str, base: str) -> list[str]:
        """获取永续合约市场列表"""
        df = self.ref_api.get_markets(
            exchange=exchange,
            market_type="future",
            base=base,
            verbose=False,
        )
        perpetuals = df[df["symbol"].str.contains("PERPETUAL", case=True, na=False)]
        return perpetuals["market"].tolist()

    def _fetch_funding_rates_batch(
        self, markets: list[str], start_time: str, end_time: str
    ) -> pd.DataFrame:
        """批量获取资金费率数据"""
        return self.ts_api.get_market_funding_rates(
            markets=",".join(markets),
            start_time=start_time,
            end_time=end_time,
            page_size=10000,
            verbose=False,
        )

    def _fetch_predicted_rates_batch(
        self, markets: list[str], start_time: str, end_time: str
    ) -> pd.DataFrame:
        """批量获取预计资金费率数据"""
        return self.ts_api.get_market_funding_rates_predicted(
            markets=",".join(markets),
            start_time=start_time,
            end_time=end_time,
            page_size=10000,
            verbose=False,
        )

    def _get_market_metadata(self, exchange: str, base: str) -> pd.DataFrame:
        """获取市场元数据"""
        return self.ref_api.get_markets(
            exchange=exchange,
            market_type="future",
            base=base,
            verbose=False,
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
        """
        获取永续合约资金费率数据

        Args:
            exchange: 交易所名称 (如 deribit, binance)
            base: 基础资产 (如 btc, eth)
            start_time: 开始时间 (ISO 8601)
            end_time: 结束时间 (ISO 8601)
            batch_size: 每批请求的市场数量
            max_workers: 最大并发数
            verbose: 是否打印进度

        Returns:
            资金费率数据 DataFrame
        """
        validate_time_range(start_time, end_time)

        if verbose:
            logger.info("=" * 60)
            logger.info(f"获取 {exchange.upper()} {base.upper()} 永续合约资金费率")
            logger.info(f"时间范围：{start_time} 至 {end_time}")

        markets = self._fetch_perpetual_markets(exchange, base)
        if verbose:
            logger.info(f"找到 {len(markets)} 个永续合约")

        if not markets:
            return pd.DataFrame()

        df = self._fetch_all_concurrent(
            markets, start_time, end_time, batch_size, max_workers,
            self._fetch_funding_rates_batch, "资金费率", verbose
        )

        if len(df) > 0:
            df = pd.merge(df, self._get_market_metadata(exchange, base), on="market", how="left")

        if verbose:
            logger.info(f"完成，共 {len(df)} 条记录")
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
        """
        获取永续合约预计资金费率数据

        Args:
            exchange: 交易所名称
            base: 基础资产
            start_time: 开始时间 (ISO 8601)
            end_time: 结束时间 (ISO 8601)
            batch_size: 每批请求的市场数量
            max_workers: 最大并发数
            verbose: 是否打印进度

        Returns:
            预计资金费率数据 DataFrame
        """
        validate_time_range(start_time, end_time)

        if verbose:
            logger.info("=" * 60)
            logger.info(f"获取 {exchange.upper()} {base.upper()} 永续合约预计资金费率")
            logger.info(f"时间范围：{start_time} 至 {end_time}")

        markets = self._fetch_perpetual_markets(exchange, base)
        if verbose:
            logger.info(f"找到 {len(markets)} 个永续合约")

        if not markets:
            return pd.DataFrame()

        df = self._fetch_all_concurrent(
            markets, start_time, end_time, batch_size, max_workers,
            self._fetch_predicted_rates_batch, "预计资金费率", verbose
        )

        if len(df) > 0:
            df = pd.merge(df, self._get_market_metadata(exchange, base), on="market", how="left")

        if verbose:
            logger.info(f"完成，共 {len(df)} 条记录")
        return df


def get_funding_rates(
    exchange: str,
    base: str,
    start_time: str,
    end_time: str,
    output_path: Optional[str] = None,
    batch_size: int = 50,
    max_workers: int = 4,
    verbose: bool = True,
) -> pd.DataFrame:
    """便捷函数：获取永续合约资金费率数据"""
    fetcher = FundingRateFetcher()
    df = fetcher.get_funding_rates(
        exchange=exchange, base=base, start_time=start_time, end_time=end_time,
        batch_size=batch_size, max_workers=max_workers, verbose=verbose
    )
    if output_path:
        fetcher.save_to_csv(df, output_path)
    return df