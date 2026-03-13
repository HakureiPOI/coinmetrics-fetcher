"""
CoinMetrics Timeseries API 封装

提供时间序列接口的封装，包括资产指标、市场数据等时间序列查询。
"""

import logging
from typing import Optional

import pandas as pd

from ..base import CoinMetricsAPI

logger = logging.getLogger(__name__)


class TimeseriesAPI(CoinMetricsAPI):
    """
    Timeseries API 封装

    提供 CoinMetrics 时间序列接口的便捷访问方法。
    """

    def get_market_candles(
        self,
        markets: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        frequency: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取市场 K 线数据

        Args:
            markets: 市场标识符，逗号分隔多个
            start_time: 开始时间
            end_time: 结束时间
            frequency: K 线频率 (1m, 5m, 10m, 15m, 30m, 1h, 4h, 1d)
            page_size: 每页大小
            verbose: 是否打印进度
        """
        params = {"markets": markets}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if frequency:
            params["frequency"] = frequency

        return self._request(
            endpoint="/timeseries/market-candles",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_market_greeks(
        self,
        markets: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        granularity: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取期权 Greeks 数据

        Args:
            markets: 市场标识符，逗号分隔多个
            start_time: 开始时间
            end_time: 结束时间
            granularity: 数据粒度 (如 1m, 1h, 1d)
            page_size: 每页大小
            verbose: 是否打印进度

        Returns:
            Greeks 数据 DataFrame，包含 delta, gamma, theta, vega, rho 等字段
        """
        params = {"markets": markets}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if granularity:
            params["granularity"] = granularity

        return self._request(
            endpoint="/timeseries/market-greeks",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_market_implied_volatility(
        self,
        markets: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        granularity: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取期权隐含波动率 (IV) 数据

        Args:
            markets: 市场标识符，逗号分隔多个
            start_time: 开始时间
            end_time: 结束时间
            granularity: 数据粒度 (如 1m, 1h, 1d)
            page_size: 每页大小
            verbose: 是否打印进度

        Returns:
            IV 数据 DataFrame，包含 iv_mark, iv_trade, iv_bid, iv_ask 等字段
        """
        params = {"markets": markets}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if granularity:
            params["granularity"] = granularity

        return self._request(
            endpoint="/timeseries/market-implied-volatility",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_market_funding_rates(
        self,
        markets: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取永续合约资金费率数据

        Args:
            markets: 市场标识符，逗号分隔多个（如 deribit-BTC-PERPETUAL）
            start_time: 开始时间
            end_time: 结束时间
            page_size: 每页大小
            verbose: 是否打印进度

        Returns:
            资金费率数据 DataFrame，包含 funding_rate 等字段
        """
        params = {"markets": markets}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time

        return self._request(
            endpoint="/timeseries/market-funding-rates",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_market_funding_rates_predicted(
        self,
        markets: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取永续合约预计资金费率数据

        Args:
            markets: 市场标识符，逗号分隔多个（如 deribit-BTC-PERPETUAL）
            start_time: 开始时间
            end_time: 结束时间
            page_size: 每页大小
            verbose: 是否打印进度

        Returns:
            预计资金费率数据 DataFrame，包含 predicted_funding_rate 等字段
        """
        params = {"markets": markets}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time

        return self._request(
            endpoint="/timeseries/market-funding-rates-predicted",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )
