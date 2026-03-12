"""
CoinMetrics Timeseries API 封装

提供时间序列接口的封装，包括资产指标、市场数据等时间序列查询。
"""

import logging
from typing import Optional

import pandas as pd

from .base import CoinMetricsAPI

logger = logging.getLogger(__name__)


class TimeseriesAPI(CoinMetricsAPI):
    """
    Timeseries API 封装

    提供 CoinMetrics 时间序列接口的便捷访问方法。
    """

    def get_asset_metrics(
        self,
        assets: str,
        metrics: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        frequency: Optional[str] = None,
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取资产指标时间序列数据

        Args:
            assets: 资产 ID，逗号分隔多个
            metrics: 指标名称，逗号分隔多个
            start_time: 开始时间 (ISO 8601)
            end_time: 结束时间 (ISO 8601)
            frequency: 频率 (1d, 1h 等)
            sort: 排序方式 (asset, time)
            limit: 限制返回记录数
            page_size: 每页大小
            verbose: 是否打印进度

        Returns:
            时间序列数据 DataFrame
        """
        params = {
            "assets": assets,
            "metrics": metrics,
        }
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if frequency:
            params["frequency"] = frequency
        if sort:
            params["sort"] = sort
        if limit:
            params["limit"] = limit

        return self._request(
            endpoint="/timeseries/asset-metrics",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_exchange_metrics(
        self,
        exchanges: str,
        metrics: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        frequency: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取交易所指标时间序列数据
        """
        params = {
            "exchanges": exchanges,
            "metrics": metrics,
        }
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if frequency:
            params["frequency"] = frequency

        return self._request(
            endpoint="/timeseries/exchange-metrics",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_market_metrics(
        self,
        markets: str,
        metrics: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        frequency: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取市场指标时间序列数据
        """
        params = {
            "markets": markets,
            "metrics": metrics,
        }
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if frequency:
            params["frequency"] = frequency

        return self._request(
            endpoint="/timeseries/market-metrics",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_pair_metrics(
        self,
        pairs: str,
        metrics: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        frequency: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取交易对指标时间序列数据
        """
        params = {
            "pairs": pairs,
            "metrics": metrics,
        }
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if frequency:
            params["frequency"] = frequency

        return self._request(
            endpoint="/timeseries/pair-metrics",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_pair_candles(
        self,
        pairs: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取交易对 K 线数据
        """
        params = {"pairs": pairs}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time

        return self._request(
            endpoint="/timeseries/pair-candles",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_market_candles(
        self,
        markets: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        granularity: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取市场 K 线数据

        Args:
            markets: 市场标识符，逗号分隔多个
            start_time: 开始时间
            end_time: 结束时间
            granularity: 数据粒度 (如 1m, 5m, 15m, 30m, 1h, 4h, 1d)
            page_size: 每页大小
            verbose: 是否打印进度
        """
        params = {"markets": markets}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if granularity:
            params["granularity"] = granularity

        return self._request(
            endpoint="/timeseries/market-candles",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_market_trades(
        self,
        markets: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: Optional[int] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取市场交易数据
        """
        params = {"markets": markets}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if limit:
            params["limit"] = limit

        return self._request(
            endpoint="/timeseries/market-trades",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_market_orderbooks(
        self,
        markets: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: Optional[int] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取市场订单簿数据
        """
        params = {"markets": markets}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if limit:
            params["limit"] = limit

        return self._request(
            endpoint="/timeseries/market-orderbooks",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_market_quotes(
        self,
        markets: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: Optional[int] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取市场报价数据
        """
        params = {"markets": markets}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if limit:
            params["limit"] = limit

        return self._request(
            endpoint="/timeseries/market-quotes",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_market_liquidations(
        self,
        markets: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取市场清算数据
        """
        params = {"markets": markets}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time

        return self._request(
            endpoint="/timeseries/market-liquidations",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_index_levels(
        self,
        indexes: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取指数点位数据
        """
        params = {"indexes": indexes}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time

        return self._request(
            endpoint="/timeseries/index-levels",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_index_candles(
        self,
        indexes: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取指数 K 线数据
        """
        params = {"indexes": indexes}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time

        return self._request(
            endpoint="/timeseries/index-candles",
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
