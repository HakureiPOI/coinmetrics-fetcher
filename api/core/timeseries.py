"""
CoinMetrics Timeseries API 封装

提供时间序列接口的封装，包括资产指标、市场数据等时间序列查询。
"""

import logging
from typing import Optional

import pandas as pd

from ..cached_api import CachedCoinMetricsAPI

logger = logging.getLogger(__name__)


class TimeseriesAPI(CachedCoinMetricsAPI):
    """
    Timeseries API 封装

    提供 CoinMetrics 时间序列接口的便捷访问方法。
    """

    def __init__(
        self,
        config=None,
        session=None,
        use_cache: bool = True,
        cache: Optional[dict] = None,
        cache_ttl: int = 300,
        use_community_api: bool = False,
    ):
        """
        初始化 TimeseriesAPI

        Args:
            config: 配置对象
            session: requests Session
            use_cache: 是否启用缓存，默认 True
            cache: 缓存实例，None 表示使用全局缓存
            cache_ttl: 缓存时间 (秒)，默认 300 秒 (5 分钟)
            use_community_api: 是否使用社区版 API，默认 False
        """
        super().__init__(
            config=config,
            session=session,
            use_cache=use_cache,
            cache=cache,
            cache_ttl=cache_ttl,
            use_community_api=use_community_api,
        )

    def get_market_candles(
        self,
        markets: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        frequency: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
        use_cache: Optional[bool] = None,
    ) -> pd.DataFrame:
        """
        获取市场 K 线数据

        Args:
            markets: 市场标识符，逗号分隔多个
            start_time: 开始时间（包含）
            end_time: 结束时间（不包含，左闭右开）
            frequency: K 线频率 (1m, 5m, 10m, 15m, 30m, 1h, 4h, 1d)
            page_size: 每页大小
            verbose: 是否打印进度
            use_cache: 是否使用缓存，None 表示使用实例默认值

        Returns:
            K 线数据 DataFrame
        """
        params = self._build_params(
            markets=markets, start_time=start_time, end_time=end_time,
            frequency=frequency, page_size=page_size, end_inclusive=False
        )

        def fetch() -> pd.DataFrame:
            return self._request(
                endpoint="/timeseries/market-candles",
                params=params,
                page_size=page_size,
                verbose=verbose,
            )

        return self._cached_request("/timeseries/market-candles", params, fetch, use_cache)

    def get_market_greeks(
        self,
        markets: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        granularity: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
        use_cache: Optional[bool] = None,
    ) -> pd.DataFrame:
        """
        获取期权 Greeks 数据

        Args:
            markets: 市场标识符，逗号分隔多个
            start_time: 开始时间（包含）
            end_time: 结束时间（不包含，左闭右开）
            granularity: 数据粒度 (如 1m, 1h, 1d)
            page_size: 每页大小
            verbose: 是否打印进度
            use_cache: 是否使用缓存，None 表示使用实例默认值

        Returns:
            Greeks 数据 DataFrame
        """
        params = self._build_params(
            markets=markets, start_time=start_time, end_time=end_time,
            granularity=granularity, page_size=page_size, end_inclusive=False
        )

        def fetch() -> pd.DataFrame:
            return self._request(
                endpoint="/timeseries/market-greeks",
                params=params,
                page_size=page_size,
                verbose=verbose,
            )

        return self._cached_request("/timeseries/market-greeks", params, fetch, use_cache)

    def get_market_implied_volatility(
        self,
        markets: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        granularity: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
        use_cache: Optional[bool] = None,
    ) -> pd.DataFrame:
        """
        获取期权隐含波动率 (IV) 数据

        Args:
            markets: 市场标识符，逗号分隔多个
            start_time: 开始时间（包含）
            end_time: 结束时间（不包含，左闭右开）
            granularity: 数据粒度 (如 1m, 1h, 1d)
            page_size: 每页大小
            verbose: 是否打印进度
            use_cache: 是否使用缓存，None 表示使用实例默认值

        Returns:
            IV 数据 DataFrame
        """
        params = self._build_params(
            markets=markets, start_time=start_time, end_time=end_time,
            granularity=granularity, page_size=page_size, end_inclusive=False
        )

        def fetch() -> pd.DataFrame:
            return self._request(
                endpoint="/timeseries/market-implied-volatility",
                params=params,
                page_size=page_size,
                verbose=verbose,
            )

        return self._cached_request("/timeseries/market-implied-volatility", params, fetch, use_cache)

    def get_market_funding_rates(
        self,
        markets: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
        use_cache: Optional[bool] = None,
    ) -> pd.DataFrame:
        """
        获取永续合约资金费率数据

        Args:
            markets: 市场标识符，逗号分隔多个
            start_time: 开始时间（包含）
            end_time: 结束时间（不包含，左闭右开）
            page_size: 每页大小
            verbose: 是否打印进度
            use_cache: 是否使用缓存，None 表示使用实例默认值

        Returns:
            资金费率数据 DataFrame
        """
        params = self._build_params(
            markets=markets, start_time=start_time, end_time=end_time,
            page_size=page_size, end_inclusive=False
        )

        def fetch() -> pd.DataFrame:
            return self._request(
                endpoint="/timeseries/market-funding-rates",
                params=params,
                page_size=page_size,
                verbose=verbose,
            )

        return self._cached_request("/timeseries/market-funding-rates", params, fetch, use_cache)

    def get_market_funding_rates_predicted(
        self,
        markets: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
        use_cache: Optional[bool] = None,
    ) -> pd.DataFrame:
        """
        获取永续合约预计资金费率数据

        Args:
            markets: 市场标识符，逗号分隔多个
            start_time: 开始时间（包含）
            end_time: 结束时间（不包含，左闭右开）
            page_size: 每页大小
            verbose: 是否打印进度
            use_cache: 是否使用缓存，None 表示使用实例默认值

        Returns:
            预计资金费率数据 DataFrame
        """
        params = self._build_params(
            markets=markets, start_time=start_time, end_time=end_time,
            page_size=page_size, end_inclusive=False
        )

        def fetch() -> pd.DataFrame:
            return self._request(
                endpoint="/timeseries/market-funding-rates-predicted",
                params=params,
                page_size=page_size,
                verbose=verbose,
            )

        return self._cached_request("/timeseries/market-funding-rates-predicted", params, fetch, use_cache)

    @staticmethod
    def _build_params(**kwargs) -> dict:
        """构建请求参数，过滤 None 值"""
        return {k: v for k, v in kwargs.items() if v is not None}
