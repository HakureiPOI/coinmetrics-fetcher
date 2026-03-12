"""
CoinMetrics API v4 Python 客户端
"""

from .base import CoinMetricsAPI
from .base_fetcher import BaseFetcher
from .reference_data import ReferenceDataAPI
from .timeseries import TimeseriesAPI
from .options import OptionsDataFetcher, OptionFilter
from .funding_rates import FundingRateFetcher
from .futures import FuturesDataFetcher
from .spot import SpotDataFetcher

__all__ = [
    # 基础类
    "CoinMetricsAPI",
    "BaseFetcher",
    # 参考数据
    "ReferenceDataAPI",
    # 时间序列
    "TimeseriesAPI",
    # 期权数据
    "OptionsDataFetcher",
    "OptionFilter",
    # 资金费率
    "FundingRateFetcher",
    # 期货数据
    "FuturesDataFetcher",
    # 现货数据
    "SpotDataFetcher",
]