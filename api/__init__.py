"""
CoinMetrics API v4 Python 客户端

目录结构:
    api/
    ├── core/           # 一级接口 - 直接调用 API 端点
    │   ├── reference_data.py   # /reference-data/markets
    │   └── timeseries.py       # /timeseries/* 端点
    ├── fetchers/       # 二级接口 - 基于一级接口的高级封装
    │   ├── options.py          # 期权数据获取器
    │   ├── funding_rates.py    # 资金费率获取器
    │   └── futures.py          # 期货数据获取器
    ├── base.py         # API 基类
    └── base_fetcher.py # 数据获取器基类
"""

from .base import CoinMetricsAPI
from .base_fetcher import BaseFetcher
from .core import ReferenceDataAPI, TimeseriesAPI
from .fetchers import OptionsDataFetcher, OptionFilter, FundingRateFetcher, FuturesDataFetcher

__all__ = [
    # 基础类
    "CoinMetricsAPI",
    "BaseFetcher",
    # 一级接口
    "ReferenceDataAPI",
    "TimeseriesAPI",
    # 二级接口
    "OptionsDataFetcher",
    "OptionFilter",
    "FundingRateFetcher",
    "FuturesDataFetcher",
]
