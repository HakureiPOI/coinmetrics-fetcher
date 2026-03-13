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
    │   ├── futures.py          # 期货数据获取器
    │   └── spot.py             # 现货数据获取器
    ├── base.py         # API 基类
    └── base_fetcher.py # 数据获取器基类
"""

# 自动初始化日志（在导入任何其他模块之前）
from utils.fetch_utils import setup_logging as _setup_logging
_setup_logging()

from .base import CoinMetricsAPI
from .base_fetcher import BaseFetcher
from .core import ReferenceDataAPI, TimeseriesAPI
from .fetchers import OptionsDataFetcher, OptionFilter, FundingRateFetcher, FuturesDataFetcher, SpotDataFetcher

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
    "SpotDataFetcher",
]
