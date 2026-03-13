"""
CoinMetrics Reference Data API 封装

提供参考数据接口的封装，包括资产、交易所、市场等元数据查询。
"""

import logging
from typing import Optional

import pandas as pd

from ..base import CoinMetricsAPI

logger = logging.getLogger(__name__)


class ReferenceDataAPI(CoinMetricsAPI):
    """
    Reference Data API 封装

    提供 CoinMetrics 参考数据接口的便捷访问方法。
    """

    def get_markets(
        self,
        exchange: Optional[str] = None,
        market_type: Optional[str] = None,
        base: Optional[str] = None,
        quote: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取市场元数据

        Args:
            exchange: 交易所名称
            market_type: 市场类型 (future/option)
            base: 基础资产
            quote: 计价货币
            page_size: 每页大小
            verbose: 是否打印进度

        Returns:
            市场元数据 DataFrame
        """
        params = {}
        if exchange:
            params["exchange"] = exchange
        if market_type:
            params["type"] = market_type
        if base:
            params["base"] = base
        if quote:
            params["quote"] = quote

        return self._request(
            endpoint="/reference-data/markets",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )
