"""
CoinMetrics Reference Data API 封装

提供参考数据接口的封装，包括资产、交易所、市场等元数据查询。
"""

import logging
from typing import Any, Optional, Union

import pandas as pd

from .base import CoinMetricsAPI

logger = logging.getLogger(__name__)


class ReferenceDataAPI(CoinMetricsAPI):
    """
    Reference Data API 封装

    提供 CoinMetrics 参考数据接口的便捷访问方法。
    """

    def get_assets(
        self,
        asset_id: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取资产元数据

        Args:
            asset_id: 资产 ID，逗号分隔多个 ID
            page_size: 每页大小
            verbose: 是否打印进度

        Returns:
            资产元数据 DataFrame
        """
        params = {}
        if asset_id:
            params["assets"] = asset_id

        return self._request(
            endpoint="/reference-data/assets",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_exchanges(
        self,
        exchange_id: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取交易所元数据

        Args:
            exchange_id: 交易所 ID，逗号分隔多个 ID
            page_size: 每页大小
            verbose: 是否打印进度

        Returns:
            交易所元数据 DataFrame
        """
        params = {}
        if exchange_id:
            params["exchanges"] = exchange_id

        return self._request(
            endpoint="/reference-data/exchanges",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_markets(
        self,
        exchange: Optional[str] = None,
        market_type: Optional[str] = None,
        base: Optional[str] = None,
        quote: Optional[str] = None,
        asset: Optional[str] = None,
        symbol: Optional[str] = None,
        status: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取市场元数据

        Args:
            exchange: 交易所名称
            market_type: 市场类型 (spot/future/option)
            base: 基础资产
            quote: 计价货币
            asset: 任意一方资产
            symbol: 交易对符号
            status: 状态 (online/offline)
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
        if asset:
            params["asset"] = asset
        if symbol:
            params["symbol"] = symbol
        if status:
            params["status"] = status

        return self._request(
            endpoint="/reference-data/markets",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_indexes(
        self,
        index_id: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取指数元数据

        Args:
            index_id: 指数 ID
            page_size: 每页大小
            verbose: 是否打印进度

        Returns:
            指数元数据 DataFrame
        """
        params = {}
        if index_id:
            params["indexes"] = index_id

        return self._request(
            endpoint="/reference-data/indexes",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_pairs(
        self,
        pair_id: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取交易对元数据

        Args:
            pair_id: 交易对 ID
            page_size: 每页大小
            verbose: 是否打印进度

        Returns:
            交易对元数据 DataFrame
        """
        params = {}
        if pair_id:
            params["pairs"] = pair_id

        return self._request(
            endpoint="/reference-data/pairs",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_asset_metrics(
        self,
        metric: Optional[str] = None,
        reviewable: Optional[bool] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取资产指标元数据

        Args:
            metric: 指标名称
            reviewable: 是否可审查
            page_size: 每页大小
            verbose: 是否打印进度

        Returns:
            资产指标元数据 DataFrame
        """
        params = {}
        if metric:
            params["metrics"] = metric
        if reviewable is not None:
            params["reviewable"] = str(reviewable).lower()

        return self._request(
            endpoint="/reference-data/asset-metrics",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_exchange_metrics(
        self,
        metric: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取交易所指标元数据
        """
        params = {}
        if metric:
            params["metrics"] = metric

        return self._request(
            endpoint="/reference-data/exchange-metrics",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_exchange_asset_metrics(
        self,
        metric: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取交易所资产指标元数据
        """
        params = {}
        if metric:
            params["metrics"] = metric

        return self._request(
            endpoint="/reference-data/exchange-asset-metrics",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_exchange_pair_metrics(
        self,
        metric: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取交易所交易对指标元数据
        """
        params = {}
        if metric:
            params["metrics"] = metric

        return self._request(
            endpoint="/reference-data/exchange-pair-metrics",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_pair_metrics(
        self,
        metric: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取交易对指标元数据
        """
        params = {}
        if metric:
            params["metrics"] = metric

        return self._request(
            endpoint="/reference-data/pair-metrics",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_institution_metrics(
        self,
        metric: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取机构指标元数据
        """
        params = {}
        if metric:
            params["metrics"] = metric

        return self._request(
            endpoint="/reference-data/institution-metrics",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )

    def get_market_metrics(
        self,
        metric: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取市场指标元数据
        """
        params = {}
        if metric:
            params["metrics"] = metric

        return self._request(
            endpoint="/reference-data/market-metrics",
            params=params,
            page_size=page_size,
            verbose=verbose,
        )
