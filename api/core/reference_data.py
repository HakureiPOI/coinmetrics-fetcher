"""
CoinMetrics Reference Data API 封装

提供参考数据接口的封装，包括资产、交易所、市场等元数据查询。
"""

import logging
from typing import Any, Optional

import pandas as pd

from ..base import CoinMetricsAPI
from utils import get_cache, MemoryCache

logger = logging.getLogger(__name__)


class ReferenceDataAPI(CoinMetricsAPI):
    """
    Reference Data API 封装

    提供 CoinMetrics 参考数据接口的便捷访问方法。
    """

    def __init__(
        self,
        config=None,
        session=None,
        use_cache: bool = True,
        cache: Optional[MemoryCache] = None,
        cache_ttl: int = 3600,
        use_community_api: bool = False,
    ):
        """
        初始化 ReferenceDataAPI

        Args:
            config: 配置对象
            session: requests Session
            use_cache: 是否启用缓存，默认 True
            cache: 缓存实例，None 表示使用全局缓存
            cache_ttl: 缓存时间 (秒)，默认 3600 秒 (1 小时)
            use_community_api: 是否使用社区版 API，默认 False
        """
        # 如果使用社区版 API，创建临时配置
        if use_community_api and (config is None or not config.use_community_api):
            from config import Config, COMMUNITY_BASE_URL
            config = Config(
                api_key="",  # 社区版不需要 key
                base_url=COMMUNITY_BASE_URL,
                use_community_api=True,
            )
        
        super().__init__(config, session)
        self.use_cache = use_cache
        self.cache = cache or get_cache()
        self.cache_ttl = cache_ttl
        self.use_community_api = use_community_api or (config and config.use_community_api)

    def get_markets(
        self,
        exchange: Optional[str] = None,
        type: Optional[str] = None,
        base: Optional[str] = None,
        quote: Optional[str] = None,
        asset: Optional[str] = None,
        symbol: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
        use_cache: Optional[bool] = None,
    ) -> pd.DataFrame:
        """
        获取市场元数据

        Args:
            exchange: 交易所名称
            type: 市场类型 (spot/future/option)
            base: 基础资产
            quote: 计价货币
            asset: 任意一方资产
            symbol: 交易对符号
            page_size: 每页大小
            verbose: 是否打印进度
            use_cache: 是否使用缓存，None 表示使用实例默认值

        Returns:
            市场元数据 DataFrame
        """
        params = {}
        if exchange:
            params["exchange"] = exchange
        if type:
            params["type"] = type
        if base:
            params["base"] = base
        if quote:
            params["quote"] = quote
        if asset:
            params["asset"] = asset
        if symbol:
            params["symbol"] = symbol
        if page_size:
            params["page_size"] = page_size

        use_cache_flag = use_cache if use_cache is not None else self.use_cache

        def fetch() -> pd.DataFrame:
            return self._request(
                endpoint="/reference-data/markets",
                params=params,
                page_size=page_size,
                verbose=verbose,
                data_key="data",
                next_page_key="next_page_url",
            )

        if use_cache_flag:
            return self._cached_request("/reference-data/markets", params, fetch)
        return fetch()

    def _cached_request(
        self,
        endpoint: str,
        params: dict[str, Any],
        fetch_func,
    ) -> Any:
        """带缓存的请求"""
        cached = self.cache.get(endpoint, params)
        if cached is not None:
            return cached
        data = fetch_func()
        self.cache.set(endpoint, params, data, self.cache_ttl)
        return data
