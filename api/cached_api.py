"""
CoinMetrics 带缓存的 API 基类
"""

import logging
from typing import Any, Optional

from .base import CoinMetricsAPI
from utils import MemoryCache, get_cache

logger = logging.getLogger(__name__)


class CachedCoinMetricsAPI(CoinMetricsAPI):
    """带缓存的 CoinMetrics API 基类"""

    def __init__(
        self,
        config=None,
        session=None,
        use_cache: bool = True,
        cache: Optional[MemoryCache] = None,
        cache_ttl: int = 300,
        use_community_api: bool = False,
    ):
        """
        初始化带缓存的 API

        Args:
            config: 配置对象
            session: requests Session
            use_cache: 是否启用缓存，默认 True
            cache: 缓存实例，None 表示使用全局缓存
            cache_ttl: 缓存时间 (秒)
            use_community_api: 是否使用社区版 API，默认 False
        """
        # 如果使用社区版 API，创建临时配置
        if use_community_api and (config is None or not config.use_community_api):
            from config import Config, COMMUNITY_BASE_URL
            config = Config(
                api_key="",
                base_url=COMMUNITY_BASE_URL,
                use_community_api=True,
            )

        super().__init__(config, session)
        self.use_cache = use_cache
        self.cache = cache or get_cache()
        self.cache_ttl = cache_ttl
        self.use_community_api = use_community_api or (config and config.use_community_api)

    def _cached_request(
        self,
        endpoint: str,
        params: dict[str, Any],
        fetch_func,
        use_cache: Optional[bool] = None,
    ) -> Any:
        """带缓存的请求

        Args:
            endpoint: API 端点
            params: 请求参数
            fetch_func: 获取数据的函数
            use_cache: 是否使用缓存，None 表示使用实例默认值

        Returns:
            缓存的数据或新获取的数据
        """
        use_cache_flag = use_cache if use_cache is not None else self.use_cache

        if not use_cache_flag:
            return fetch_func()

        cached = self.cache.get(endpoint, params)
        if cached is not None:
            logger.debug(f"[Cache HIT] {endpoint}")
            return cached

        logger.debug(f"[Cache MISS] {endpoint}")
        data = fetch_func()
        self.cache.set(endpoint, params, data, self.cache_ttl)
        return data
