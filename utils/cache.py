"""
内存缓存模块

提供简单的内存缓存功能，用于缓存 API 响应数据。
"""

import hashlib
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class CacheEntry:
    """缓存条目"""

    def __init__(self, data: Any, ttl_seconds: int):
        self.data = data
        self.expires_at = datetime.now() + timedelta(seconds=ttl_seconds)

    def is_expired(self) -> bool:
        return datetime.now() > self.expires_at


class MemoryCache:
    """内存缓存"""

    def __init__(self, default_ttl: int = 300, max_size: int = 1000):
        """
        初始化内存缓存

        Args:
            default_ttl: 默认缓存时间 (秒)，默认 5 分钟
            max_size: 最大缓存条目数，默认 1000
        """
        self._cache: dict[str, CacheEntry] = {}
        self.default_ttl = default_ttl
        self.max_size = max_size
        self._hits = 0
        self._misses = 0

    def _generate_key(self, endpoint: str, params: dict[str, Any]) -> str:
        """生成缓存键"""
        key_data = {
            "endpoint": endpoint,
            "params": sorted(params.items()),
        }
        key_str = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.md5(key_str.encode()).hexdigest()

    def get(self, endpoint: str, params: dict[str, Any]) -> Optional[Any]:
        """
        从缓存获取数据

        Args:
            endpoint: API 端点
            params: 请求参数

        Returns:
            缓存的数据，如果不存在或已过期则返回 None
        """
        key = self._generate_key(endpoint, params)
        entry = self._cache.get(key)

        if entry is None:
            self._misses += 1
            return None

        if entry.is_expired():
            self._cache.pop(key, None)
            self._misses += 1
            return None

        self._hits += 1
        logger.debug(f"[Cache] HIT: {endpoint}")
        return entry.data

    def set(
        self, endpoint: str, params: dict[str, Any], data: Any, ttl: Optional[int] = None
    ) -> None:
        """
        设置缓存

        Args:
            endpoint: API 端点
            params: 请求参数
            data: 要缓存的数据
            ttl: 缓存时间 (秒)，None 表示使用默认值
        """
        # 清理过期条目
        self._cleanup()

        # 如果缓存已满，删除最早的条目
        if len(self._cache) >= self.max_size:
            oldest_key = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].expires_at,
            )
            self._cache.pop(oldest_key)

        key = self._generate_key(endpoint, params)
        ttl = ttl if ttl is not None else self.default_ttl
        self._cache[key] = CacheEntry(data, ttl)
        logger.debug(f"[Cache] SET: {endpoint}, ttl={ttl}s")

    def _cleanup(self) -> None:
        """清理过期条目"""
        expired_keys = [
            key for key, entry in self._cache.items() if entry.is_expired()
        ]
        for key in expired_keys:
            self._cache.pop(key)

    def clear(self) -> None:
        """清空缓存"""
        self._cache.clear()
        self._hits = 0
        self._misses = 0

    def stats(self) -> dict[str, Any]:
        """获取缓存统计信息"""
        total = self._hits + self._misses
        hit_rate = (self._hits / total * 100) if total > 0 else 0
        return {
            "size": len(self._cache),
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{hit_rate:.2f}%",
        }


# 全局缓存实例
_cache: Optional[MemoryCache] = None


def get_cache() -> MemoryCache:
    """获取全局缓存实例"""
    global _cache
    if _cache is None:
        _cache = MemoryCache()
    return _cache


def init_cache(default_ttl: int = 300, max_size: int = 1000) -> MemoryCache:
    """初始化全局缓存实例"""
    global _cache
    _cache = MemoryCache(default_ttl=default_ttl, max_size=max_size)
    return _cache


def cached_request(
    endpoint: str,
    params: dict[str, Any],
    fetch_func,
    cache: Optional[MemoryCache] = None,
    ttl: Optional[int] = None,
    use_cache: bool = True,
) -> Any:
    """
    带缓存的请求

    Args:
        endpoint: API 端点
        params: 请求参数
        fetch_func: 实际获取数据的函数
        cache: 缓存实例，None 表示使用全局缓存
        ttl: 缓存时间 (秒)
        use_cache: 是否使用缓存

    Returns:
        获取的数据
    """
    if not use_cache:
        return fetch_func()

    cache = cache or get_cache()
    data = cache.get(endpoint, params)

    if data is not None:
        return data

    data = fetch_func()
    cache.set(endpoint, params, data, ttl)
    return data
