"""
CoinMetrics 数据获取器基类

提供通用的并发获取、保存和资源管理功能。
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import pandas as pd

from .core import ReferenceDataAPI, TimeseriesAPI
from config import Config, get_config
from utils import BatchFetchError, MemoryCache

logger = logging.getLogger(__name__)


class BaseFetcher:
    """数据获取器基类"""

    def __init__(
        self,
        config: Optional[Config] = None,
        use_cache: bool = True,
        cache: Optional[MemoryCache] = None,
        ref_cache_ttl: int = 3600,
        ts_cache_ttl: int = 300,
    ):
        """
        初始化 BaseFetcher

        Args:
            config: 配置对象
            use_cache: 是否启用缓存，默认 True
            cache: 缓存实例，None 表示使用全局缓存
            ref_cache_ttl: ReferenceDataAPI 缓存时间 (秒)，默认 3600
            ts_cache_ttl: TimeseriesAPI 缓存时间 (秒)，默认 300
        """
        self.config = config or get_config()
        self.ref_api = ReferenceDataAPI(
            config=self.config,
            use_cache=use_cache,
            cache=cache,
            cache_ttl=ref_cache_ttl,
        )
        self.ts_api = TimeseriesAPI(
            config=self.config,
            use_cache=use_cache,
            cache=cache,
            cache_ttl=ts_cache_ttl,
        )

    def _fetch_all_concurrent(
        self,
        markets: list[str],
        start_time: str,
        end_time: str,
        batch_size: int,
        max_workers: int,
        fetch_func,
        data_type: str,
        verbose: bool = True,
        **kwargs,
    ) -> pd.DataFrame:
        """并发获取所有数据"""
        all_dfs = []
        total_batches = (len(markets) + batch_size - 1) // batch_size
        errors: list[tuple[int, Exception]] = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for i in range(0, len(markets), batch_size):
                batch = markets[i : i + batch_size]
                batch_num = i // batch_size + 1
                future = executor.submit(fetch_func, batch, start_time, end_time, **kwargs)
                futures[future] = batch_num

            for future in as_completed(futures):
                batch_num = futures[future]
                try:
                    df = future.result()
                    if len(df) > 0:
                        all_dfs.append(df)
                    if verbose:
                        logger.info(f"[{data_type}] {batch_num}/{total_batches}")
                except Exception as e:
                    logger.error(f"[{data_type}] 批次 {batch_num} 失败: {e}")
                    errors.append((batch_num, e))

        if errors:
            raise BatchFetchError(errors)

        if all_dfs:
            return pd.concat(all_dfs, ignore_index=True)
        return pd.DataFrame()

    def save_to_csv(self, df: pd.DataFrame, output_path: str, overwrite: bool = True) -> str:
        """保存 DataFrame 到 CSV 文件"""
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        if not overwrite and os.path.exists(output_path):
            raise FileExistsError(f"文件已存在：{output_path}")

        df.to_csv(output_path, index=False)
        logger.info(f"已保存: {output_path} ({len(df)} 条)")
        return output_path

    def close(self) -> None:
        """关闭底层 API Sessions"""
        self.ref_api.close()
        self.ts_api.close()

    def __enter__(self) -> "BaseFetcher":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()