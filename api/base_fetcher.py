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
        use_community_api: bool = False,
    ):
        """
        初始化 BaseFetcher

        Args:
            config: 配置对象
            use_cache: 是否启用缓存，默认 True
            cache: 缓存实例，None 表示使用全局缓存
            ref_cache_ttl: ReferenceDataAPI 缓存时间 (秒)，默认 3600
            ts_cache_ttl: TimeseriesAPI 缓存时间 (秒)，默认 300
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
        """并发获取所有数据
        
        Args:
            markets: 市场列表
            start_time: 开始时间
            end_time: 结束时间
            batch_size: 每批市场数量
            max_workers: 最大并发数
            fetch_func: 获取函数，签名应为 (markets, start_time, end_time, **kwargs)
            data_type: 数据类型标识（用于日志）
            verbose: 是否打印进度
            **kwargs: 额外参数传递给 fetch_func（如 frequency, granularity）
        """
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
                    logger.error(f"[{data_type}] 批次 {batch_num} 失败：{e}")
                    errors.append((batch_num, e))

        if errors:
            raise BatchFetchError(errors)

        if all_dfs:
            return pd.concat(all_dfs, ignore_index=True)
        return pd.DataFrame()

    def _fetch_markets(self, exchange: str, base: str, market_type: str, verbose: bool = False) -> list[str]:
        """获取市场列表
        
        Args:
            exchange: 交易所名称
            base: 基础资产
            market_type: 市场类型 (future/option)
            verbose: 是否打印日志
            
        Returns:
            市场标识符列表
        """
        df = self.ref_api.get_markets(
            exchange=exchange, type=market_type, base=base, verbose=verbose
        )
        return df["market"].tolist()

    def _get_market_metadata(self, exchange: str, base: str, market_type: str) -> pd.DataFrame:
        """获取市场元数据
        
        Args:
            exchange: 交易所名称
            base: 基础资产
            market_type: 市场类型 (future/option)
            
        Returns:
            包含 market, symbol, pair 的 DataFrame
        """
        return self.ref_api.get_markets(
            exchange=exchange, type=market_type, base=base, verbose=False
        )[["market", "symbol", "pair"]]

    def save_to_csv(self, df: pd.DataFrame, output_path: str, overwrite: bool = True) -> str:
        """保存 DataFrame 到 CSV 文件"""
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        if not overwrite and os.path.exists(output_path):
            raise FileExistsError(f"文件已存在：{output_path}")

        df.to_csv(output_path, index=False)
        logger.info(f"已保存：{output_path} ({len(df)} 条)")
        return output_path

    def close(self) -> None:
        """关闭底层 API Sessions"""
        self.ref_api.close()
        self.ts_api.close()

    def __enter__(self) -> "BaseFetcher":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
