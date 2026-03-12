"""
CoinMetrics 期货数据获取模块

提供期货市场 K 线数据的便捷获取功能。
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import pandas as pd

from api.reference_data import ReferenceDataAPI
from api.timeseries import TimeseriesAPI
from config import Config, get_config
from utils import BatchFetchError, validate_time_range

logger = logging.getLogger(__name__)


class FuturesDataFetcher:
    """
    期货数据获取器

    提供获取期货市场 K 线数据的高级接口。

    Example:
        >>> fetcher = FuturesDataFetcher()
        >>> df = fetcher.get_candles(
        ...     exchange="deribit",
        ...     base="btc",
        ...     start_time="2024-01-01",
        ...     end_time="2024-01-02",
        ... )
        >>> print(f"获取到 {len(df)} 条 K 线数据")
    """

    def __init__(self, config: Optional[Config] = None):
        """
        初始化期货数据获取器

        Args:
            config: 配置实例，None 则使用全局配置
        """
        self.config = config or get_config()
        self.ref_api = ReferenceDataAPI(config=self.config)
        self.ts_api = TimeseriesAPI(config=self.config)
        logger.info("FuturesDataFetcher 初始化完成")

    def _fetch_futures_markets(
        self,
        exchange: str,
        base: str,
    ) -> list[str]:
        """
        获取指定交易所和基础资产的所有期货市场列表（包括永续和交割）

        Args:
            exchange: 交易所名称
            base: 基础资产

        Returns:
            期货市场列表
        """
        df = self.ref_api.get_markets(
            exchange=exchange,
            market_type="future",
            base=base,
            verbose=False,
        )

        return df["market"].tolist()

    def _fetch_candles_batch(
        self,
        markets: list[str],
        start_time: str,
        end_time: str,
    ) -> pd.DataFrame:
        """
        批量获取一批市场的 K 线数据

        Args:
            markets: 市场列表
            start_time: 开始时间
            end_time: 结束时间

        Returns:
            K 线数据 DataFrame
        """
        markets_str = ",".join(markets)

        df = self.ts_api.get_market_candles(
            markets=markets_str,
            start_time=start_time,
            end_time=end_time,
            page_size=10000,
            verbose=False,
        )

        return df

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
    ) -> pd.DataFrame:
        """
        并发获取所有数据

        Args:
            markets: 市场列表
            start_time: 开始时间
            end_time: 结束时间
            batch_size: 每批数量
            max_workers: 最大并发数
            fetch_func: 获取数据的函数
            data_type: 数据类型名称（用于日志）
            verbose: 是否打印进度

        Returns:
            数据 DataFrame
        """
        all_dfs = []
        total_batches = (len(markets) + batch_size - 1) // batch_size
        errors: list[tuple[int, Exception]] = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for i in range(0, len(markets), batch_size):
                batch = markets[i : i + batch_size]
                batch_num = i // batch_size + 1
                future = executor.submit(fetch_func, batch, start_time, end_time)
                futures[future] = batch_num

            completed = 0
            for future in as_completed(futures):
                batch_num = futures[future]
                completed += 1
                try:
                    df = future.result()
                    if len(df) > 0:
                        all_dfs.append(df)
                    if verbose:
                        logger.info(
                            f"{data_type}: 完成批次 {batch_num}/{total_batches} "
                            f"({completed}/{total_batches})"
                        )
                except Exception as e:
                    logger.error(f"{data_type}: 批次 {batch_num} 失败 - {e}")
                    errors.append((batch_num, e))

        # 如果有错误，抛出异常
        if errors:
            raise BatchFetchError(errors)

        if all_dfs:
            return pd.concat(all_dfs, ignore_index=True)
        return pd.DataFrame()

    def get_candles(
        self,
        exchange: str,
        base: str,
        start_time: str,
        end_time: str,
        batch_size: int = 50,
        max_workers: int = 4,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取指定交易所和基础资产的所有期货 K 线数据

        获取所有期货市场（包括永续合约和交割合约）的分钟级 K 线数据。

        Args:
            exchange: 交易所名称 (如 deribit, binance)
            base: 基础资产 (如 btc, eth)
            start_time: 开始时间 (ISO 8601)
            end_time: 结束时间 (ISO 8601)
            batch_size: 每批请求的市场数量
            max_workers: 最大并发数，默认 4
            verbose: 是否打印进度

        Returns:
            K 线数据 DataFrame，包含以下字段：
            - market: 市场标识符
            - time: 时间戳
            - open, high, low, close: OHLC 价格
            - volume: 成交量
            - symbol, pair: 市场元数据

        Example:
            >>> fetcher = FuturesDataFetcher()
            >>> df = fetcher.get_candles(
            ...     exchange="deribit",
            ...     base="btc",
            ...     start_time="2024-01-01",
            ...     end_time="2024-01-02",
            ... )
            >>> print(f"获取到 {len(df)} 条 K 线数据")
        """
        # 验证时间参数
        validate_time_range(start_time, end_time)

        if verbose:
            logger.info("=" * 60)
            logger.info(f"获取 {exchange.upper()} {base.upper()} 期货 K 线数据")
            logger.info("=" * 60)
            logger.info(f"时间范围：{start_time} 至 {end_time}")

        # 步骤 1: 获取所有期货市场列表
        if verbose:
            logger.info("步骤 1/3: 获取期货市场列表...")

        markets = self._fetch_futures_markets(exchange, base)

        if verbose:
            logger.info(f"  找到 {len(markets)} 个期货市场")

        if not markets:
            if verbose:
                logger.warning("没有找到符合条件的期货市场")
            return pd.DataFrame()

        # 步骤 2: 获取 K 线数据
        if verbose:
            logger.info("步骤 2/3: 获取 K 线数据...")

        candles_df = self._fetch_all_concurrent(
            markets,
            start_time,
            end_time,
            batch_size,
            max_workers,
            self._fetch_candles_batch,
            "K线",
            verbose,
        )

        if verbose:
            logger.info(f"  获取到 {len(candles_df)} 条 K 线记录")

        # 步骤 3: 添加市场元数据
        if verbose:
            logger.info("步骤 3/3: 添加市场元数据...")

        if len(candles_df) > 0:
            # 获取市场元数据
            ref_df = self.ref_api.get_markets(
                exchange=exchange,
                market_type="future",
                base=base,
                verbose=False,
            )

            # 合并元数据
            result = pd.merge(
                candles_df,
                ref_df[["market", "symbol", "pair"]],
                on="market",
                how="left",
            )
        else:
            result = candles_df

        if verbose:
            logger.info(f"  最终数据：{len(result)} 条记录")
            logger.info("完成!")

        return result

    def save_to_csv(
        self,
        df: pd.DataFrame,
        output_path: str,
        overwrite: bool = True,
    ) -> str:
        """
        保存 DataFrame 到 CSV 文件

        Args:
            df: 要保存的 DataFrame
            output_path: 输出文件路径
            overwrite: 是否覆盖已存在的文件

        Returns:
            保存的文件路径
        """
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        if not overwrite and os.path.exists(output_path):
            raise FileExistsError(f"文件已存在：{output_path}")

        df.to_csv(output_path, index=False)
        logger.info(f"数据已保存到：{output_path}")
        return output_path

    def close(self) -> None:
        """关闭底层 API Sessions"""
        self.ref_api.close()
        self.ts_api.close()
        logger.info("FuturesDataFetcher Sessions 已关闭")

    def __enter__(self) -> "FuturesDataFetcher":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


def get_futures_candles(
    exchange: str,
    base: str,
    start_time: str,
    end_time: str,
    output_path: Optional[str] = None,
    batch_size: int = 50,
    max_workers: int = 4,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    便捷函数：获取期货 K 线数据

    Args:
        exchange: 交易所名称
        base: 基础资产
        start_time: 开始时间
        end_time: 结束时间
        output_path: 输出文件路径，None 表示不保存
        batch_size: 每批请求的市场数量
        max_workers: 最大并发数
        verbose: 是否打印进度

    Returns:
        K 线数据 DataFrame
    """
    fetcher = FuturesDataFetcher()
    df = fetcher.get_candles(
        exchange=exchange,
        base=base,
        start_time=start_time,
        end_time=end_time,
        batch_size=batch_size,
        max_workers=max_workers,
        verbose=verbose,
    )

    if output_path:
        fetcher.save_to_csv(df, output_path)

    return df