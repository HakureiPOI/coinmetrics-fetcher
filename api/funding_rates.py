"""
CoinMetrics 永续合约资金费率和期货 K 线数据获取模块

提供 Deribit 等交易所永续合约资金费率、预计资金费率和期货 K 线的便捷获取功能。
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


class FundingRateFetcher:
    """
    资金费率数据获取器

    提供获取永续合约资金费率和预计资金费率的高级接口。

    Example:
        >>> fetcher = FundingRateFetcher()
        >>> df = fetcher.get_funding_rates(
        ...     exchange="deribit",
        ...     base="btc",
        ...     start_time="2024-01-01",
        ...     end_time="2024-01-31",
        ... )
        >>> print(f"获取到 {len(df)} 条资金费率数据")
    """

    def __init__(self, config: Optional[Config] = None):
        """
        初始化资金费率数据获取器

        Args:
            config: 配置实例，None 则使用全局配置
        """
        self.config = config or get_config()
        self.ref_api = ReferenceDataAPI(config=self.config)
        self.ts_api = TimeseriesAPI(config=self.config)
        logger.info("FundingRateFetcher 初始化完成")

    def _fetch_perpetual_markets(
        self,
        exchange: str,
        base: str,
    ) -> list[str]:
        """
        获取指定交易所和基础资产的永续合约市场列表

        Args:
            exchange: 交易所名称
            base: 基础资产

        Returns:
            永续合约市场列表
        """
        df = self.ref_api.get_markets(
            exchange=exchange,
            market_type="future",
            base=base,
            verbose=False,
        )

        # 过滤永续合约（通常 symbol 包含 PERPETUAL）
        perpetuals = df[df["symbol"].str.contains("PERPETUAL", case=True, na=False)]

        return perpetuals["market"].tolist()

    def _fetch_funding_rates_batch(
        self,
        markets: list[str],
        start_time: str,
        end_time: str,
    ) -> pd.DataFrame:
        """
        批量获取一批市场的资金费率数据

        Args:
            markets: 市场列表
            start_time: 开始时间
            end_time: 结束时间

        Returns:
            资金费率数据 DataFrame
        """
        markets_str = ",".join(markets)

        df = self.ts_api.get_market_funding_rates(
            markets=markets_str,
            start_time=start_time,
            end_time=end_time,
            page_size=10000,
            verbose=False,
        )

        return df

    def _fetch_predicted_rates_batch(
        self,
        markets: list[str],
        start_time: str,
        end_time: str,
    ) -> pd.DataFrame:
        """
        批量获取一批市场的预计资金费率数据

        Args:
            markets: 市场列表
            start_time: 开始时间
            end_time: 结束时间

        Returns:
            预计资金费率数据 DataFrame
        """
        markets_str = ",".join(markets)

        df = self.ts_api.get_market_funding_rates_predicted(
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

    def get_funding_rates_clean(
        self,
        exchange: str,
        base: str,
        start_time: str,
        end_time: str,
        batch_size: int = 50,
        max_workers: int = 4,
        dropna: bool = True,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取永续合约资金费率数据（干净版本，仅包含实际资金费率记录）

        与 get_funding_rates() 的区别：
        - 默认 dropna=True，过滤掉 NaN 记录
        - 返回的数据仅包含实际资金费率更新的时间点（每 8 小时）
        - 适合只需要实际资金费率数据的场景

        Args:
            exchange: 交易所名称 (如 deribit, binance)
            base: 基础资产 (如 btc, eth)
            start_time: 开始时间 (ISO 8601)
            end_time: 结束时间 (ISO 8601)
            batch_size: 每批请求的市场数量
            max_workers: 最大并发数，默认 4
            dropna: 是否过滤 NaN 记录，默认 True
            verbose: 是否打印进度

        Returns:
            资金费率数据 DataFrame（仅包含有效记录）

        Example:
            >>> fetcher = FundingRateFetcher()
            >>> # 获取干净的资金费率数据（每 8 小时一条）
            >>> df = fetcher.get_funding_rates_clean(
            ...     exchange="deribit",
            ...     base="btc",
            ...     start_time="2024-01-01",
            ...     end_time="2024-01-31",
            ... )
            >>> # 每天 3 条数据（00:00, 08:00, 16:00 UTC）
        """
        # 验证时间参数
        validate_time_range(start_time, end_time)

        if verbose:
            logger.info("=" * 60)
            logger.info(f"获取 {exchange.upper()} {base.upper()} 永续合约资金费率（干净版）")
            logger.info("=" * 60)
            logger.info(f"时间范围：{start_time} 至 {end_time}")
            logger.info(f"过滤 NaN: {dropna}")

        # 步骤 1: 获取永续合约市场列表
        if verbose:
            logger.info("步骤 1/3: 获取永续合约市场列表...")

        markets = self._fetch_perpetual_markets(exchange, base)

        if verbose:
            logger.info(f"  找到 {len(markets)} 个永续合约")

        if not markets:
            if verbose:
                logger.warning("没有找到符合条件的永续合约")
            return pd.DataFrame()

        # 步骤 2: 获取资金费率数据
        if verbose:
            logger.info("步骤 2/3: 获取资金费率数据...")

        funding_df = self._fetch_all_concurrent(
            markets,
            start_time,
            end_time,
            batch_size,
            max_workers,
            self._fetch_funding_rates_batch,
            "资金费率",
            verbose,
        )

        # 过滤 NaN 记录
        if dropna and len(funding_df) > 0:
            before_count = len(funding_df)
            funding_df = funding_df.dropna(subset=["rate"])
            after_count = len(funding_df)
            if verbose:
                logger.info(f"  过滤 NaN: {before_count} -> {after_count} 条")

        if verbose:
            logger.info(f"  获取到 {len(funding_df)} 条资金费率记录")

        # 步骤 3: 添加市场元数据
        if verbose:
            logger.info("步骤 3/3: 添加市场元数据...")

        if len(funding_df) > 0:
            # 获取市场元数据
            ref_df = self.ref_api.get_markets(
                exchange=exchange,
                market_type="future",
                base=base,
                verbose=False,
            )

            # 合并元数据
            result = pd.merge(
                funding_df,
                ref_df[["market", "symbol", "pair"]],
                on="market",
                how="left",
            )
        else:
            result = funding_df

        if verbose:
            logger.info(f"  最终数据：{len(result)} 条记录")
            logger.info("完成!")

        return result

    def get_funding_rates(
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
        获取永续合约资金费率数据

        Args:
            exchange: 交易所名称 (如 deribit, binance)
            base: 基础资产 (如 btc, eth)
            start_time: 开始时间 (ISO 8601)
            end_time: 结束时间 (ISO 8601)
            batch_size: 每批请求的市场数量
            max_workers: 最大并发数，默认 4
            verbose: 是否打印进度

        Returns:
            资金费率数据 DataFrame

        Example:
            >>> fetcher = FundingRateFetcher()
            >>> df = fetcher.get_funding_rates(
            ...     exchange="deribit",
            ...     base="btc",
            ...     start_time="2024-01-01",
            ...     end_time="2024-01-31",
            ... )
        """
        # 验证时间参数
        validate_time_range(start_time, end_time)

        if verbose:
            logger.info("=" * 60)
            logger.info(f"获取 {exchange.upper()} {base.upper()} 永续合约资金费率")
            logger.info("=" * 60)
            logger.info(f"时间范围：{start_time} 至 {end_time}")

        # 步骤 1: 获取永续合约市场列表
        if verbose:
            logger.info("步骤 1/3: 获取永续合约市场列表...")

        markets = self._fetch_perpetual_markets(exchange, base)

        if verbose:
            logger.info(f"  找到 {len(markets)} 个永续合约")

        if not markets:
            if verbose:
                logger.warning("没有找到符合条件的永续合约")
            return pd.DataFrame()

        # 步骤 2: 获取资金费率数据
        if verbose:
            logger.info("步骤 2/3: 获取资金费率数据...")

        funding_df = self._fetch_all_concurrent(
            markets,
            start_time,
            end_time,
            batch_size,
            max_workers,
            self._fetch_funding_rates_batch,
            "资金费率",
            verbose,
        )

        if verbose:
            logger.info(f"  获取到 {len(funding_df)} 条资金费率记录")

        # 步骤 3: 添加市场元数据
        if verbose:
            logger.info("步骤 3/3: 添加市场元数据...")

        if len(funding_df) > 0:
            # 获取市场元数据
            ref_df = self.ref_api.get_markets(
                exchange=exchange,
                market_type="future",
                base=base,
                verbose=False,
            )

            # 合并元数据
            result = pd.merge(
                funding_df,
                ref_df[["market", "symbol", "pair"]],
                on="market",
                how="left",
            )
        else:
            result = funding_df

        if verbose:
            logger.info(f"  最终数据：{len(result)} 条记录")
            logger.info("完成!")

        return result

    def get_predicted_funding_rates(
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
        获取永续合约预计资金费率数据

        Args:
            exchange: 交易所名称 (如 deribit, binance)
            base: 基础资产 (如 btc, eth)
            start_time: 开始时间 (ISO 8601)
            end_time: 结束时间 (ISO 8601)
            batch_size: 每批请求的市场数量
            max_workers: 最大并发数，默认 4
            verbose: 是否打印进度

        Returns:
            预计资金费率数据 DataFrame
        """
        # 验证时间参数
        validate_time_range(start_time, end_time)

        if verbose:
            logger.info("=" * 60)
            logger.info(f"获取 {exchange.upper()} {base.upper()} 永续合约预计资金费率")
            logger.info("=" * 60)
            logger.info(f"时间范围：{start_time} 至 {end_time}")

        # 步骤 1: 获取永续合约市场列表
        if verbose:
            logger.info("步骤 1/3: 获取永续合约市场列表...")

        markets = self._fetch_perpetual_markets(exchange, base)

        if verbose:
            logger.info(f"  找到 {len(markets)} 个永续合约")

        if not markets:
            if verbose:
                logger.warning("没有找到符合条件的永续合约")
            return pd.DataFrame()

        # 步骤 2: 获取预计资金费率数据
        if verbose:
            logger.info("步骤 2/3: 获取预计资金费率数据...")

        predicted_df = self._fetch_all_concurrent(
            markets,
            start_time,
            end_time,
            batch_size,
            max_workers,
            self._fetch_predicted_rates_batch,
            "预计资金费率",
            verbose,
        )

        if verbose:
            logger.info(f"  获取到 {len(predicted_df)} 条预计资金费率记录")

        # 步骤 3: 添加市场元数据
        if verbose:
            logger.info("步骤 3/3: 添加市场元数据...")

        if len(predicted_df) > 0:
            # 获取市场元数据
            ref_df = self.ref_api.get_markets(
                exchange=exchange,
                market_type="future",
                base=base,
                verbose=False,
            )

            # 合并元数据
            result = pd.merge(
                predicted_df,
                ref_df[["market", "symbol", "pair"]],
                on="market",
                how="left",
            )
        else:
            result = predicted_df

        if verbose:
            logger.info(f"  最终数据：{len(result)} 条记录")
            logger.info("完成!")

        return result

    def _fetch_all_futures_markets(
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

    def get_futures_candles(
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
            >>> fetcher = FundingRateFetcher()
            >>> df = fetcher.get_futures_candles(
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

        markets = self._fetch_all_futures_markets(exchange, base)

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
        logger.info("FundingRateFetcher Sessions 已关闭")

    def __enter__(self) -> "FundingRateFetcher":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


def get_funding_rates(
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
    便捷函数：获取永续合约资金费率数据

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
        资金费率数据 DataFrame
    """
    fetcher = FundingRateFetcher()
    df = fetcher.get_funding_rates(
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
    fetcher = FundingRateFetcher()
    df = fetcher.get_futures_candles(
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
