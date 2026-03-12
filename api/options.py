"""
CoinMetrics 期权数据获取模块

提供 Deribit 等交易所期权数据的便捷获取功能。
"""

import logging
import os
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd

from api.reference_data import ReferenceDataAPI
from api.timeseries import TimeseriesAPI
from config import Config, get_config

logger = logging.getLogger(__name__)


@dataclass
class OptionFilter:
    """期权过滤条件"""

    exchange: str  # 交易所名称
    base: str  # 基础资产 (如 btc, eth)
    market_type: str = "option"  # 市场类型
    quote: Optional[str] = None  # 计价货币 (如 usd, usdc)
    option_type: Optional[str] = None  # 期权类型 (call/put)
    status: Optional[str] = None  # 状态 (online/offline)


class OptionsDataFetcher:
    """
    期权数据获取器

    提供获取全量期权数据的高级接口。

    Example:
        >>> fetcher = OptionsDataFetcher()
        >>> df = fetcher.get_deribit_btc_options()
        >>> print(f"获取到 {len(df)} 条期权数据")
    """

    def __init__(self, config: Optional[Config] = None):
        """
        初始化期权数据获取器

        Args:
            config: 配置实例，None 则使用全局配置
        """
        self.config = config or get_config()
        self.ref_api = ReferenceDataAPI(config=self.config)
        self.ts_api = TimeseriesAPI(config=self.config)
        logger.info("OptionsDataFetcher 初始化完成")

    def get_options(
        self,
        filter: OptionFilter,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取期权数据

        Args:
            filter: 期权过滤条件
            page_size: 每页大小，默认使用配置值
            verbose: 是否打印进度

        Returns:
            期权数据 DataFrame
        """
        logger.info(f"获取期权数据：exchange={filter.exchange}, base={filter.base}")

        df = self.ref_api.get_markets(
            exchange=filter.exchange,
            market_type=filter.market_type,
            base=filter.base,
            quote=filter.quote,
            page_size=page_size,
            verbose=verbose,
        )

        # 应用额外过滤
        if filter.option_type:
            df = df[df["option_contract_type"] == filter.option_type]
            logger.info(f"过滤后数据量 (option_type={filter.option_type}): {len(df)}")

        if filter.status:
            df = df[df["status"] == filter.status]
            logger.info(f"过滤后数据量 (status={filter.status}): {len(df)}")

        logger.info(f"获取完成，共 {len(df)} 条记录")
        return df

    def get_deribit_btc_options(
        self,
        quote: Optional[str] = None,
        option_type: Optional[str] = None,
        status: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取 Deribit BTC 期权数据

        Args:
            quote: 计价货币 (None 表示全部)
            option_type: 期权类型 call/put (None 表示全部)
            status: 状态 online/offline (None 表示全部)
            page_size: 每页大小
            verbose: 是否打印进度

        Returns:
            Deribit BTC 期权数据 DataFrame
        """
        filter = OptionFilter(
            exchange="deribit",
            base="btc",
            quote=quote,
            option_type=option_type,
            status=status,
        )
        return self.get_options(filter, page_size=page_size, verbose=verbose)

    def get_deribit_eth_options(
        self,
        quote: Optional[str] = None,
        option_type: Optional[str] = None,
        status: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取 Deribit ETH 期权数据

        Args:
            quote: 计价货币 (None 表示全部)
            option_type: 期权类型 call/put (None 表示全部)
            status: 状态 online/offline (None 表示全部)
            page_size: 每页大小
            verbose: 是否打印进度

        Returns:
            Deribit ETH 期权数据 DataFrame
        """
        filter = OptionFilter(
            exchange="deribit",
            base="eth",
            quote=quote,
            option_type=option_type,
            status=status,
        )
        return self.get_options(filter, page_size=page_size, verbose=verbose)

    def get_exchange_options(
        self,
        exchange: str,
        base: str,
        quote: Optional[str] = None,
        option_type: Optional[str] = None,
        status: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取指定交易所的期权数据

        Args:
            exchange: 交易所名称
            base: 基础资产
            quote: 计价货币 (None 表示全部)
            option_type: 期权类型 call/put (None 表示全部)
            status: 状态 online/offline (None 表示全部)
            page_size: 每页大小
            verbose: 是否打印进度

        Returns:
            期权数据 DataFrame
        """
        filter = OptionFilter(
            exchange=exchange,
            base=base,
            quote=quote,
            option_type=option_type,
            status=status,
        )
        return self.get_options(filter, page_size=page_size, verbose=verbose)

    def _fetch_option_markets(
        self,
        exchange: str,
        base: str,
        option_type: Optional[str],
        status: Optional[str],
        start_time: str,
        end_time: str,
    ) -> Tuple[List[str], pd.DataFrame]:
        """
        获取符合条件的期权市场列表，并根据时间范围过滤

        Args:
            exchange: 交易所名称
            base: 基础资产
            option_type: 期权类型 (call/put)
            status: 状态 (online/offline)
            start_time: 开始时间
            end_time: 结束时间

        Returns:
            (markets, metadata) - 市场列表和元数据 DataFrame
        """
        # 获取原始市场列表（注意：API 不支持 status 参数，需要后续过滤）
        df = self.ref_api.get_markets(
            exchange=exchange,
            market_type="option",
            base=base,
            verbose=False,
        )

        # 过滤 status
        if status:
            df = df[df["status"] == status]

        # 过滤 Call/Put
        if option_type:
            df = df[df["option_contract_type"] == option_type]

        # 时间过滤：期权在查询时间段内必须"存在"
        # 条件：listing <= end_time AND expiration >= start_time
        df = df.copy()
        df["listing"] = pd.to_datetime(df["listing"], errors="coerce")
        df["expiration"] = pd.to_datetime(df["expiration"], errors="coerce")

        start_dt = pd.to_datetime(start_time)
        end_dt = pd.to_datetime(end_time)

        # 只保留在时间范围内有交易的期权
        mask = (df["listing"] <= end_dt) & (df["expiration"] >= start_dt)
        df = df[mask]

        # 返回结果
        markets = df["market"].tolist()
        metadata = df[
            ["market", "strike", "expiration", "option_contract_type", "listing"]
        ].copy()

        return markets, metadata

    def _fetch_greeks_batch(
        self,
        markets: List[str],
        start_time: str,
        end_time: str,
        granularity: str,
    ) -> pd.DataFrame:
        """
        批量请求一批期权的 Greeks 数据

        Args:
            markets: 市场列表
            start_time: 开始时间
            end_time: 结束时间
            granularity: 数据粒度

        Returns:
            Greeks 数据 DataFrame
        """
        markets_str = ",".join(markets)

        df = self.ts_api.get_market_greeks(
            markets=markets_str,
            start_time=start_time,
            end_time=end_time,
            granularity=granularity,
            page_size=10000,
            verbose=False,
        )

        return df

    def _fetch_all_greeks(
        self,
        markets: List[str],
        start_time: str,
        end_time: str,
        granularity: str,
        batch_size: int,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        分批获取所有 Greeks 数据

        Args:
            markets: 市场列表
            start_time: 开始时间
            end_time: 结束时间
            granularity: 数据粒度
            batch_size: 每批数量
            verbose: 是否打印进度

        Returns:
            Greeks 数据 DataFrame
        """
        all_dfs = []
        total_batches = (len(markets) + batch_size - 1) // batch_size

        for i in range(0, len(markets), batch_size):
            batch = markets[i : i + batch_size]
            batch_num = i // batch_size + 1

            if verbose:
                logger.info(f"Greeks: 请求批次 {batch_num}/{total_batches} ({len(batch)} 个期权)")

            df = self._fetch_greeks_batch(batch, start_time, end_time, granularity)
            if len(df) > 0:
                all_dfs.append(df)

            # 速率限制
            if i + batch_size < len(markets):
                time.sleep(0.2)

        if all_dfs:
            return pd.concat(all_dfs, ignore_index=True)
        return pd.DataFrame()

    def _fetch_iv_batch(
        self,
        markets: List[str],
        start_time: str,
        end_time: str,
        granularity: str,
    ) -> pd.DataFrame:
        """
        批量请求一批期权的 IV 数据

        Args:
            markets: 市场列表
            start_time: 开始时间
            end_time: 结束时间
            granularity: 数据粒度

        Returns:
            IV 数据 DataFrame
        """
        markets_str = ",".join(markets)

        df = self.ts_api.get_market_implied_volatility(
            markets=markets_str,
            start_time=start_time,
            end_time=end_time,
            granularity=granularity,
            page_size=10000,
            verbose=False,
        )

        return df

    def _fetch_all_iv(
        self,
        markets: List[str],
        start_time: str,
        end_time: str,
        granularity: str,
        batch_size: int,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        分批获取所有 IV 数据

        Args:
            markets: 市场列表
            start_time: 开始时间
            end_time: 结束时间
            granularity: 数据粒度
            batch_size: 每批数量
            verbose: 是否打印进度

        Returns:
            IV 数据 DataFrame
        """
        all_dfs = []
        total_batches = (len(markets) + batch_size - 1) // batch_size

        for i in range(0, len(markets), batch_size):
            batch = markets[i : i + batch_size]
            batch_num = i // batch_size + 1

            if verbose:
                logger.info(f"IV: 请求批次 {batch_num}/{total_batches} ({len(batch)} 个期权)")

            df = self._fetch_iv_batch(batch, start_time, end_time, granularity)
            if len(df) > 0:
                all_dfs.append(df)

            # 速率限制
            if i + batch_size < len(markets):
                time.sleep(0.2)

        if all_dfs:
            return pd.concat(all_dfs, ignore_index=True)
        return pd.DataFrame()

    def _merge_greeks_iv(
        self, greeks_df: pd.DataFrame, iv_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        合并 Greeks 和 IV 数据

        Args:
            greeks_df: Greeks 数据
            iv_df: IV 数据

        Returns:
            合并后的 DataFrame
        """
        if greeks_df.empty and iv_df.empty:
            return pd.DataFrame()
        if greeks_df.empty:
            return iv_df
        if iv_df.empty:
            return greeks_df

        # 按 (market, time) 外连接
        merged = pd.merge(
            greeks_df,
            iv_df,
            on=["market", "time"],
            how="outer",
        )

        return merged

    def _enrich_metadata(
        self, data_df: pd.DataFrame, metadata_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        添加期权元数据（行权价、到期日等）

        Args:
            data_df: Greeks+IV 数据
            metadata_df: 期权元数据

        Returns:
             enriched DataFrame
        """
        if data_df.empty:
            return data_df
        if metadata_df.empty:
            return data_df

        # 合并元数据
        result = pd.merge(
            data_df,
            metadata_df,
            on="market",
            how="left",
        )

        # 选择并排序输出列
        output_columns = [
            "market",
            "time",
            "exchange",
            "base",
            "quote",
            "option_contract_type",
            "strike",
            "expiration",
            # Greeks
            "delta",
            "gamma",
            "theta",
            "vega",
            "rho",
            # IV
            "iv_mark",
            "iv_trade",
            "iv_bid",
            "iv_ask",
            # 时间戳
            "database_time",
            "exchange_time",
        ]

        # 只保留存在的列
        output_columns = [c for c in output_columns if c in result.columns]

        return result[output_columns]

    def get_options_greeks_iv(
        self,
        exchange: str,
        base: str,
        start_time: str,
        end_time: str,
        option_type: Optional[str] = None,
        status: Optional[str] = None,  # 默认不过滤，因为历史数据查询需要 offline 期权
        granularity: str = "1m",
        batch_size: int = 100,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取期权 Greeks 和 IV 数据

        Args:
            exchange: 交易所名称 (如 deribit)
            base: 基础资产 (如 btc, eth)
            start_time: 开始时间 (ISO 8601, 如 "2024-01-01" 或 "2024-01-01T00:00:00Z")
            end_time: 结束时间 (ISO 8601)
            option_type: 期权类型 (call/put)，None 表示全部
            status: 状态 (online/offline)，None 表示全部
            granularity: 数据粒度，默认 "1m" (分钟频)
            batch_size: 每批请求的期权数量
            verbose: 是否打印进度

        Returns:
            包含 Greeks 和 IV 数据的 DataFrame

        Example:
            >>> fetcher = OptionsDataFetcher()
            >>> df = fetcher.get_options_greeks_iv(
            ...     exchange="deribit",
            ...     base="btc",
            ...     start_time="2024-01-01",
            ...     end_time="2024-01-02",
            ...     granularity="1m",
            ... )
            >>> print(df.columns.tolist())
        """
        if verbose:
            logger.info("=" * 60)
            logger.info(f"获取 {exchange.upper()} {base.upper()} 期权 Greeks 和 IV 数据")
            logger.info("=" * 60)
            logger.info(f"时间范围：{start_time} 至 {end_time}")
            logger.info(f"数据粒度：{granularity}")

        # 步骤 1: 获取期权列表（包含时间过滤）
        if verbose:
            logger.info("步骤 1/4: 获取期权列表...")

        markets, metadata = self._fetch_option_markets(
            exchange=exchange,
            base=base,
            option_type=option_type,
            status=status,
            start_time=start_time,
            end_time=end_time,
        )

        if verbose:
            logger.info(f"  找到 {len(markets)} 个符合条件的期权")

        if not markets:
            if verbose:
                logger.warning("没有找到符合条件的期权")
            return pd.DataFrame()

        # 步骤 2: 获取 Greeks 数据
        if verbose:
            logger.info("步骤 2/4: 获取 Greeks 数据...")

        greeks_df = self._fetch_all_greeks(
            markets, start_time, end_time, granularity, batch_size, verbose
        )

        if verbose:
            logger.info(f"  获取到 {len(greeks_df)} 条 Greeks 记录")

        # 步骤 3: 获取 IV 数据
        if verbose:
            logger.info("步骤 3/4: 获取 IV 数据...")

        iv_df = self._fetch_all_iv(
            markets, start_time, end_time, granularity, batch_size, verbose
        )

        if verbose:
            logger.info(f"  获取到 {len(iv_df)} 条 IV 记录")

        # 步骤 4: 合并数据
        if verbose:
            logger.info("步骤 4/4: 合并数据...")

        merged_df = self._merge_greeks_iv(greeks_df, iv_df)
        result = self._enrich_metadata(merged_df, metadata)

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

        Raises:
            FileExistsError: 文件已存在且 overwrite=False
        """
        # 确保目录存在
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        if not overwrite and os.path.exists(output_path):
            raise FileExistsError(f"文件已存在：{output_path}")

        df.to_csv(output_path, index=False)
        logger.info(f"数据已保存到：{output_path}")
        return output_path

    def fetch_and_save(
        self,
        filter: OptionFilter,
        output_path: str,
        page_size: Optional[int] = None,
        verbose: bool = True,
        overwrite: bool = True,
    ) -> str:
        """
        获取期权数据并保存到文件

        Args:
            filter: 期权过滤条件
            output_path: 输出文件路径
            page_size: 每页大小
            verbose: 是否打印进度
            overwrite: 是否覆盖已存在的文件

        Returns:
            保存的文件路径
        """
        df = self.get_options(filter, page_size=page_size, verbose=verbose)
        return self.save_to_csv(df, output_path, overwrite=overwrite)


def get_deribit_btc_options(
    output_path: Optional[str] = None,
    quote: Optional[str] = None,
    option_type: Optional[str] = None,
    status: Optional[str] = None,
    page_size: Optional[int] = None,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    便捷函数：获取 Deribit BTC 期权数据

    Args:
        output_path: 输出文件路径，None 表示不保存
        quote: 计价货币 (None 表示全部)
        option_type: 期权类型 call/put (None 表示全部)
        status: 状态 online/offline (None 表示全部)
        page_size: 每页大小
        verbose: 是否打印进度

    Returns:
        Deribit BTC 期权数据 DataFrame
    """
    fetcher = OptionsDataFetcher()
    df = fetcher.get_deribit_btc_options(
        quote=quote,
        option_type=option_type,
        status=status,
        page_size=page_size,
        verbose=verbose,
    )

    if output_path:
        fetcher.save_to_csv(df, output_path)

    return df
