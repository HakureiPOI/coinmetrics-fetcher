"""
CoinMetrics 期权数据获取模块

提供 Deribit 等交易所期权数据的便捷获取功能。
"""

import logging
import os
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd

from api.reference_data import ReferenceDataAPI
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
