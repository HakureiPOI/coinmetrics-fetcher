"""
CoinMetrics 期权数据获取模块
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from api.base_fetcher import BaseFetcher
from utils import BatchFetchError, ValidationError, validate_time_range

logger = logging.getLogger(__name__)


@dataclass
class OptionFilter:
    """期权过滤条件"""
    exchange: str
    base: str
    market_type: str = "option"
    quote: Optional[str] = None
    option_type: Optional[str] = None
    status: Optional[str] = None


class OptionsDataFetcher(BaseFetcher):
    """
    期权数据获取器

    提供获取期权市场列表、Greeks 和 IV 数据的高级接口。
    """

    def get_options(
        self,
        filter: OptionFilter,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """获取期权市场列表"""
        df = self.ref_api.get_markets(
            exchange=filter.exchange,
            market_type=filter.market_type,
            base=filter.base,
            quote=filter.quote,
            page_size=page_size,
            verbose=verbose,
        )

        if filter.option_type:
            df = df[df["option_contract_type"] == filter.option_type]
        if filter.status:
            df = df[df["status"] == filter.status]

        return df

    def get_deribit_btc_options(
        self,
        quote: Optional[str] = None,
        option_type: Optional[str] = None,
        status: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """获取 Deribit BTC 期权列表"""
        return self.get_options(
            OptionFilter("deribit", "btc", quote=quote, option_type=option_type, status=status),
            page_size=page_size, verbose=verbose
        )

    def get_deribit_eth_options(
        self,
        quote: Optional[str] = None,
        option_type: Optional[str] = None,
        status: Optional[str] = None,
        page_size: Optional[int] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """获取 Deribit ETH 期权列表"""
        return self.get_options(
            OptionFilter("deribit", "eth", quote=quote, option_type=option_type, status=status),
            page_size=page_size, verbose=verbose
        )

    def _fetch_option_markets(
        self,
        exchange: str,
        base: str,
        option_type: Optional[str],
        status: Optional[str],
        start_time: str,
        end_time: str,
    ) -> tuple[list[str], pd.DataFrame]:
        """获取符合条件的期权市场列表"""
        df = self.ref_api.get_markets(
            exchange=exchange, market_type="option", base=base, verbose=False
        )

        if status:
            df = df[df["status"] == status]
        if option_type:
            df = df[df["option_contract_type"] == option_type]

        # 时间过滤
        df = df.copy()
        listing_dt = pd.to_datetime(df["listing"], errors="coerce")
        expiration_dt = pd.to_datetime(df["expiration"], errors="coerce")
        
        # 统一转换为 tz-naive
        if listing_dt.dt.tz is not None:
            listing_dt = listing_dt.dt.tz_convert(None)
        if expiration_dt.dt.tz is not None:
            expiration_dt = expiration_dt.dt.tz_convert(None)
        
        df["listing"] = listing_dt
        df["expiration"] = expiration_dt
        
        start_dt = pd.to_datetime(start_time)
        end_dt = pd.to_datetime(end_time)
        mask = (df["listing"] <= end_dt) & (df["expiration"] >= start_dt)
        df = df[mask]

        markets = df["market"].tolist()
        metadata = df[["market", "strike", "expiration", "option_contract_type", "listing"]].copy()
        return markets, metadata

    def _fetch_greeks_batch(
        self, markets: list[str], start_time: str, end_time: str, granularity: str
    ) -> pd.DataFrame:
        """批量获取 Greeks 数据"""
        return self.ts_api.get_market_greeks(
            markets=",".join(markets),
            start_time=start_time,
            end_time=end_time,
            granularity=granularity,
            page_size=10000,
            verbose=False,
        )

    def _fetch_iv_batch(
        self, markets: list[str], start_time: str, end_time: str, granularity: str
    ) -> pd.DataFrame:
        """批量获取 IV 数据"""
        return self.ts_api.get_market_implied_volatility(
            markets=",".join(markets),
            start_time=start_time,
            end_time=end_time,
            granularity=granularity,
            page_size=10000,
            verbose=False,
        )

    def _fetch_all_with_granularity(
        self,
        markets: list[str],
        start_time: str,
        end_time: str,
        granularity: str,
        batch_size: int,
        max_workers: int,
        fetch_func,
        data_type: str,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """并发获取数据（带 granularity 参数）"""
        all_dfs = []
        total_batches = (len(markets) + batch_size - 1) // batch_size
        errors: list[tuple[int, Exception]] = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for i in range(0, len(markets), batch_size):
                batch = markets[i : i + batch_size]
                batch_num = i // batch_size + 1
                future = executor.submit(fetch_func, batch, start_time, end_time, granularity)
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
                        logger.info(f"{data_type}: 完成批次 {batch_num}/{total_batches} ({completed}/{total_batches})")
                except Exception as e:
                    logger.error(f"{data_type}: 批次 {batch_num} 失败 - {e}")
                    errors.append((batch_num, e))

        if errors:
            raise BatchFetchError(errors)

        if all_dfs:
            return pd.concat(all_dfs, ignore_index=True)
        return pd.DataFrame()

    def get_options_greeks_iv(
        self,
        exchange: str,
        base: str,
        start_time: str,
        end_time: str,
        option_type: Optional[str] = None,
        status: Optional[str] = None,
        granularity: str = "1m",
        batch_size: int = 100,
        max_workers: int = 4,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        获取期权 Greeks 和 IV 数据

        Args:
            exchange: 交易所名称 (如 deribit)
            base: 基础资产 (如 btc, eth)
            start_time: 开始时间 (ISO 8601)
            end_time: 结束时间 (ISO 8601)
            option_type: 期权类型 (call/put)，None 表示全部
            status: 状态 (online/offline)，None 表示全部
            granularity: 数据粒度 (1m/5m/15m/30m/1h/4h/1d)
            batch_size: 每批请求的期权数量
            max_workers: 最大并发数
            verbose: 是否打印进度

        Returns:
            包含 Greeks 和 IV 数据的 DataFrame
        """
        validate_time_range(start_time, end_time)

        valid_granularities = {"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
        if granularity not in valid_granularities:
            raise ValidationError(f"granularity 必须是 {valid_granularities} 之一")

        if verbose:
            logger.info("=" * 60)
            logger.info(f"获取 {exchange.upper()} {base.upper()} 期权 Greeks 和 IV 数据")
            logger.info(f"时间范围：{start_time} 至 {end_time}，粒度：{granularity}")

        markets, metadata = self._fetch_option_markets(exchange, base, option_type, status, start_time, end_time)
        if verbose:
            logger.info(f"找到 {len(markets)} 个符合条件的期权")

        if not markets:
            return pd.DataFrame()

        # 获取 Greeks
        if verbose:
            logger.info("获取 Greeks 数据...")
        greeks_df = self._fetch_all_with_granularity(
            markets, start_time, end_time, granularity, batch_size, max_workers,
            self._fetch_greeks_batch, "Greeks", verbose
        )

        # 获取 IV
        if verbose:
            logger.info("获取 IV 数据...")
        iv_df = self._fetch_all_with_granularity(
            markets, start_time, end_time, granularity, batch_size, max_workers,
            self._fetch_iv_batch, "IV", verbose
        )

        # 合并
        if greeks_df.empty and iv_df.empty:
            return pd.DataFrame()
        if greeks_df.empty:
            merged = iv_df
        elif iv_df.empty:
            merged = greeks_df
        else:
            merged = pd.merge(greeks_df, iv_df, on=["market", "time"], how="outer")

        # 添加元数据
        if not merged.empty:
            merged = pd.merge(merged, metadata, on="market", how="left")

        if verbose:
            logger.info(f"完成，共 {len(merged)} 条记录")
        return merged