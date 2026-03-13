"""
CoinMetrics 期权数据获取模块
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from ..base_fetcher import BaseFetcher
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
    """期权数据获取器"""

    def get_options(self, filter: OptionFilter, page_size: Optional[int] = None, verbose: bool = True) -> pd.DataFrame:
        """获取期权市场列表"""
        df = self.ref_api.get_markets(
            exchange=filter.exchange, type=filter.market_type,
            base=filter.base, quote=filter.quote,
            page_size=page_size, verbose=verbose,
        )
        if filter.option_type:
            df = df[df["option_contract_type"] == filter.option_type]
        if filter.status:
            df = df[df["status"] == filter.status]

        # 按到期日和行权价排序
        if len(df) > 0 and "expiration" in df.columns and "strike" in df.columns:
            df = df.sort_values(["expiration", "strike"]).reset_index(drop=True)
        return df

    def get_deribit_btc_options(self, quote=None, option_type=None, status=None, page_size=None, verbose=True) -> pd.DataFrame:
        """获取 Deribit BTC 期权列表"""
        return self.get_options(OptionFilter("deribit", "btc", quote=quote, option_type=option_type, status=status), page_size, verbose)

    def get_deribit_eth_options(self, quote=None, option_type=None, status=None, page_size=None, verbose=True) -> pd.DataFrame:
        """获取 Deribit ETH 期权列表"""
        return self.get_options(OptionFilter("deribit", "eth", quote=quote, option_type=option_type, status=status), page_size, verbose)

    def _fetch_option_markets(self, exchange: str, base: str, option_type: Optional[str], status: Optional[str], start_time: str, end_time: str) -> tuple[list[str], pd.DataFrame]:
        """获取符合条件的期权市场列表"""
        df = self.ref_api.get_markets(exchange=exchange, type="option", base=base, verbose=False)

        if status:
            df = df[df["status"] == status]
        if option_type:
            df = df[df["option_contract_type"] == option_type]

        # 时间过滤（统一转换为 tz-naive）
        df = df.copy()
        listing_dt = pd.to_datetime(df["listing"], errors="coerce")
        expiration_dt = pd.to_datetime(df["expiration"], errors="coerce")
        if listing_dt.dt.tz is not None:
            listing_dt = listing_dt.dt.tz_convert(None)
        if expiration_dt.dt.tz is not None:
            expiration_dt = expiration_dt.dt.tz_convert(None)
        df["listing"] = listing_dt
        df["expiration"] = expiration_dt

        start_dt = pd.to_datetime(start_time)
        end_dt = pd.to_datetime(end_time)
        df = df[(df["listing"] <= end_dt) & (df["expiration"] >= start_dt)]

        return df["market"].tolist(), df[["market", "strike", "expiration", "option_contract_type", "listing"]].copy()

    def _fetch_greeks_batch(self, markets: list[str], start_time: str, end_time: str, granularity: str) -> pd.DataFrame:
        return self.ts_api.get_market_greeks(
            markets=",".join(markets), start_time=start_time, end_time=end_time,
            granularity=granularity, page_size=10000, verbose=False,
        )

    def _fetch_iv_batch(self, markets: list[str], start_time: str, end_time: str, granularity: str) -> pd.DataFrame:
        return self.ts_api.get_market_implied_volatility(
            markets=",".join(markets), start_time=start_time, end_time=end_time,
            granularity=granularity, page_size=10000, verbose=False,
        )

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
        """获取期权 Greeks 和 IV 数据"""
        validate_time_range(start_time, end_time)

        valid_granularities = {"raw", "1m", "1h", "1d"}
        if granularity not in valid_granularities:
            raise ValidationError(f"granularity 必须是 {valid_granularities} 之一")

        markets, metadata = self._fetch_option_markets(exchange, base, option_type, status, start_time, end_time)
        if verbose:
            logger.info(f"[Greeks/IV] {exchange.upper()} {base.upper()} | {len(markets)} 个期权")

        if not markets:
            return pd.DataFrame()

        # 使用基类的并发方法
        greeks_df = self._fetch_all_concurrent(
            markets, start_time, end_time, batch_size, max_workers,
            self._fetch_greeks_batch, "Greeks", verbose, granularity=granularity
        )

        iv_df = self._fetch_all_concurrent(
            markets, start_time, end_time, batch_size, max_workers,
            self._fetch_iv_batch, "IV", verbose, granularity=granularity
        )

        # 合并
        if greeks_df.empty and iv_df.empty:
            return pd.DataFrame()
        merged = greeks_df if iv_df.empty else iv_df if greeks_df.empty else pd.merge(greeks_df, iv_df, on=["market", "time"], how="outer")

        if not merged.empty:
            merged = pd.merge(merged, metadata, on="market", how="left")
            merged = merged.sort_values(["market", "time"]).reset_index(drop=True)

        if verbose:
            logger.info(f"[Greeks/IV] 完成：{len(merged)} 条")
        return merged
