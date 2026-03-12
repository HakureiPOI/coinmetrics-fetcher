"""
CoinMetrics API 基础封装类
"""

import logging
from typing import Any, Optional, Union

import pandas as pd
import requests

from config import Config, get_config
from utils.fetch_utils import build_session, fetch_all_pages

logger = logging.getLogger(__name__)


class CoinMetricsAPI:
    """CoinMetrics API 基础封装类"""

    def __init__(self, config: Optional[Config] = None, session: Optional[requests.Session] = None):
        self.config = config or get_config()
        self.base_url = self.config.base_url
        self.session = session or self._create_session()

    def _create_session(self) -> requests.Session:
        """创建带连接池和重试机制的 Session"""
        return build_session(
            total_retries=self.config.max_retries,
            headers={"User-Agent": "coinmetrics-fetcher/1.0"},
            timeout=self.config.timeout,
        )

    def _build_url(self, endpoint: str) -> str:
        """构建完整 API URL"""
        if endpoint.startswith("/"):
            return f"{self.base_url}{endpoint}"
        return f"{self.base_url}/{endpoint}"

    def _request(
        self,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
        page_size: Optional[int] = None,
        max_pages: Optional[int] = None,
        verbose: bool = True,
        return_dataframe: bool = True,
        data_key: str = "data",
        next_page_key: str = "next_page_url",
    ) -> Union[pd.DataFrame, list[dict[str, Any]]]:
        """通用分页请求方法"""
        url = self._build_url(endpoint)

        request_params = params.copy() if params else {}
        request_params["api_key"] = self.config.api_key

        if page_size is None:
            page_size = self.config.page_size
        request_params["page_size"] = page_size

        return fetch_all_pages(
            url=url,
            params=request_params,
            timeout=self.config.timeout,
            max_pages=max_pages,
            max_retries=self.config.max_retries,
            rate_limit_delay=self.config.rate_limit_delay,
            verbose=verbose,
            return_dataframe=return_dataframe,
            data_key=data_key,
            next_page_key=next_page_key,
            session=self.session,
        )

    def close(self):
        """关闭 Session"""
        if self.session:
            self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
