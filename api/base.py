"""
CoinMetrics API 基础封装类

提供通用的 API 请求方法和配置管理。
"""

import logging
from typing import Any, Optional, Union

import pandas as pd
import requests

from config import Config, get_config
from utils.fetch_utils import build_session, fetch_all_pages


logger = logging.getLogger(__name__)


class CoinMetricsAPI:
    """
    CoinMetrics API 基础封装类

    提供通用的分页请求、连接池管理和配置加载功能。

    Attributes:
        config: 配置实例
        session: requests Session 实例
        base_url: API 基础 URL
    """

    def __init__(
        self,
        config: Optional[Config] = None,
        session: Optional[requests.Session] = None,
    ):
        """
        初始化 API 客户端

        Args:
            config: 配置实例，None 则使用全局配置
            session: requests Session 实例，None 则自动创建
        """
        self.config = config or get_config()
        self.base_url = self.config.base_url

        if session:
            self.session = session
        else:
            self.session = self._create_session()

        logger.info(f"CoinMetricsAPI 初始化完成，base_url={self.base_url}")

    def _create_session(self) -> requests.Session:
        """创建带连接池和重试机制的 Session"""
        headers = {
            "User-Agent": "coinmetrics-fetcher/1.0",
        }
        return build_session(
            total_retries=self.config.max_retries,
            headers=headers,
            timeout=self.config.timeout,
        )

    def _build_url(self, endpoint: str) -> str:
        """
        构建完整 API URL

        Args:
            endpoint: API 端点路径 (如 '/reference-data/assets')

        Returns:
            完整的 URL
        """
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
        """
        通用分页请求方法

        Args:
            endpoint: API 端点路径
            params: 查询参数
            page_size: 每页大小，默认使用配置值
            max_pages: 最大页数，None 表示不限制
            verbose: 是否打印进度
            return_dataframe: 是否返回 DataFrame
            data_key: 数据字段名
            next_page_key: 下一页 URL 字段名

        Returns:
            DataFrame 或 list[dict]
        """
        url = self._build_url(endpoint)

        # 合并默认参数
        request_params = params.copy() if params else {}
        request_params["api_key"] = self.config.api_key

        # 设置 page_size
        if page_size is None:
            page_size = self.config.page_size
        request_params["page_size"] = page_size

        # 日志中隐藏 API key
        log_params = request_params.copy()
        if "api_key" in log_params:
            log_params["api_key"] = "***MASKED***"

        logger.debug(
            f"请求 API: {url}, params={log_params}, page_size={page_size}"
        )

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
            logger.info("Session 已关闭")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
