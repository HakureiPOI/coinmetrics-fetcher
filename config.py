"""
CoinMetrics 配置管理模块

从环境变量加载配置，提供统一的配置访问接口。
"""

import os
import logging
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv

# 加载环境变量
load_dotenv()


# API 基础 URL 预设
COMMUNITY_BASE_URL = "https://community-api.coinmetrics.io/v4"
PRO_BASE_URL = "https://api.coinmetrics.io/v4"


@dataclass
class Config:
    """CoinMetrics API 配置"""

    # API 认证
    api_key: str = field(default_factory=lambda: os.getenv("COINMETRICS_API_KEY", ""))

    # API 基础 URL
    base_url: str = field(
        default_factory=lambda: os.getenv(
            "COINMETRICS_BASE_URL", PRO_BASE_URL
        )
    )

    # 是否使用社区版 API（如果为 True，则忽略 api_key）
    use_community_api: bool = field(
        default_factory=lambda: os.getenv("COINMETRICS_USE_COMMUNITY_API", "false").lower() == "true"
    )

    # 请求超时 (秒)
    timeout: float = field(
        default_factory=lambda: float(os.getenv("COINMETRICS_TIMEOUT", "30"))
    )

    # 分页大小默认值
    page_size: int = field(
        default_factory=lambda: int(os.getenv("COINMETRICS_PAGE_SIZE", "10000"))
    )

    # 最大重试次数
    max_retries: int = field(
        default_factory=lambda: int(os.getenv("COINMETRICS_MAX_RETRIES", "3"))
    )

    # 请求间隔 (秒)
    rate_limit_delay: float = field(
        default_factory=lambda: float(os.getenv("COINMETRICS_RATE_LIMIT_DELAY", "0"))
    )

    # 日志配置
    log_level: str = field(
        default_factory=lambda: os.getenv("LOG_LEVEL", "INFO")
    )
    log_file: Optional[str] = field(
        default_factory=lambda: os.getenv("LOG_FILE")
    )

    def __post_init__(self):
        """验证配置"""
        # 如果 api_key 为空，自动使用社区版 API
        if not self.api_key:
            self.use_community_api = True
        
        if not (1 <= self.page_size <= 10000):
            raise ValueError("COINMETRICS_PAGE_SIZE 必须在 1-10000 之间")

        if self.timeout <= 0:
            raise ValueError("COINMETRICS_TIMEOUT 必须大于 0")

        # 如果启用社区版 API，自动设置 base_url
        if self.use_community_api:
            self.base_url = COMMUNITY_BASE_URL

    @property
    def log_level_int(self) -> int:
        """返回日志级别整数值"""
        return getattr(logging, self.log_level.upper(), logging.INFO)


# 全局配置实例
config = Config()


def get_config() -> Config:
    """获取全局配置实例"""
    return config


def setup_logging() -> logging.Logger:
    """
    根据配置设置日志系统

    Returns:
        配置好的 logger 实例
    """
    from utils.fetch_utils import setup_logging as _setup_logging

    cfg = get_config()
    _setup_logging(
        level=cfg.log_level_int,
        log_file=cfg.log_file,
    )

    return logging.getLogger("coinmetrics")
