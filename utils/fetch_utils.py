import logging
import sys
import time
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

_log_initialized = False


def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    format_str: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
) -> None:
    """
    配置日志系统。

    Args:
        level: 日志级别，默认 INFO
        log_file: 日志文件路径，None 表示仅输出到控制台
        format_str: 日志格式字符串
    """
    global _log_initialized

    if _log_initialized:
        logger.warning("日志系统已初始化，跳过配置")
        return

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format=format_str,
        handlers=handlers,
        force=True,
    )

    _log_initialized = True
    logger.info(f"日志系统初始化完成，日志文件：{log_file or '控制台'}")


class FetchError(Exception):
    """数据抓取异常基类"""
    pass


class PaginationError(FetchError):
    """分页错误"""
    pass


class APIError(FetchError):
    """API 响应错误"""
    pass


class BatchFetchError(FetchError):
    """批量获取错误，包含所有失败的批次信息"""

    def __init__(self, errors: list[tuple[int, Exception]]):
        """
        Args:
            errors: 失败批次列表，每个元素为 (batch_num, exception)
        """
        self.errors = errors
        error_msgs = [f"批次 {num}: {e}" for num, e in errors]
        super().__init__(f"批量获取失败 ({len(errors)} 个批次):\n" + "\n".join(error_msgs))


class ValidationError(FetchError):
    """参数验证错误"""
    pass


def validate_time_range(
    start_time: str,
    end_time: str,
) -> tuple[str, str]:
    """
    验证时间范围参数

    Args:
        start_time: 开始时间 (ISO 8601)
        end_time: 结束时间 (ISO 8601)

    Returns:
        验证后的 (start_time, end_time) 元组

    Raises:
        ValidationError: 时间格式无效或 start_time >= end_time
    """
    import re
    from datetime import datetime

    # ISO 8601 格式正则
    iso_pattern = re.compile(
        r"^\d{4}-\d{2}-\d{2}"  # 日期部分
        r"(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$"  # 可选的时间部分
    )

    def parse_time(t: str, name: str) -> datetime:
        if not iso_pattern.match(t):
            raise ValidationError(
                f"{name} 格式无效，应为 ISO 8601 格式 (如 2024-01-01 或 2024-01-01T00:00:00Z): {t}"
            )
        # 尝试解析
        try:
            # 尝试带时间解析
            if "T" in t:
                if t.endswith("Z"):
                    return datetime.fromisoformat(t.replace("Z", "+00:00"))
                return datetime.fromisoformat(t)
            else:
                return datetime.fromisoformat(t)
        except ValueError as e:
            raise ValidationError(f"{name} 解析失败: {e}") from e

    start_dt = parse_time(start_time, "start_time")
    end_dt = parse_time(end_time, "end_time")

    if start_dt >= end_dt:
        raise ValidationError(
            f"start_time ({start_time}) 必须早于 end_time ({end_time})"
        )

    return start_time, end_time


def build_session(
    total_retries: int = 3,
    backoff_factor: float = 0.5,
    pool_connections: int = 10,
    pool_maxsize: int = 10,
    headers: Optional[dict[str, str]] = None,
    timeout: float = 30.0,
) -> requests.Session:
    """
    构造带连接池和重试机制的 Session。

    Args:
        total_retries: 最大重试次数
        backoff_factor: 重试退避因子
        pool_connections: 连接池大小
        pool_maxsize: 每个 host 最大连接数
        headers: 默认请求头
        timeout: 默认超时时间（秒）

    Returns:
        配置好的 Session 对象
    """
    session = requests.Session()

    retry = Retry(
        total=total_retries,
        read=total_retries,
        connect=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
    )

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    if headers:
        session.headers.update(headers)

    return session


def fetch_all_pages(
    url: str,
    params: Optional[dict[str, Any]] = None,
    timeout: float = 30.0,
    max_pages: Optional[int] = None,
    max_retries: int = 3,
    rate_limit_delay: float = 0.0,
    verbose: bool = True,
    return_dataframe: bool = True,
    data_key: str = "data",
    next_page_key: str = "next_page_url",
    session: Optional[requests.Session] = None,
    headers: Optional[dict[str, str]] = None,
) -> pd.DataFrame | list[dict[str, Any]]:
    """
    通用分页抓取函数。

    Args:
        url: 初始请求 URL
        params: 仅第一页附带的 query 参数
        timeout: 单次请求超时秒数
        max_pages: 最大抓取页数，None 表示不限制
        max_retries: 单页最大重试次数
        rate_limit_delay: 请求间隔秒数（用于速率限制）
        verbose: 是否打印抓取进度
        return_dataframe: 是否返回 DataFrame，否则返回 list[dict]
        data_key: 返回 JSON 中数据列表对应字段名
        next_page_key: 返回 JSON 中下一页 URL 对应字段名
        session: 可选外部传入 Session，便于复用连接
        headers: 请求头（当 session 未设置时）

    Returns:
        pd.DataFrame 或 list[dict]

    Raises:
        PaginationError: 检测到循环分页或分页结构异常
        APIError: API 返回错误或请求失败
    """
    rows: list[dict[str, Any]] = []
    next_url: Optional[str] = url
    page = 0
    seen_urls: set[str] = set()

    own_session = session is None
    if own_session:
        default_headers = {"User-Agent": "coinmetrics-fetcher/1.0"}
        if headers:
            default_headers.update(headers)
        session = build_session(headers=default_headers, timeout=timeout)

    try:
        while next_url:
            if next_url in seen_urls:
                raise PaginationError(f"检测到循环分页：{next_url}")
            seen_urls.add(next_url)

            current_params = params if page == 0 else None

            for attempt in range(max_retries):
                try:
                    resp = session.get(next_url, params=current_params, timeout=timeout)
                    resp.raise_for_status()
                    payload = resp.json()
                    break
                except requests.Timeout as e:
                    if attempt == max_retries - 1:
                        raise APIError(f"请求超时，第 {page + 1} 页：{next_url}") from e
                    logger.warning(f"请求超时，重试 {attempt + 1}/{max_retries}")
                    time.sleep(1.0 * (2 ** attempt))
                except requests.RequestException as e:
                    if attempt == max_retries - 1:
                        raise APIError(f"请求失败，第 {page + 1} 页：{e}") from e
                    logger.warning(f"请求失败，重试 {attempt + 1}/{max_retries}: {e}")
                    time.sleep(1.0 * (2 ** attempt))
                except ValueError as e:
                    raise APIError(f"响应不是合法 JSON，第 {page + 1} 页") from e

            data = payload.get(data_key, [])
            if not isinstance(data, list):
                raise PaginationError(
                    f"字段 `{data_key}` 应为 list，实际类型：{type(data).__name__}"
                )

            rows.extend(data)
            page += 1

            if verbose:
                logger.info(f"page={page}, current_rows={len(data)}, total_rows={len(rows)}")

            if max_pages is not None and page >= max_pages:
                logger.info(f"达到 max_pages={max_pages}，停止抓取")
                break

            if rate_limit_delay > 0 and payload.get(next_page_key):
                time.sleep(rate_limit_delay)

            next_url = payload.get(next_page_key)

    finally:
        if own_session and session is not None:
            session.close()

    if return_dataframe:
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    return rows
