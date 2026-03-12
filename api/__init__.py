"""
CoinMetrics API v4 Python 客户端高级封装模块

本模块提供 CoinMetrics API 的高级封装，简化数据获取流程。

## 模块结构

| 模块 | 类 | 功能 |
|------|-----|------|
| base | CoinMetricsAPI | 基础 API 类，提供分页请求、连接池管理 |
| reference_data | ReferenceDataAPI | 参考数据接口（资产、交易所、市场元数据） |
| timeseries | TimeseriesAPI | 时间序列接口（价格、K线、Greeks、IV等） |
| options | OptionsDataFetcher | 期权数据获取（市场列表、Greeks、IV） |
| funding_rates | FundingRateFetcher | 永续合约资金费率数据 |
| futures | FuturesDataFetcher | 期货 K 线数据 |

## 快速开始

### 1. 基础配置

```python
# 确保 .env 文件中配置了 COINMETRICS_API_KEY
from config import setup_logging
setup_logging()  # 可选：初始化日志系统
```

### 2. 参考数据查询

```python
from api import ReferenceDataAPI

api = ReferenceDataAPI()

# 获取所有交易所
exchanges = api.get_exchanges()

# 获取 Deribit BTC 期权市场
markets = api.get_markets(
    exchange="deribit",
    market_type="option",
    base="btc"
)

# 获取资产元数据
assets = api.get_assets(asset_id="btc,eth")
```

### 3. 时间序列数据

```python
from api import TimeseriesAPI

api = TimeseriesAPI()

# 获取 BTC 价格
df = api.get_asset_metrics(
    assets="btc",
    metrics="price_usd",
    start_time="2024-01-01",
    end_time="2024-01-31"
)

# 获取市场 K 线
df = api.get_market_candles(
    markets="deribit-btc-perp",
    start_time="2024-01-01"
)

# 获取期权 Greeks
df = api.get_market_greeks(
    markets="deribit-BTC-27DEC24-50000-C-option",
    start_time="2024-01-01",
    granularity="1h"
)
```

### 4. 期权数据（高级封装）

```python
from api import OptionsDataFetcher

fetcher = OptionsDataFetcher()

# 获取 Deribit BTC 期权列表
df = fetcher.get_deribit_btc_options(status="online")

# 获取 Greeks 和 IV 数据（并发优化）
df = fetcher.get_options_greeks_iv(
    exchange="deribit",
    base="btc",
    start_time="2024-01-01",
    end_time="2024-01-02",
    granularity="1m",
    max_workers=4
)
```

### 5. 资金费率数据

```python
from api import FundingRateFetcher

fetcher = FundingRateFetcher()

# 获取资金费率
df = fetcher.get_funding_rates(
    exchange="deribit",
    base="btc",
    start_time="2024-01-01",
    end_time="2024-01-31"
)

# 获取干净版（过滤 NaN）
df = fetcher.get_funding_rates_clean(
    exchange="deribit",
    base="btc",
    start_time="2024-01-01",
    end_time="2024-01-31"
)

# 获取预计资金费率
df = fetcher.get_predicted_funding_rates(
    exchange="deribit",
    base="btc",
    start_time="2024-01-01",
    end_time="2024-01-31"
)
```

### 6. 期货 K 线数据

```python
from api import FuturesDataFetcher

fetcher = FuturesDataFetcher()

# 获取所有期货的分钟级 K 线
df = fetcher.get_candles(
    exchange="deribit",
    base="btc",
    start_time="2024-01-01",
    end_time="2024-01-02"
)
```

## 便捷函数

对于简单场景，可直接使用便捷函数：

```python
from api import (
    get_deribit_btc_options,
    get_funding_rates,
    get_futures_candles,
)

# 一行获取并保存
df = get_deribit_btc_options(output_path="data/options.csv")
df = get_funding_rates("deribit", "btc", "2024-01-01", "2024-01-31", output_path="data/funding.csv")
df = get_futures_candles("deribit", "btc", "2024-01-01", "2024-01-02", output_path="data/candles.csv")
```

## 上下文管理器

所有 Fetcher 类都支持上下文管理器：

```python
from api import OptionsDataFetcher

with OptionsDataFetcher() as fetcher:
    df = fetcher.get_deribit_btc_options()
# Session 自动关闭
```

## 错误处理

```python
from utils import BatchFetchError, ValidationError

try:
    df = fetcher.get_options_greeks_iv(...)
except ValidationError as e:
    print(f"参数错误: {e}")
except BatchFetchError as e:
    print(f"部分批次失败: {e.errors}")
```

## 自定义配置

```python
from config import Config
from api import ReferenceDataAPI

config = Config(
    api_key="YOUR_API_KEY",
    timeout=60,
    max_retries=5,
)

api = ReferenceDataAPI(config=config)
```

## 注意事项

1. **API 密钥**: 需在 .env 中配置 COINMETRICS_API_KEY
2. **速率限制**: 社区版 10 请求/6 秒，专业版 6000 请求/20 秒
3. **并发控制**: 建议 max_workers=4，过高收益递减
4. **时间格式**: ISO 8601 格式，如 "2024-01-01" 或 "2024-01-01T00:00:00Z"
"""

from .base import CoinMetricsAPI
from .reference_data import ReferenceDataAPI
from .timeseries import TimeseriesAPI
from .options import OptionsDataFetcher, OptionFilter, get_deribit_btc_options
from .funding_rates import FundingRateFetcher, get_funding_rates
from .futures import FuturesDataFetcher, get_futures_candles

__all__ = [
    # 基础类
    "CoinMetricsAPI",
    # 参考数据
    "ReferenceDataAPI",
    # 时间序列
    "TimeseriesAPI",
    # 期权数据
    "OptionsDataFetcher",
    "OptionFilter",
    "get_deribit_btc_options",
    # 资金费率
    "FundingRateFetcher",
    "get_funding_rates",
    # 期货数据
    "FuturesDataFetcher",
    "get_futures_candles",
]
