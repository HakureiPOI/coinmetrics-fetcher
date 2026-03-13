# coinmetrics-fetcher

CoinMetrics API v4 Python 客户端

## 安装

### 从 GitHub 安装（推荐）

```bash
pip install git+https://github.com/HakureiPOI/coinmetrics-fetcher.git
```

### 在 Google Colab 中使用

```python
!pip install git+https://github.com/HakureiPOI/coinmetrics-fetcher.git
```

### 本地开发安装

```bash
git clone https://github.com/HakureiPOI/coinmetrics-fetcher.git
cd coinmetrics-fetcher
pip install -e .
```

## 快速开始

### 1. 配置 API 密钥

**使用专业版 API**:

```python
import os
os.environ['COINMETRICS_API_KEY'] = 'your-api-key'

# 可选：启用日志
from config import setup_logging
setup_logging()
```

**使用社区版 API** (无需 API 密钥):

```python
import os
os.environ['COINMETRICS_USE_COMMUNITY_API'] = 'true'

# 或者在代码中直接指定
from api import FuturesDataFetcher
fetcher = FuturesDataFetcher(use_community_api=True)
```

### 2. 使用示例

```python
from api import FuturesDataFetcher

fetcher = FuturesDataFetcher()
df = fetcher.get_candles(
    exchange='deribit',
    base='btc',
    start_time='2024-01-01',
    end_time='2024-01-02'
)
df.head()
```

## 功能模块

| 模块 | 类 | 层级 | 功能 |
|------|-----|------|------|
| reference_data | ReferenceDataAPI | 一级 | 市场元数据查询 |
| timeseries | TimeseriesAPI | 一级 | 时间序列数据（K 线、Greeks、IV、资金费率） |
| options | OptionsDataFetcher | 二级 | 期权 Greeks 和隐含波动率 (IV) |
| funding_rates | FundingRateFetcher | 二级 | 永续合约资金费率（实际/预计） |
| futures | FuturesDataFetcher | 二级 | 期货 K 线数据 |

## 支持的 API 端点

**一级接口（直接调用）**:
- `/reference-data/markets` - 市场元数据
- `/timeseries/market-candles` - 市场 K 线数据
- `/timeseries/market-greeks` - 期权 Greeks 数据
- `/timeseries/market-implied-volatility` - 期权隐含波动率
- `/timeseries/market-funding-rates` - 资金费率
- `/timeseries/market-funding-rates-predicted` - 预计资金费率

**二级接口（高级封装）**:
- 期权数据获取器 - 自动获取市场列表 + 批量获取 Greeks/IV
- 资金费率获取器 - 自动筛选永续合约 + 批量获取费率
- 期货 K 线获取器 - 自动获取市场列表 + 批量获取 K 线

## API 使用指南

### 一级接口

```python
from api import ReferenceDataAPI, TimeseriesAPI

# 参考数据
ref = ReferenceDataAPI()
markets = ref.get_markets(exchange="deribit", market_type="option", base="btc")

# 时间序列
ts = TimeseriesAPI()

# K 线数据
candles = ts.get_market_candles(
    markets="deribit-btc-perp",
    start_time="2024-01-01",
    end_time="2024-01-02",
    frequency="1h"
)

# 期权 Greeks
greeks = ts.get_market_greeks(
    markets="deribit-BTC-27DEC24-50000-C-option",
    start_time="2024-01-01",
    granularity="1h"
)

# 隐含波动率
iv = ts.get_market_implied_volatility(
    markets="deribit-BTC-27DEC24-50000-C-option",
    start_time="2024-01-01"
)

# 资金费率
funding = ts.get_market_funding_rates(
    markets="deribit-BTC-PERPETUAL",
    start_time="2024-01-01"
)

# 预计资金费率
predicted = ts.get_market_funding_rates_predicted(
    markets="deribit-BTC-PERPETUAL",
    start_time="2024-01-01"
)
```

### 二级接口

```python
from api import OptionsDataFetcher, FundingRateFetcher, FuturesDataFetcher

# 期权数据
options = OptionsDataFetcher()
df = options.get_options_greeks_iv(
    exchange="deribit",
    base="btc",
    start_time="2024-01-01",
    end_time="2024-01-02",
    granularity="1m"
)

# 资金费率
funding = FundingRateFetcher()
df = funding.get_funding_rates(
    exchange="deribit",
    base="btc",
    start_time="2024-01-01",
    end_time="2024-01-31"
)

# 期货 K 线
futures = FuturesDataFetcher()
df = futures.get_candles(
    exchange="deribit",
    base="btc",
    start_time="2024-01-01",
    end_time="2024-01-02",
    frequency="1h"
)
```

**K 线频率支持**: `1m`, `5m`, `10m`, `15m`, `30m`, `1h`, `4h`, `1d`

## 社区版 API

CoinMetrics 提供免费的社区版 API，无需 API 密钥即可使用（部分数据可能受限）。

### 使用方式

**方式 1: 环境变量**
```python
import os
os.environ['COINMETRICS_USE_COMMUNITY_API'] = 'true'

from api import ReferenceDataAPI
ref = ReferenceDataAPI()  # 自动使用社区版
```

**方式 2: 构造函数参数**
```python
from api import ReferenceDataAPI, TimeseriesAPI, FuturesDataFetcher

# 一级接口
ref = ReferenceDataAPI(use_community_api=True)
ts = TimeseriesAPI(use_community_api=True)

# 二级接口
fetcher = FuturesDataFetcher(use_community_api=True)
```

**方式 3: 自定义配置**
```python
from config import Config, COMMUNITY_BASE_URL
from api import ReferenceDataAPI

config = Config(
    api_key="",  # 社区版不需要
    base_url=COMMUNITY_BASE_URL,
    use_community_api=True,
)
ref = ReferenceDataAPI(config=config)
```

### 社区版 vs 专业版

| 特性 | 社区版 | 专业版 |
|------|--------|--------|
| API 密钥 | 不需要 | 需要 |
| 速率限制 | 10 请求/6 秒 | 6000 请求/20 秒 |
| 数据范围 | 部分数据 | 全部数据 |
| Base URL | `community-api.coinmetrics.io` | `api.coinmetrics.io` |

## 内存缓存

一级接口 (`ReferenceDataAPI`, `TimeseriesAPI`) 内置内存缓存功能，可减少重复 API 请求。

```python
from api import ReferenceDataAPI, TimeseriesAPI
from utils import init_cache

# 初始化全局缓存（可选）
init_cache(default_ttl=300, max_size=1000)

# 一级接口默认启用缓存
ref = ReferenceDataAPI()  # 缓存 TTL 默认 3600 秒
ts = TimeseriesAPI()      # 缓存 TTL 默认 300 秒

# 禁用缓存
ref = ReferenceDataAPI(use_cache=False)
ts = TimeseriesAPI(use_cache=False)

# 自定义缓存时间
ref = ReferenceDataAPI(cache_ttl=7200)  # 2 小时
ts = TimeseriesAPI(cache_ttl=600)       # 10 分钟

# 单次请求禁用缓存
df = ts.get_market_candles(..., use_cache=False)

# 查看缓存统计
cache = ref.cache
print(cache.stats())  # {'size': 10, 'hits': 5, 'misses': 2, 'hit_rate': '71.43%'}

# 清空缓存
cache.clear()
```

**二级接口** (`FuturesDataFetcher`, `FundingRateFetcher`, `OptionsDataFetcher`) 会自动将缓存配置传递给底层 API：

```python
from api import FuturesDataFetcher

# 启用缓存（默认）
fetcher = FuturesDataFetcher(use_cache=True)

# 自定义缓存时间
fetcher = FuturesDataFetcher(
    use_cache=True,
    ref_cache_ttl=3600,   # 市场元数据缓存 1 小时
    ts_cache_ttl=300,     # K 线数据缓存 5 分钟
)
```

## 上下文管理器

```python
from api import FuturesDataFetcher

with FuturesDataFetcher() as fetcher:
    df = fetcher.get_candles("deribit", "btc", "2024-01-01", "2024-01-02")
# Session 自动关闭
```

## 错误处理

```python
from utils import BatchFetchError, ValidationError

try:
    df = fetcher.get_options_greeks_iv(...)
except ValidationError as e:
    print(f"参数错误：{e}")
except BatchFetchError as e:
    print(f"部分批次失败：{e.errors}")
```

## 自定义配置

```python
from config import Config
from api import FuturesDataFetcher

config = Config(
    api_key='your-api-key',
    timeout=60,
    max_retries=5,
)

fetcher = FuturesDataFetcher(config=config)
```

## 项目结构

```
coinmetrics-fetcher/
├── config.py               # 配置管理
├── pyproject.toml          # 项目配置
├── api/
│   ├── __init__.py
│   ├── base.py             # API 基类
│   ├── base_fetcher.py     # 数据获取器基类
│   ├── core/               # 一级接口 - 直接调用 API
│   │   ├── __init__.py
│   │   ├── reference_data.py   # /reference-data/markets
│   │   └── timeseries.py       # /timeseries/* 端点
│   └── fetchers/           # 二级接口 - 高级封装
│       ├── __init__.py
│       ├── options.py          # 期权数据获取器
│       ├── funding_rates.py    # 资金费率获取器
│       └── futures.py          # 期货数据获取器
└── utils/
    ├── __init__.py
    ├── fetch_utils.py      # 工具函数和异常类
    └── cache.py            # 内存缓存模块
```

## 注意事项

1. **API 密钥**: 需在 CoinMetrics 官网申请，并在代码中配置
2. **速率限制**: 社区版 10 请求/6 秒，专业版 6000 请求/20 秒
3. **并发控制**: 建议 `max_workers=4`，过高收益递减
4. **时间格式**: ISO 8601，如 `2024-01-01` 或 `2024-01-01T00:00:00Z`

## 相关资源

- [CoinMetrics API 文档](https://docs.coinmetrics.io/)
- [CoinMetrics 指标覆盖](https://coverage.coinmetrics.io/)
