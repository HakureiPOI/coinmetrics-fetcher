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

```python
import os
os.environ['COINMETRICS_API_KEY'] = 'your-api-key'

# 可选：启用日志
from config import setup_logging
setup_logging()
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

| 模块 | 类 | 功能 |
|------|-----|------|
| reference_data | ReferenceDataAPI | 参考数据（资产、交易所、市场元数据） |
| timeseries | TimeseriesAPI | 时间序列（价格、K线、Greeks、IV） |
| options | OptionsDataFetcher | 期权数据（市场列表、Greeks、IV） |
| funding_rates | FundingRateFetcher | 永续合约资金费率 |
| futures | FuturesDataFetcher | 期货 K 线数据 |

## API 使用指南

### 参考数据

```python
from api import ReferenceDataAPI

api = ReferenceDataAPI()

# 获取所有交易所
exchanges = api.get_exchanges()

# 获取 Deribit BTC 期权市场
markets = api.get_markets(exchange="deribit", market_type="option", base="btc")

# 获取资产元数据
assets = api.get_assets(asset_id="btc,eth")
```

### 时间序列

```python
from api import TimeseriesAPI

api = TimeseriesAPI()

# 获取 BTC 价格
df = api.get_asset_metrics(assets="btc", metrics="price_usd", start_time="2024-01-01", end_time="2024-01-31")

# 获取市场 K 线
df = api.get_market_candles(markets="deribit-btc-perp", start_time="2024-01-01")

# 获取期权 Greeks
df = api.get_market_greeks(markets="deribit-BTC-27DEC24-50000-C-option", start_time="2024-01-01", granularity="1h")
```

### 期权数据

```python
from api import OptionsDataFetcher

fetcher = OptionsDataFetcher()

# 获取期权列表
df = fetcher.get_deribit_btc_options(status="online")

# 获取 Greeks 和 IV 数据
df = fetcher.get_options_greeks_iv(
    exchange="deribit",
    base="btc",
    start_time="2024-01-01",
    end_time="2024-01-02",
    granularity="1m"
)
```

### 资金费率

```python
from api import FundingRateFetcher

fetcher = FundingRateFetcher()

# 获取资金费率
df = fetcher.get_funding_rates(exchange="deribit", base="btc", start_time="2024-01-01", end_time="2024-01-31")

# 获取预计资金费率
df = fetcher.get_predicted_funding_rates(exchange="deribit", base="btc", start_time="2024-01-01", end_time="2024-01-31")
```

### 期货 K 线

```python
from api import FuturesDataFetcher

fetcher = FuturesDataFetcher()

# 获取所有期货的分钟级 K 线
df = fetcher.get_candles(exchange="deribit", base="btc", start_time="2024-01-01", end_time="2024-01-02")
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
    print(f"参数错误: {e}")
except BatchFetchError as e:
    print(f"部分批次失败: {e.errors}")
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
│   ├── base.py             # 基础 API 类
│   ├── base_fetcher.py     # 数据获取器基类
│   ├── reference_data.py   # 参考数据接口
│   ├── timeseries.py       # 时间序列接口
│   ├── options.py          # 期权数据模块
│   ├── funding_rates.py    # 资金费率模块
│   └── futures.py          # 期货 K 线模块
└── utils/
    ├── __init__.py
    └── fetch_utils.py      # 工具函数和异常类
```

## 注意事项

1. **API 密钥**: 需在 CoinMetrics 官网申请，并在代码中配置
2. **速率限制**: 社区版 10 请求/6 秒，专业版 6000 请求/20 秒
3. **并发控制**: 建议 `max_workers=4`，过高收益递减
4. **时间格式**: ISO 8601，如 `2024-01-01` 或 `2024-01-01T00:00:00Z`

## 相关资源

- [CoinMetrics API 文档](https://docs.coinmetrics.io/)
- [CoinMetrics 指标覆盖](https://coverage.coinmetrics.io/)