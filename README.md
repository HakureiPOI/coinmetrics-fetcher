# CoinMetrics Fetcher

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

CoinMetrics API v4 的 Python 客户端封装，提供简洁易用的接口获取加密货币市场数据。

## ✨ 特性

- **双模式支持** - 自动支持社区版（免费）和专业版 API
- **内存缓存** - 内置缓存机制，减少重复请求
- **并发获取** - 支持批量并发请求，高效获取数据
- **类型安全** - 自动类型转换，时间/数值列智能识别
- **开箱即用** - 封装常用接口，无需处理分页和错误重试

## 📦 安装

### 从 GitHub 安装

```bash
pip install git+https://github.com/HakureiPOI/coinmetrics-fetcher.git
```

### 本地开发安装

```bash
git clone https://github.com/HakureiPOI/coinmetrics-fetcher.git
cd coinmetrics-fetcher
pip install -e .
```

## 🚀 快速开始

### 1. 配置 API

**专业版**（需要 API 密钥）:
```python
import os
os.environ['COINMETRICS_API_KEY'] = 'your-api-key'
```

**社区版**（免费，无需密钥）:
```python
# 不设置 API key 即自动使用社区版
# 或显式设置
os.environ['COINMETRICS_USE_COMMUNITY_API'] = 'true'
```

> **注意**: 日志系统会自动初始化，默认输出到控制台（INFO 级别）。
> 如需自定义日志配置，见 [高级配置 - 日志配置](#日志配置)。

### 2. 获取数据

```python
from api import FuturesDataFetcher

# 创建获取器（自动使用社区版或专业版）
fetcher = FuturesDataFetcher()

# 获取期货 K 线
df = fetcher.get_candles(
    exchange='deribit',
    base='btc',
    start_time='2024-01-01',
    end_time='2024-01-02',
    frequency='1h'
)
print(df.head())
```

## 📚 功能模块

### 一级接口（直接调用 API）

| 类 | 功能 |
|----|------|
| `ReferenceDataAPI` | 市场元数据查询 |
| `TimeseriesAPI` | 时间序列数据（K 线、Greeks、IV、资金费率） |

### 二级接口（高级封装）

| 类 | 功能 |
|----|------|
| `SpotDataFetcher` | 现货 K 线数据 |
| `FuturesDataFetcher` | 期货 K 线数据 |
| `OptionsDataFetcher` | 期权 Greeks 和隐含波动率 |
| `FundingRateFetcher` | 永续合约资金费率 |

## 📖 使用示例

### 现货 K 线

```python
from api import SpotDataFetcher

spot = SpotDataFetcher()

# 获取 BTC 现货 K 线
df = spot.get_candles(
    exchange='binance',
    base='btc',
    start_time='2024-01-01',
    end_time='2024-01-02',
    frequency='1h'
)

# 指定计价货币
df = spot.get_candles(
    exchange='binance',
    base='btc',
    quote='usdt',  # 只获取 BTC/USDT
    start_time='2024-01-01',
    end_time='2024-01-02',
    frequency='1h'
)
```

### 期货 K 线

```python
from api import FuturesDataFetcher

futures = FuturesDataFetcher()

df = futures.get_candles(
    exchange='deribit',
    base='btc',
    start_time='2024-01-01',
    end_time='2024-01-02',
    frequency='1h'
)
```

### 期权 Greeks 和 IV

```python
from api import OptionsDataFetcher

options = OptionsDataFetcher()

# 获取 Deribit BTC 期权的 Greeks 和隐含波动率
df = options.get_options_greeks_iv(
    exchange='deribit',
    base='btc',
    start_time='2024-01-01',
    end_time='2024-01-02',
    granularity='1h',      # 数据粒度：raw, 1m, 1h, 1d
    batch_size=100,        # 每批市场数
    max_workers=4          # 并发数
)
```

### 资金费率

```python
from api import FundingRateFetcher

funding = FundingRateFetcher()

# 获取实际资金费率
df = funding.get_funding_rates(
    exchange='deribit',
    base='btc',
    start_time='2024-01-01',
    end_time='2024-01-31'
)

# 获取预计资金费率
df = funding.get_predicted_funding_rates(
    exchange='deribit',
    base='btc',
    start_time='2024-01-01',
    end_time='2024-01-31'
)
```

### 直接使用一级接口

```python
from api import ReferenceDataAPI, TimeseriesAPI

# 查询市场元数据
ref = ReferenceDataAPI()
markets = ref.get_markets(
    exchange='deribit',
    type='option',
    base='btc'
)

# 查询时间序列数据
ts = TimeseriesAPI()

# K 线数据
candles = ts.get_market_candles(
    markets='deribit-btc-perp',
    start_time='2024-01-01',
    end_time='2024-01-02',
    frequency='1h'
)

# 期权 Greeks
greeks = ts.get_market_greeks(
    markets='deribit-BTC-27DEC24-50000-C-option',
    start_time='2024-01-01',
    granularity='1h'
)

# 隐含波动率
iv = ts.get_market_implied_volatility(
    markets='deribit-BTC-27DEC24-50000-C-option',
    start_time='2024-01-01'
)

# 资金费率
funding = ts.get_market_funding_rates(
    markets='deribit-BTC-PERPETUAL',
    start_time='2024-01-01'
)
```

## ⚙️ 高级配置

### 日志配置

日志系统会自动初始化，默认配置：
- **输出**: 控制台（彩色格式）
- **级别**: INFO
- **文件**: 无

**环境变量配置**:
```bash
# 设置日志级别
export LOG_LEVEL=DEBUG

# 输出到文件
export LOG_FILE=logs/coinmetrics.log
```

**代码中配置**:
```python
from config import setup_logging

# 自定义日志级别和文件
setup_logging(level="DEBUG", log_file="logs/app.log")
```

### 缓存配置

```python
from api import TimeseriesAPI
from utils import init_cache

# 初始化全局缓存
init_cache(default_ttl=300, max_size=1000)

# 禁用缓存
api = TimeseriesAPI(use_cache=False)

# 自定义缓存时间
api = TimeseriesAPI(cache_ttl=600)  # 10 分钟

# 单次请求禁用缓存
df = api.get_market_candles(..., use_cache=False)

# 查看缓存统计
print(api.cache.stats())
# {'size': 10, 'hits': 5, 'misses': 2, 'hit_rate': '71.43%'}
```

### 自定义配置

```python
from config import Config
from api import FuturesDataFetcher

config = Config(
    api_key='your-api-key',
    timeout=60,
    max_retries=5,
    page_size=5000,
)

fetcher = FuturesDataFetcher(config=config)
```

### 上下文管理器

```python
from api import FuturesDataFetcher

with FuturesDataFetcher() as fetcher:
    df = fetcher.get_candles('deribit', 'btc', '2024-01-01', '2024-01-02')
# Session 自动关闭
```

### 错误处理

```python
from utils import BatchFetchError, ValidationError

try:
    df = fetcher.get_options_greeks_iv(...)
except ValidationError as e:
    print(f"参数错误：{e}")
except BatchFetchError as e:
    print(f"部分批次失败：{e.errors}")
```

## 📊 支持的 API 端点

| 端点 | 说明 |
|------|------|
| `/reference-data/markets` | 市场元数据 |
| `/timeseries/market-candles` | K 线数据 |
| `/timeseries/market-greeks` | 期权 Greeks |
| `/timeseries/market-implied-volatility` | 隐含波动率 |
| `/timeseries/market-funding-rates` | 资金费率 |
| `/timeseries/market-funding-rates-predicted` | 预计资金费率 |

## 📝 参数说明

### K 线频率

| 频率 | 说明 |
|------|------|
| `1m`, `5m`, `10m`, `15m`, `30m` | 分钟级 K 线 |
| `1h`, `4h` | 小时级 K 线 |
| `1d` | 日 K 线 |

### Greeks/IV 粒度

| 粒度 | 说明 |
|------|------|
| `raw` | 原始数据 |
| `1m`, `1h`, `1d` | 聚合数据 |

### 时间格式

使用 ISO 8601 格式：
- `2024-01-01`
- `2024-01-01T00:00:00Z`
- `2024-01-01T00:00:00+00:00`

## 🔧 项目结构

```
coinmetrics-fetcher/
├── config.py               # 配置管理
├── pyproject.toml          # 项目配置
├── api/
│   ├── core/               # 一级接口
│   │   ├── reference_data.py
│   │   └── timeseries.py
│   ├── fetchers/           # 二级接口
│   │   ├── spot.py
│   │   ├── futures.py
│   │   ├── options.py
│   │   └── funding_rates.py
│   ├── base.py
│   └── base_fetcher.py
└── utils/
    ├── fetch_utils.py
    └── cache.py
```

## ⚠️ 注意事项

1. **API 密钥**: 专业版需在 [CoinMetrics](https://coinmetrics.io/) 申请
2. **速率限制**:
   - 社区版：10 请求/6 秒
   - 专业版：6000 请求/20 秒
3. **并发建议**: `max_workers=4` 为佳，过高收益递减
4. **社区版限制**: 社区版 API 仅支持参考数据端点（如 `/reference-data/markets`），时间序列端点（K 线、Greeks、IV、资金费率）需要专业版 API

## 🔗 相关资源

- [CoinMetrics API 文档](https://docs.coinmetrics.io/)
- [CoinMetrics 指标覆盖](https://coverage.coinmetrics.io/)
- [GitHub 仓库](https://github.com/HakureiPOI/coinmetrics-fetcher)

## 📄 许可证

MIT License
