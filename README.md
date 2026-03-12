# coinmetrics-fetcher

CoinMetrics API v4 Python 客户端封装

## 功能特性

- **配置管理**: 通过环境变量统一管理 API 配置
- **分页自动处理**: 自动遍历所有分页数据
- **连接池与重试**: 内置连接池和自动重试机制
- **日志系统**: 支持控制台和文件双输出
- **高级封装**: 提供便捷的期权数据获取接口

## 快速开始

### 1. 安装依赖

```bash
./venv/bin/pip install -r requirements.txt
```

### 2. 配置 API 密钥

```bash
cp .env.example .env
# 编辑 .env 文件，填入你的 COINMETRICS_API_KEY
```

### 3. 使用示例

```python
from api import OptionsDataFetcher

# 获取 Deribit BTC 期权数据
fetcher = OptionsDataFetcher()
df = fetcher.get_deribit_btc_options(status="online")

# 保存数据
fetcher.save_to_csv(df, "data/deribit_btc_options.csv")
```

## 配置说明

编辑 `.env` 文件：

```ini
# CoinMetrics API 密钥（必填）
COINMETRICS_API_KEY=YOUR_API_KEY

# API 基础 URL（可选）
COINMETRICS_BASE_URL=https://api.coinmetrics.io/v4

# 请求超时时间，单位秒（可选，默认 30）
COINMETRICS_TIMEOUT=30

# 分页大小，范围 1-10000（可选，默认 10000）
COINMETRICS_PAGE_SIZE=10000

# 最大重试次数（可选，默认 3）
COINMETRICS_MAX_RETRIES=3

# 请求间隔，用于速率限制（可选，默认 0）
COINMETRICS_RATE_LIMIT_DELAY=0

# 日志级别（可选，默认 INFO）
LOG_LEVEL=INFO

# 日志文件路径（可选）
LOG_FILE=logs/coinmetrics.log
```

## API 使用指南

### Reference Data API（参考数据）

```python
from api import ReferenceDataAPI

api = ReferenceDataAPI()

# 获取所有交易所
exchanges = api.get_exchanges()

# 获取特定交易所
deribit = api.get_exchanges(exchange_id="deribit")

# 获取 Deribit 所有 BTC 期权市场
options = api.get_markets(
    exchange="deribit",
    market_type="option",
    base="btc"
)

# 获取资产元数据
assets = api.get_assets(asset_id="btc,eth")

# 获取资产指标元数据
metrics = api.get_asset_metrics()
```

### Timeseries API（时间序列）

```python
from api import TimeseriesAPI

api = TimeseriesAPI()

# 获取 BTC 价格数据
df = api.get_asset_metrics(
    assets="btc",
    metrics="price_usd",
    start_time="2024-01-01",
    end_time="2024-12-31"
)

# 获取交易所指标
df = api.get_exchange_metrics(
    exchanges="deribit",
    metrics="volume_trusted_spot_usd_1d"
)

# 获取市场 K 线数据
df = api.get_market_candles(
    markets="deribit-btc-perp",
    start_time="2024-01-01"
)
```

### Options API（期权数据）

```python
from api import OptionsDataFetcher, OptionFilter

fetcher = OptionsDataFetcher()

# 方式 1: 便捷方法获取 Deribit BTC 期权
df = fetcher.get_deribit_btc_options(
    status="online"      # 仅在线交易
)

# 方式 2: 获取 Deribit ETH 期权
df = fetcher.get_deribit_eth_options(
    option_type="call",  # 仅 Call 期权
    quote="usd"          # 仅 USD 计价
)

# 方式 3: 自定义筛选条件
filter = OptionFilter(
    exchange="binance",
    base="btc",
    market_type="option",
    status="online"
)
df = fetcher.get_options(filter)

# 方式 4: 获取并保存
fetcher.fetch_and_save(
    filter=OptionFilter(exchange="deribit", base="btc"),
    output_path="data/options.csv"
)
```

### 便捷函数

```python
from api import get_deribit_btc_options

# 一行代码获取并保存
df = get_deribit_btc_options(
    output_path="data/deribit_btc_options.csv",
    status="online"
)
```

### 自定义配置

```python
from config import Config
from api import ReferenceDataAPI

# 创建自定义配置
config = Config(
    api_key="YOUR_API_KEY",
    page_size=5000,
    timeout=60,
    max_retries=5,
)

# 使用自定义配置
api = ReferenceDataAPI(config=config)
```

### 上下文管理器

```python
from api import ReferenceDataAPI

with ReferenceDataAPI() as api:
    df = api.get_exchanges()
# Session 自动关闭
```

## 项目结构

```
coinmetrics-fetcher/
├── .env                    # 环境变量配置（需自行创建）
├── .env.example            # 环境变量模板
├── .gitignore              # Git 忽略规则
├── config.py               # 配置管理模块
├── requirements.txt        # Python 依赖
├── README.md               # 项目文档
├── api/                    # API 高级封装模块
│   ├── __init__.py
│   ├── base.py             # 基础 API 类
│   ├── reference_data.py   # 参考数据接口（12 个）
│   ├── timeseries.py       # 时间序列接口（27 个）
│   └── options.py          # 期权数据获取模块
├── utils/
│   ├── __init__.py
│   └── fetch_utils.py      # 底层分页抓取工具
├── data/                   # 数据输出目录（.gitignore）
└── logs/                   # 日志目录（.gitignore）
```

## 可用接口统计

### Reference Data（12 个接口）

| 端点 | 说明 |
|------|------|
| `/reference-data/assets` | 资产元数据 |
| `/reference-data/exchanges` | 交易所元数据 |
| `/reference-data/markets` | 市场元数据 |
| `/reference-data/indexes` | 指数元数据 |
| `/reference-data/pairs` | 交易对元数据 |
| `/reference-data/asset-metrics` | 资产指标元数据 |
| `/reference-data/exchange-metrics` | 交易所指标元数据 |
| `/reference-data/exchange-asset-metrics` | 交易所资产指标元数据 |
| `/reference-data/exchange-pair-metrics` | 交易所交易对指标元数据 |
| `/reference-data/pair-metrics` | 交易对指标元数据 |
| `/reference-data/institution-metrics` | 机构指标元数据 |
| `/reference-data/market-metrics` | 市场指标元数据 |

### Timeseries（27 个接口）

主要接口包括：
- `/timeseries/asset-metrics` - 资产指标时间序列
- `/timeseries/exchange-metrics` - 交易所指标时间序列
- `/timeseries/market-metrics` - 市场指标时间序列
- `/timeseries/pair-metrics` - 交易对指标时间序列
- `/timeseries/pair-candles` - 交易对 K 线数据
- `/timeseries/market-candles` - 市场 K 线数据
- `/timeseries/market-trades` - 市场交易数据
- `/timeseries/market-orderbooks` - 市场订单簿
- `/timeseries/market-quotes` - 市场报价
- `/timeseries/market-funding-rates` - 资金费率
- `/timeseries/market-liquidations` - 清算数据
- `/timeseries/index-levels` - 指数点位
- `/timeseries/index-candles` - 指数 K 线

## 注意事项

1. **API 密钥安全**: 请勿将 `.env` 文件提交到版本控制
2. **page_size 限制**: 最大值为 10000（根据 CoinMetrics API 规范）
3. **速率限制**: 
   - 社区版：10 请求/6 秒
   - 专业版：6000 请求/20 秒
4. **并行限制**: 最多 10 个并行请求

## 相关资源

- [CoinMetrics API 文档](https://docs.coinmetrics.io/)
- [CoinMetrics 指标覆盖](https://coverage.coinmetrics.io/)
- [OpenAPI 规范](coinmetrics-openapi.json)
