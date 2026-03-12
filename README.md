# coinmetrics-fetcher

CoinMetrics API v4 Python 客户端封装

## 功能特性

- **配置管理**: 通过环境变量统一管理 API 配置
- **分页自动处理**: 自动遍历所有分页数据
- **连接池与重试**: 内置连接池和自动重试机制
- **日志系统**: 支持控制台和文件双输出
- **并发请求**: 线程池并发获取数据，性能提升 60%+
- **高级封装**: 提供便捷的期权 Greeks 和 IV 数据获取接口

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

## 日志配置

日志系统支持同时输出到控制台和文件：

```python
from config import setup_logging

# 初始化日志（自动从 .env 读取配置）
setup_logging()

# 之后所有 API 操作都会记录到日志文件
from api import OptionsDataFetcher
fetcher = OptionsDataFetcher()
df = fetcher.get_options_greeks_iv(
    exchange="deribit",
    base="btc",
    start_time="2024-01-01",
    end_time="2024-01-02",
)
```

日志文件位置：`logs/coinmetrics.log`

查看实时日志：
```bash
tail -f logs/coinmetrics.log
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

# 获取期权 Greeks 数据
df = api.get_market_greeks(
    markets="deribit-BTC-27DEC24-50000-C-option",
    start_time="2024-01-01",
    end_time="2024-01-31",
    granularity="1h",
)

# 获取期权 IV 数据
df = api.get_market_implied_volatility(
    markets="deribit-BTC-27DEC24-50000-C-option",
    start_time="2024-01-01",
    end_time="2024-01-31",
)
```

### Funding Rates API（永续合约资金费率）

#### 1. 获取资金费率数据

```python
from api import FundingRateFetcher

fetcher = FundingRateFetcher()

# 获取 Deribit BTC 永续合约资金费率
df = fetcher.get_funding_rates(
    exchange="deribit",
    base="btc",
    start_time="2024-01-01",
    end_time="2024-01-31",
)

# 获取预计资金费率
df = fetcher.get_predicted_funding_rates(
    exchange="deribit",
    base="btc",
    start_time="2024-01-01",
    end_time="2024-01-31",
)
```

#### 2. 同时获取资金费率 + 预计资金费率

```python
from api import FundingRateFetcher

fetcher = FundingRateFetcher()

# 同时获取两者并自动合并
df = fetcher.get_all_funding_rates(
    exchange="deribit",
    base="btc",
    start_time="2024-01-01",
    end_time="2024-01-31",
    
    # 并发控制
    batch_size=50,        # 每批请求的市场数量
    max_workers=4,        # 最大并发数
    verbose=True,         # 是否打印进度
)

# 输出字段:
# - market: 市场标识符
# - time: 时间戳
# - symbol: 合约符号 (如 BTC-PERPETUAL)
# - pair: 交易对 (如 btc-usd)
# - funding_rate: 资金费率
# - rate_predicted: 预计资金费率
# - database_time: 数据库时间
# - period: 计息周期
# - interval: 计息间隔
```

#### 3. 便捷函数

```python
from api import get_funding_rates

# 一行代码获取并保存
df = get_funding_rates(
    exchange="deribit",
    base="btc",
    start_time="2024-01-01",
    end_time="2024-01-31",
    output_path="data/funding_rates.csv",
)
```

### Options API（期权数据）

#### 1. 获取期权市场列表

```python
from api import OptionsDataFetcher

fetcher = OptionsDataFetcher()

# 获取 Deribit BTC 期权
df = fetcher.get_deribit_btc_options(
    status=None,        # None=全部，"online"=在线，"offline"=已下线
    option_type=None,   # None=全部，"call"=看涨，"put"=看跌
    quote=None,         # None=全部，"usd"、"usdc" 等
)

# 获取 Deribit ETH 期权
df = fetcher.get_deribit_eth_options()

# 获取任意交易所期权
df = fetcher.get_exchange_options(
    exchange="binance",
    base="btc",
)
```

#### 2. 获取期权 Greeks 和 IV 数据（进阶功能）

```python
from api import OptionsDataFetcher

fetcher = OptionsDataFetcher()

# 获取 Deribit BTC 期权在指定时间范围内的 Greeks 和 IV 数据
df = fetcher.get_options_greeks_iv(
    exchange="deribit",
    base="btc",
    start_time="2024-01-01",
    end_time="2024-01-31",
    
    # 可选过滤
    option_type=None,       # None/"call"/"put"
    status=None,            # None/"online"/"offline"（历史数据建议用 None）
    
    # 数据粒度
    granularity="1m",       # "1m"=分钟频，"1h"=小时频，"1d"=日频
    
    # 批量控制
    batch_size=100,         # 每批请求的期权数量
    max_workers=4,          # 并发数，默认 4（可提升 60% 性能）
    verbose=True,           # 是否打印进度
)

# 输出列：
# - market: 市场标识符
# - time: 时间戳
# - option_contract_type: 期权类型 (call/put)
# - strike: 行权价
# - expiration: 到期时间
# - delta, gamma, theta, vega, rho: Greeks 指标
# - iv_mark, iv_bid, iv_ask: 隐含波动率
```

**性能对比**（24 小时数据，113 万条记录）：

| 并发数 | 用时 | 速度 | 提升 |
|--------|------|------|------|
| 串行 (workers=1) | 390 秒 (6.5 分钟) | 2,907 条/秒 | - |
| 并发 (workers=4) | 144 秒 (2.4 分钟) | 7,866 条/秒 | **+63%** |

**注意事项**：
- `status=None`（默认）：包含所有期权（推荐用于历史数据查询）
- `status="online"`：仅在线交易的期权（已到期期权会被过滤）

#### 3. 使用 OptionFilter 自定义筛选

```python
from api import OptionsDataFetcher, OptionFilter

fetcher = OptionsDataFetcher()

filter = OptionFilter(
    exchange="deribit",
    base="btc",
    market_type="option",
    quote="usd",
    option_type="call",
    status="online",
)

df = fetcher.get_options(filter)
```

#### 4. 便捷函数

```python
from api import get_deribit_btc_options

# 一行代码获取并保存
df = get_deribit_btc_options(
    output_path="data/deribit_btc_options.csv",
    status="online",
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
    log_level="DEBUG",
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
│   ├── timeseries.py       # 时间序列接口（27 个 + Greeks/IV）
│   └── options.py          # 期权数据获取模块（并发优化）
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

### Timeseries（29 个接口）

**时间序列数据**：
- `/timeseries/asset-metrics` - 资产指标时间序列
- `/timeseries/exchange-metrics` - 交易所指标时间序列
- `/timeseries/market-metrics` - 市场指标时间序列
- `/timeseries/pair-metrics` - 交易对指标时间序列

**K 线和交易数据**：
- `/timeseries/pair-candles` - 交易对 K 线数据
- `/timeseries/market-candles` - 市场 K 线数据
- `/timeseries/market-trades` - 市场交易数据
- `/timeseries/market-orderbooks` - 市场订单簿
- `/timeseries/market-quotes` - 市场报价

**衍生品数据**：
- `/timeseries/market-funding-rates` - 资金费率
- `/timeseries/market-funding-rates-predicted` - 预测资金费率
- `/timeseries/market-liquidations` - 清算数据
- `/timeseries/market-openinterest` - 未平仓合约
- `/timeseries/market-greeks` - **期权 Greeks** (delta/gamma/theta/vega/rho)
- `/timeseries/market-implied-volatility` - **期权隐含波动率** (IV)

**指数数据**：
- `/timeseries/index-levels` - 指数点位
- `/timeseries/index-candles` - 指数 K 线
- `/timeseries/index-constituents` - 指数成分

## 注意事项

1. **API 密钥安全**: 请勿将 `.env` 文件提交到版本控制
2. **page_size 限制**: 最大值为 10000（根据 CoinMetrics API 规范）
3. **速率限制**:
   - 社区版：10 请求/6 秒
   - 专业版：6000 请求/20 秒
4. **并行限制**: 最多 10 个并行请求
5. **历史数据查询**: 使用 `get_options_greeks_iv()` 时，建议 `status=None`（默认），因为已到期期权的状态为 `offline`
6. **并发性能**: `max_workers=4` 可获得约 60% 性能提升，过高并发数收益递减

## 相关资源

- [CoinMetrics API 文档](https://docs.coinmetrics.io/)
- [CoinMetrics 指标覆盖](https://coverage.coinmetrics.io/)
- [OpenAPI 规范](coinmetrics-openapi.json)
