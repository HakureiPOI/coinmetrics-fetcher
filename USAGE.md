# 外部调用使用手册

本文档说明如何在其他项目（如 Google Colab、Jupyter Notebook、独立脚本）中调用 coinmetrics-fetcher。

---

## 方法一：从 GitHub 安装（推荐）

### 1. 安装

```bash
pip install git+https://github.com/your-username/coinmetrics-fetcher.git
```

或在 Colab/Notebook 中：

```python
!pip install git+https://github.com/your-username/coinmetrics-fetcher.git
```

### 2. 配置 API 密钥

**方式 A：环境变量**

```python
import os
os.environ['COINMETRICS_API_KEY'] = 'your-api-key-here'
```

**方式 B：直接传入 Config**

```python
from config import Config
from api import OptionsDataFetcher

config = Config(api_key='your-api-key-here')
fetcher = OptionsDataFetcher(config=config)
```

### 3. 使用示例

```python
from api import FuturesDataFetcher

fetcher = FuturesDataFetcher()
df = fetcher.get_candles(
    exchange='deribit',
    base='btc',
    start_time='2024-01-01',
    end_time='2024-01-02'
)
print(df.head())
```

---

## 方法二：复制源码使用

适用于无法访问 GitHub 或需要自定义修改的场景。

### 1. 复制文件

将以下文件/目录复制到你的项目中：

```
your-project/
├── config.py
├── api/
│   ├── __init__.py
│   ├── base.py
│   ├── reference_data.py
│   ├── timeseries.py
│   ├── options.py
│   ├── funding_rates.py
│   └── futures.py
└── utils/
    ├── __init__.py
    └── fetch_utils.py
```

### 2. 安装依赖

```bash
pip install requests pandas python-dotenv
```

### 3. 使用

```python
import os
os.environ['COINMETRICS_API_KEY'] = 'your-api-key-here'

from api import get_funding_rates

df = get_funding_rates(
    exchange='deribit',
    base='btc',
    start_time='2024-01-01',
    end_time='2024-01-31'
)
```

---

## 方法三：本地开发安装

适用于需要频繁修改源码的开发场景。

### 1. 克隆并安装

```bash
git clone https://github.com/your-username/coinmetrics-fetcher.git
cd coinmetrics-fetcher
pip install -e .
```

### 2. 配置

```bash
cp .env.example .env
# 编辑 .env 文件，填入 API 密钥
```

### 3. 使用

```python
from api import OptionsDataFetcher

fetcher = OptionsDataFetcher()
df = fetcher.get_deribit_btc_options()
```

---

## 完整示例：Google Colab

```python
# ========== Cell 1: 安装 ==========
!pip install git+https://github.com/your-username/coinmetrics-fetcher.git

# ========== Cell 2: 配置 API 密钥 ==========
import os
os.environ['COINMETRICS_API_KEY'] = 'your-api-key-here'

# 可选：启用日志
from config import setup_logging
setup_logging()

# ========== Cell 3: 获取期货 K 线 ==========
from api import FuturesDataFetcher

fetcher = FuturesDataFetcher()
df = fetcher.get_candles(
    exchange='deribit',
    base='btc',
    start_time='2024-01-01',
    end_time='2024-01-02'
)
print(f"获取到 {len(df)} 条记录")
df.head()

# ========== Cell 4: 获取资金费率 ==========
from api import FundingRateFetcher

fetcher = FundingRateFetcher()
df = fetcher.get_funding_rates_clean(
    exchange='deribit',
    base='btc',
    start_time='2024-01-01',
    end_time='2024-01-31'
)
df.head()

# ========== Cell 5: 获取期权 Greeks/IV ==========
from api import OptionsDataFetcher

fetcher = OptionsDataFetcher()
df = fetcher.get_options_greeks_iv(
    exchange='deribit',
    base='btc',
    start_time='2024-01-01',
    end_time='2024-01-02',
    granularity='1h'
)
df.head()

# ========== Cell 6: 保存数据 ==========
fetcher.save_to_csv(df, 'data/options_greeks.csv')
```

---

## 便捷函数速查

```python
from api import (
    get_deribit_btc_options,
    get_funding_rates,
    get_futures_candles,
)

# 期权列表
df = get_deribit_btc_options(output_path='options.csv')

# 资金费率
df = get_funding_rates('deribit', 'btc', '2024-01-01', '2024-01-31', output_path='funding.csv')

# 期货 K 线
df = get_futures_candles('deribit', 'btc', '2024-01-01', '2024-01-02', output_path='candles.csv')
```

---

## 错误处理

```python
from utils import BatchFetchError, ValidationError
from api import OptionsDataFetcher

fetcher = OptionsDataFetcher()

try:
    df = fetcher.get_options_greeks_iv(
        exchange='deribit',
        base='btc',
        start_time='2024-01-01',
        end_time='2024-01-02'
    )
except ValidationError as e:
    print(f"参数验证失败: {e}")
except BatchFetchError as e:
    print(f"部分批次失败:")
    for batch_num, error in e.errors:
        print(f"  批次 {batch_num}: {error}")
```

---

## 常见问题

### Q: 如何查看所有可用接口？

```python
import api
print(api.__all__)
```

### Q: 如何查看某个方法的文档？

```python
from api import FuturesDataFetcher
help(FuturesDataFetcher.get_candles)
```

### Q: 如何自定义超时、重试等参数？

```python
from config import Config
from api import FuturesDataFetcher

config = Config(
    api_key='your-api-key',
    timeout=60,           # 超时 60 秒
    max_retries=5,        # 最大重试 5 次
    page_size=5000,       # 分页大小
)

fetcher = FuturesDataFetcher(config=config)
```

### Q: 如何在不设置环境变量的情况下使用？

```python
from config import Config
from api import FuturesDataFetcher

# 直接传入 api_key，无需设置环境变量
config = Config(api_key='your-api-key')
fetcher = FuturesDataFetcher(config=config)
```

---

## 注意事项

1. **API 密钥安全**: 不要在公开代码中硬编码 API 密钥
2. **速率限制**: 社区版 10 请求/6 秒，专业版 6000 请求/20 秒
3. **并发控制**: 建议 `max_workers=4`，过高收益递减
4. **时间格式**: ISO 8601，如 `2024-01-01` 或 `2024-01-01T00:00:00Z`