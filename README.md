# 多交易所多品种价差套利系统 v2.0

## 功能特性

- **多品种支持**：XAUUSD（黄金）、XAGUSD（白银）、BTCUSD、ETHUSD 等
- **多交易所支持**：MT5、OKX、Binance、Bybit、Gate.io、Bitget、BitMart、LBank
- **可视化管理台**：Web UI 创建/管理套利任务
- **实时价差图表**：LightweightCharts 图表展示价差走势和 EMA
- **EMA 价差策略**：基于 EMA 的自动开平仓策略
- **Dry Run 模式**：模拟交易，不实际下单

## 目录结构

```
spread_trading/
├── src/                    # 新版模块化后端
│   ├── config/             # 配置（settings.py, symbols.py）
│   ├── models/             # 数据模型
│   ├── gateway/            # 交易所网关（MT5, CCXT）
│   ├── core/               # 核心逻辑（K线聚合, 策略）
│   ├── services/           # 服务层（ArbitrageManager, OrderService）
│   ├── server/             # WebSocket 服务器
│   ├── web/                # Web 前端
│   │   ├── admin.html      # 管理台页面
│   │   └── index.html      # 图表页面
│   └── main.py             # 入口
├── bot/                    # 旧版后端（兼容保留）
├── gateway/                # 旧版网关
└── web/                    # 旧版前端
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 启动后端

```bash
# 新版模块化后端
python -m src.main

# 或者旧版后端
python bot/serve_ws_backend.py
```


Get-NetTCPConnection -LocalPort 8766 -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }; Start-Sleep 2; cd c:\Users\Administrator\Desktop\workspace\spread_trading; python -m src.main
cd c:\Users\Administrator\Desktop\workspace\spread_trading\src\web
python -m http.server 8080

### 3. 打开管理台

浏览器访问 `http://localhost:8766` 或直接打开：
- `src/web/admin.html` - 管理台
- `src/web/index.html` - 图表页面

### 4. 创建套利任务

1. 点击 "New Arbitrage" 按钮
2. 选择交易品种（如 XAUUSD）
3. 选择两个交易所（如 MT5 和 OKX）
4. 设置策略参数
5. 点击 "Create Task" 开始套利

## 品种符号映射

| 品种 | MT5 | OKX | Binance | Bybit | Gate | Bitget |
|------|-----|-----|---------|-------|------|--------|
| 黄金 | XAUUSD | XAU/USDT:USDT | PAXG/USDT | XAUUSDT | PAXG_USDT | XAUUSDT |
| 白银 | XAGUSD | XAG/USDT:USDT | - | XAGUSDT | - | XAGUSDT |
| BTC | BTCUSD | BTC/USDT:USDT | BTC/USDT | BTCUSDT | BTC_USDT | BTCUSDT |
| ETH | ETHUSD | ETH/USDT:USDT | ETH/USDT | ETHUSDT | ETH_USDT | ETHUSDT |

## 策略参数说明

- **EMA Period**: EMA 周期（默认 20）
- **First Spread**: L1 入场价差阈值
- **Next Spread**: L2+ 加仓价差阈值
- **Take Profit**: 止盈价差阈值
- **Max Positions**: 最大持仓层数
- **Trade Volume**: 每次交易手数

## 风险提示

- 该系统仅供学习研究使用
- 真实交易请充分测试并做好风控
- 默认开启 Dry Run 模式，不会实际下单