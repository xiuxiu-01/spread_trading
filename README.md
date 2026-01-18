# 跨交易所黄金价差套利机器人（初版）

说明（中文）

这个项目是一个初步的跨交易所价差套利示例：
- 使用 MQL5 在 MT5 客户端输出 XAUUSD（现货黄金）价格到文件；
- 使用 Python（ccxt + TA-Lib + MetaTrader5 Python API）来读取 MT5 输出、获取 OKX 上的 `PAXG/USDT` 价格，计算价差并下套利单；

注意（重要）
- 这是一个教学/原型实现，真实环境下需加入更严格的风控、手续费/slippage 处理、并发/持仓同步等；
- 运行前需在本机安装并登录 MT5；Python 脚本通过官方 `MetaTrader5` 包连接本地 MT5；
- OKX/API Key 请在 `config` 中填写（不应把密钥存入版本库）；
- TA-Lib Python 绑定需要先安装系统级库（macOS: `brew install ta-lib`），然后 `pip install TA-Lib`；

文件说明
- `mql5/XAU_PAXG_Arb.mq5` - MQL5 专家顾问（EA），会在每个 tick 写入当前 XAUUSD 价格到 `mt5_xau_price.json` 并轮询 `mt5_cmd.json` 执行简单指令；
- `bot/arbitrage_bot.py` - Python 主程序，负责读取 MT5 的价格文件、通过 ccxt 获取 PAXG/USDT 价格、计算价差并执行交易；
- `requirements.txt` - Python 依赖列表；

快速开始（简要）
1. 在 MT5 终端中编译并加载 `XAU_PAXG_Arb.mq5` 到图表，允许文件写入（Files）和 WebRequest（若需要）；
2. 在系统上安装依赖：
   - 安装系统库：`brew install ta-lib`（macOS）
   - Python 包：`pip install -r requirements.txt`
3. 编辑 `bot/arbitrage_bot.py` 中的 `CONFIG`：填入 OKX API Key/Secret、调整符号名称（MT5 的 XAUUSD 符号可能不同，例如 `XAUUSD`、`XAUUSDmicro` 等）；
4. 运行 Python 程序：`python3 bot/arbitrage_bot.py`

风险提示
- 该示例未覆盖合约杠杆、保证金、跨平台结算、交易对手风险等；仅做学习演示用途。
1229630844    w2386y6    7351028  DUx6*eYk