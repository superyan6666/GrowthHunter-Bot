import requests
import pandas as pd
import pandas_ta as ta
import os
import datetime
import time
import json

# ================= 核心配置 =================
SYMBOL = "BTCUSDT"
TIMEFRAME = "15m"  
LIMIT = 500        
RR_RATIO = 2.0     
ACCOUNT_BALANCE = 10000  
RISK_PER_TRADE = 0.02    

# [新增] 工程运行配置
STATE_FILE = "trade_state.json"
DAEMON_MODE = False  # 设置为 True 则脚本会死循环运行，自动休眠等待下一个 K 线；False 则适合 crontab 单次触发

def wait_for_exact_kline():
    """[新增] 确保在 K 线彻底闭合后（+5秒）再执行计算，杜绝闪烁信号"""
    now = datetime.datetime.utcnow()
    minutes = now.minute
    seconds = now.second
    
    if DAEMON_MODE:
        # 持续运行模式：计算距离下一次 15m 闭合的精准秒数
        next_minute = ((minutes // 15) + 1) * 15
        next_time = now.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(minutes=next_minute, seconds=5)
        wait_seconds = (next_time - now).total_seconds()
        print(f"[{now.strftime('%H:%M:%S')}] 守护模式运行中，休眠 {wait_seconds:.1f} 秒至 {next_time.strftime('%H:%M:%S')}...")
        time.sleep(wait_seconds)
    else:
        # Crontab 模式：如果当前恰好在 0-4 秒被唤醒，等待到 5 秒确信数据入库
        if minutes % 15 == 0 and seconds < 5:
            sleep_time = 5 - seconds
            print(f"[{now.strftime('%H:%M:%S')}] K 线刚收盘，等待 {sleep_time} 秒确信数据落盘...")
            time.sleep(sleep_time)

def load_state():
    """[新增] 加载本地持久化状态"""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"读取状态文件失败: {e}，将初始化新状态")
    # 默认状态
    return {
        "last_action": "HOLD", 
        "last_push_time": "", 
        "simulated_position": 0, # 1 为做多，-1 为做空，0 为空仓
        "entry_price": 0.0
    }

def save_state(state):
    """[新增] 持久化状态到本地"""
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=4)

def fetch_binance_data(symbol, timeframe, limit):
    endpoints = [
        "https://data-api.binance.vision/api/v3/klines",
        "https://api1.binance.com/api/v3/klines",
        "https://api.binance.com/api/v3/klines"
    ]
    params = {"symbol": symbol, "interval": timeframe, "limit": limit}
    for attempt, url in enumerate(endpoints):
        try:
            if attempt > 0:
                time.sleep(2 ** attempt)
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status() 
            data = response.json()
            df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset', 'taker_buy_quote_asset', 'ignore'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].apply(pd.to_numeric)
            return df
        except Exception as e:
            continue
    raise Exception("所有 API 节点均连接失败。")

def calculate_indicators(df):
    df['SMA_20'] = ta.sma(df['close'], length=20)
    df['RSI_14'] = ta.rsi(df['close'], length=14)
    df['ATR_14'] = ta.atr(df['high'], df['low'], df['close'], length=14)
    macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
    df['MACD'] = macd['MACD_12_26_9']
    df['MACD_Hist'] = macd['MACDh_12_26_9']
    bbands = ta.bbands(df['close'], length=20, std=2)
    df['BB_Lower'] = bbands[[c for c in bbands.columns if c.startswith('BBL')][0]]
    df['BB_Middle'] = bbands[[c for c in bbands.columns if c.startswith('BBM')][0]]
    df['BB_Upper'] = bbands[[c for c in bbands.columns if c.startswith('BBU')][0]]
    adx = ta.adx(df['high'], df['low'], df['close'], length=14)
    df['ADX'] = adx['ADX_14']
    prev = df.iloc[-2]
    pivot = (prev['high'] + prev['low'] + prev['close']) / 3
    df['R1'] = (2 * pivot) - prev['low']
    df['S1'] = (2 * pivot) - prev['high']
    df['Vol_SMA_20'] = ta.sma(df['volume'], length=20)
    df['EMA_200'] = ta.ema(df['close'], length=200)
    return df

def safe_fmt(value, fmt="{:.2f}"):
    return "数据不足" if pd.isna(value) else fmt.format(value)

def build_ai_context(df):
    """构建序列化 JSON（此处折叠，保持原样）"""
    latest = df.iloc[-1]
    recent_5 = [{"time": str(df.iloc[i]['timestamp']), "close": round(df.iloc[i]['close'], 2), "volume": round(df.iloc[i]['volume'], 2), "macd_hist": round(df.iloc[i]['MACD_Hist'], 2) if not pd.isna(df.iloc[i]['MACD_Hist']) else 0} for i in range(-5, 0)]
    context = {
        "current_price": latest['close'], "macro_trend_ema200": latest['EMA_200'] if not pd.isna(latest['EMA_200']) else None,
        "trend_strength_adx14": latest['ADX'] if not pd.isna(latest['ADX']) else None,
        "vol_sma_20": latest['Vol_SMA_20'] if not pd.isna(latest['Vol_SMA_20']) else None,
        "recent_5_candles": recent_5
    }
    return json.dumps(context, ensure_ascii=False)

def call_llm_decision(context_json):
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key: return {"action": "HOLD", "confidence": 0, "reason": "由于当前不启用大模型，由内部策略引擎接管拦截。"}
    # ... 保留原有 LLM 请求逻辑 ...
    return {"action": "HOLD", "confidence": 0.0, "reason": "待完善"}

def get_internal_signal(df):
    """[提取] 如果不启用 LLM，退回使用内部信号引擎"""
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    current_price = latest['close']
    adx, ema_200, vol_sma_20 = latest['ADX'], latest['EMA_200'], latest['Vol_SMA_20']
    
    if pd.isna(adx) or pd.isna(ema_200): return {"action": "HOLD", "reason": "数据不足，持续观望"}
    
    macd_cross_up = latest['MACD_Hist'] > prev['MACD_Hist']
    macd_cross_down = latest['MACD_Hist'] < prev['MACD_Hist']
    vol_ratio = latest['volume'] / vol_sma_20 if vol_sma_20 > 0 else 0
    rsi = latest['RSI_14']
    s1, r1 = latest['S1'], latest['R1']
    
    if adx < 20:
        if current_price <= s1 * 1.002 and rsi < 30: return {"action": "BUY", "reason": "[内部信号] 震荡市触底S1且RSI超卖"}
        elif current_price >= r1 * 0.998 and rsi > 70: return {"action": "SELL", "reason": "[内部信号] 震荡市触顶R1且RSI超买"}
    elif adx > 25:
        if current_price > ema_200 and macd_cross_up and vol_ratio > 1.2 and current_price <= latest['BB_Middle']:
            return {"action": "BUY", "reason": "[内部信号] 牛市基调+动能转正+放量"}
        elif current_price < ema_200 and macd_cross_down and vol_ratio > 1.2 and current_price >= latest['BB_Middle']:
            return {"action": "SELL", "reason": "[内部信号] 熊市基调+动能转负+放量"}
            
    return {"action": "HOLD", "reason": "无明显高胜率结构，保持观望"}

def format_final_report(df, final_action, reason, alert_prefix):
    latest = df.iloc[-1]
    current_price = latest['close']
    atr = latest['ATR_14']
    
    if not pd.isna(atr) and atr > 0:
        sl_dist = 1.5 * atr
        pos_size_btc_val = (ACCOUNT_BALANCE * RISK_PER_TRADE) / sl_dist
        pos_size_btc = f"{pos_size_btc_val:.4f} BTC"
        sl_long_str = safe_fmt(current_price - sl_dist)
        tp_long_str = safe_fmt(current_price + (sl_dist * RR_RATIO))
        sl_short_str = safe_fmt(current_price + sl_dist)
        tp_short_str = safe_fmt(current_price - (sl_dist * RR_RATIO))
    else:
        pos_size_btc = sl_long_str = tp_long_str = sl_short_str = tp_short_str = "数据不足"

    time_str = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    action_icon = {"BUY": "🟢 做多 (BUY)", "SELL": "🔴 做空 (SELL)", "HOLD": "⚪ 观望 (HOLD)"}.get(final_action, "⚪ 观望")
    
    report = f"""{alert_prefix}
【BTC {TIMEFRAME} 实盘量化引擎】
时间: {time_str}
闭合确认价: {current_price:.2f}

[⚡ 当前决策]
- 指令: {action_icon}
- 逻辑: {reason}

[💰 资金与风控测算 (基于 {ACCOUNT_BALANCE}U 账户)]
- 建议开仓规模: {pos_size_btc}
- 若做多: 止损 {sl_long_str} | 止盈 {tp_long_str}
- 若做空: 止损 {sl_short_str} | 止盈 {tp_short_str}
    """
    return report

def push_to_dingtalk(message):
    webhook = os.getenv("DINGTALK_WEBHOOK")
    if not webhook: return
    headers = {'Content-Type': 'application/json'}
    payload = {"msgtype": "text", "text": {"content": message}}
    try:
        requests.post(webhook, headers=headers, json=payload, timeout=10)
    except Exception as e:
        print(f"钉钉推送失败: {e}")

def run_logic():
    df = fetch_binance_data(SYMBOL, TIMEFRAME, LIMIT)
    df = calculate_indicators(df)
    
    # 优先尝试 LLM，如果无 Key 则降级使用刚才抽象出来的内部信号引擎
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        context_json = build_ai_context(df)
        decision = call_llm_decision(context_json)
    else:
        decision = get_internal_signal(df)
        
    final_action = decision.get("action", "HOLD")
    reason = decision.get("reason", "")
    current_price = df.iloc[-1]['close']
    
    # === 告警去重与分级模块 ===
    state = load_state()
    last_action = state.get("last_action", "HOLD")
    should_push = False
    alert_prefix = ""

    if final_action != "HOLD" and final_action != last_action:
        should_push = True
        alert_prefix = "🚨 [强烈信号：方向反转/新趋势确立]\n"
        # 记录模拟持仓状态
        state["simulated_position"] = 1 if final_action == "BUY" else -1
        state["entry_price"] = float(current_price)
        
    elif final_action == "HOLD" and last_action != "HOLD":
        should_push = True
        alert_prefix = "⚠️ [状态变更：动能衰竭/回归观望]\n"
        state["simulated_position"] = 0
        
    elif final_action != "HOLD" and final_action == last_action:
        print(f"[{datetime.datetime.utcnow()}] 信号维持 {final_action}，跳过推送以免轰炸。")
        should_push = False
        
    else:
        print(f"[{datetime.datetime.utcnow()}] 持续观望中，无高价值信号。")
        should_push = False

    if should_push:
        final_report = format_final_report(df, final_action, reason, alert_prefix)
        print(final_report)
        push_to_dingtalk(final_report)
        
        # 刷新记忆
        state["last_action"] = final_action
        state["last_push_time"] = datetime.datetime.utcnow().isoformat()
        save_state(state)

def main():
    if DAEMON_MODE:
        print(f"启动守护进程模式 (每 15 分钟 K线闭合触发)...")
        while True:
            wait_for_exact_kline()
            try:
                run_logic()
            except Exception as e:
                print(f"单次执行异常: {e}")
    else:
        wait_for_exact_kline()
        run_logic()

if __name__ == "__main__":
    main()
