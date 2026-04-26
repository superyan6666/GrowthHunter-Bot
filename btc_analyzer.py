import requests
import pandas as pd
import pandas_ta as ta
import os
import datetime
import time
import json

# ================= 核心配置 =================
SYMBOL = "BTCUSDT"       # 恢复为 Bitget/币安 的交易对格式
TIMEFRAME = "15m"        # 恢复为 Bitget 的时间周期格式
LIMIT = 500        
RR_RATIO = 2.0     
ACCOUNT_BALANCE = 10000  
RISK_PER_TRADE = 0.02    

# 工程运行配置
STATE_FILE = "trade_state.json"
DAEMON_MODE = False  # True=后台常驻死循环，False=Crontab单次触发

def wait_for_exact_kline():
    """确保在 K 线彻底闭合后（+5秒）再执行计算，杜绝闪烁信号"""
    now = datetime.datetime.now(datetime.UTC)
    minutes = now.minute
    seconds = now.second
    
    if DAEMON_MODE:
        next_minute = ((minutes // 15) + 1) * 15
        next_time = now.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(minutes=next_minute, seconds=5)
        wait_seconds = (next_time - now).total_seconds()
        print(f"[{now.strftime('%H:%M:%S')}] 守护模式休眠 {wait_seconds:.1f} 秒至 {next_time.strftime('%H:%M:%S')}...")
        time.sleep(wait_seconds)
    else:
        if minutes % 15 == 0 and seconds < 5:
            sleep_time = 5 - seconds
            print(f"[{now.strftime('%H:%M:%S')}] K 线刚收盘，等待 {sleep_time} 秒确信数据落盘...")
            time.sleep(sleep_time)

def load_state():
    """加载本地持久化状态"""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"读取状态文件失败: {e}，将初始化新状态")
    return {
        "last_action": "HOLD", 
        "last_push_time": "", 
        "simulated_position": 0,
        "entry_price": 0.0
    }

def save_state(state):
    """持久化状态到本地"""
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=4)

def fetch_bitget_data(symbol, timeframe, limit):
    """通过 Bitget V2 公共 API 获取 K 线数据 (稳定免签抗封锁)"""
    url = "https://api.bitget.com/api/v2/spot/market/candles"
    bg_timeframe = timeframe.replace('m', 'min') if timeframe.endswith('m') else timeframe
    params = {"symbol": symbol, "granularity": bg_timeframe, "limit": limit}
    
    for attempt in range(3):
        try:
            if attempt > 0:
                time.sleep(2 ** attempt)
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status() 
            data = response.json()
            
            if str(data.get("code")) != "00000":
                raise Exception(f"Bitget API 返回错误: {data.get('msg')}")
                
            bars = data.get("data", [])
            if not bars:
                raise Exception("Bitget 返回数据为空")
                
            # 关键切片修复：防止 API 返回列数异动
            df = pd.DataFrame(bars)
            df = df.iloc[:, :6] 
            df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            
            df['timestamp'] = pd.to_datetime(pd.to_numeric(df['timestamp']), unit='ms')
            df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].apply(pd.to_numeric)
            
            # 翻转倒序数组
            df = df.sort_values('timestamp').reset_index(drop=True)
            return df
        except Exception as e:
            print(f"Bitget API 尝试 {attempt+1} 失败: {e}")
            continue
            
    raise Exception("Bitget API 连续连接失败，请检查网络。")

def calculate_indicators(df):
    """计算核心技术指标"""
    df['SMA_20'] = ta.sma(df['close'], length=20)
    df['RSI_14'] = ta.rsi(df['close'], length=14)
    df['ATR_14'] = ta.atr(df['high'], df['low'], df['close'], length=14)
    
    # MACD
    macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
    df['MACD'] = macd['MACD_12_26_9']
    df['MACD_Hist'] = macd['MACDh_12_26_9']
    
    # 布林带
    bbands = ta.bbands(df['close'], length=20, std=2)
    df['BB_Lower'] = bbands[[c for c in bbands.columns if c.startswith('BBL')][0]]
    df['BB_Middle'] = bbands[[c for c in bbands.columns if c.startswith('BBM')][0]]
    df['BB_Upper'] = bbands[[c for c in bbands.columns if c.startswith('BBU')][0]]
    
    # 变盘检测指标
    df['BB_Width'] = (df['BB_Upper'] - df['BB_Lower']) / df['BB_Middle']
    df['BB_Width_Min_50'] = df['BB_Width'].rolling(window=50).min()
    
    # 趋势强度
    adx = ta.adx(df['high'], df['low'], df['close'], length=14)
    df['ADX'] = adx['ADX_14']
    
    # [MTF 优化] 引入大周期模拟 (EMA144 约等于 1H 级别的 EMA36)
    df['EMA_HTF_Proxy'] = ta.ema(df['close'], length=144)
    df['EMA_200'] = ta.ema(df['close'], length=200)
    
    # 支撑阻力
    prev = df.iloc[-2]
    pivot = (prev['high'] + prev['low'] + prev['close']) / 3
    df['R1'] = (2 * pivot) - prev['low']
    df['S1'] = (2 * pivot) - prev['high']
    df['Vol_SMA_20'] = ta.sma(df['volume'], length=20)
    
    return df

def safe_fmt(value, fmt="{:.2f}"):
    return "数据不足" if pd.isna(value) else fmt.format(value)

def build_ai_context(df):
    """构建序列化 JSON 喂给大模型"""
    latest = df.iloc[-1]
    recent_5 = [{"time": str(df.iloc[i]['timestamp']), "close": round(df.iloc[i]['close'], 2), "volume": round(df.iloc[i]['volume'], 2), "macd_hist": round(df.iloc[i]['MACD_Hist'], 2) if not pd.isna(df.iloc[i]['MACD_Hist']) else 0} for i in range(-5, 0)]
    context = {
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "current_price": latest['close'], 
        "macro_trend_ema200": latest['EMA_200'] if not pd.isna(latest['EMA_200']) else None,
        "htf_trend_proxy": latest['EMA_HTF_Proxy'] if not pd.isna(latest['EMA_HTF_Proxy']) else None,
        "trend_strength_adx14": latest['ADX'] if not pd.isna(latest['ADX']) else None,
        "volatility_atr14": latest['ATR_14'] if not pd.isna(latest['ATR_14']) else None,
        "recent_5_candles": recent_5
    }
    return json.dumps(context, ensure_ascii=False)

def call_llm_decision(context_json):
    """调用大模型决策"""
    api_key = os.getenv("OPENAI_API_KEY")
    api_base = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1") 
    model = os.getenv("LLM_MODEL", "gpt-4o-mini")

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    system_prompt = """你是一个顶级的加密货币交易AI。请根据技术指标和K线序列做出判断。
必须以JSON格式输出：{"action": "BUY"|"SELL"|"HOLD", "confidence": 0.0-1.0, "reason": "分析理由"}
重点：关注大周期趋势共振和底背离形态。"""

    payload = {
        "model": model,
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": context_json}],
        "response_format": {"type": "json_object"},
        "temperature": 0.2
    }

    try:
        response = requests.post(f"{api_base}/chat/completions", headers=headers, json=payload, timeout=20)
        response.raise_for_status()
        decision = json.loads(response.json()['choices'][0]['message']['content'])
        return {"action": decision.get("action", "HOLD").upper(), "confidence": float(decision.get("confidence", 0.0)), "reason": decision.get("reason", "")}
    except Exception as e:
        print(f"LLM API 异常: {e}")
        return {"action": "HOLD", "confidence": 0.0, "reason": "AI请求失败，降级。"}

def get_internal_signal(df):
    """[深度重构] 包含状态机、背离检测及 MTF 共振的决策引擎"""
    latest, prev = df.iloc[-1], df.iloc[-2]
    current_price = latest['close']
    adx, ema_200, ema_htf = latest['ADX'], latest['EMA_200'], latest['EMA_HTF_Proxy']
    
    if pd.isna(adx) or pd.isna(ema_htf): 
        return {"action": "HOLD", "reason": "数据预热中"}
    
    macd_hist = latest['MACD_Hist']
    macd_cross_up = macd_hist > prev['MACD_Hist']
    macd_cross_down = macd_hist < prev['MACD_Hist']
    vol_ratio = latest['volume'] / latest['Vol_SMA_20'] if latest['Vol_SMA_20'] > 0 else 0
    rsi = latest['RSI_14']
    
    # 大周期背景颜色 (MTF 滤网)
    is_htf_bull = current_price > ema_htf
    is_htf_bear = current_price < ema_htf

    # === 模式 S: 顶/底背离检测 (最高优先级) ===
    recent_20 = df.iloc[-21:-1]
    min_idx, max_idx = recent_20['close'].idxmin(), recent_20['close'].idxmax()
    
    # 底背离: 价格创新低但动能抬高 + 大周期不处于极端空头
    if current_price < df.loc[min_idx, 'close'] and macd_hist > df.loc[min_idx, 'MACD_Hist'] and macd_cross_up:
        if not (is_htf_bear and adx > 40): # 拒绝在大单边跌势中摸底
            return {"action": "BUY", "reason": "[MTF: 底背离反转] 动能衰竭且大周期支撑"}

    # 顶背离
    if current_price > df.loc[max_idx, 'close'] and macd_hist < df.loc[max_idx, 'MACD_Hist'] and macd_cross_down:
        if not (is_htf_bull and adx > 40):
            return {"action": "SELL", "reason": "[MTF: 顶背离反转] 动能衰竭且大周期见顶"}
        
    # === 模式 A: 极度收敛 (变盘共振) ===
    is_squeeze = latest['BB_Width'] <= latest['BB_Width_Min_50'] * 1.10
    if is_squeeze:
        if current_price > latest['BB_Upper'] and vol_ratio > 1.5 and is_htf_bull:
            return {"action": "BUY", "reason": "[MTF: 共振爆发] 布林带收敛后随大趋势向上突破"}
        elif current_price < latest['BB_Lower'] and vol_ratio > 1.5 and is_htf_bear:
            return {"action": "SELL", "reason": "[MTF: 共振爆发] 布林带收敛后随大趋势向下破位"}

    # === 模式 B: 单边趋势市 ===
    if adx > 25:
        if is_htf_bull and current_price > ema_200 and macd_cross_up and vol_ratio > 1.1:
            return {"action": "BUY", "reason": "[MTF: 趋势共振] 双周期牛市+回踩买入"}
        elif is_htf_bear and current_price < ema_200 and macd_cross_down and vol_ratio > 1.1:
            return {"action": "SELL", "reason": "[MTF: 趋势共振] 双周期熊市+反抽卖出"}
                
    # === 模式 C: 震荡市均值回归 ===
    elif adx < 20:
        if current_price <= latest['BB_Lower'] and rsi < 30: return {"action": "BUY", "reason": "[自适应: 震荡] 下轨超卖"}
        elif current_price >= latest['BB_Upper'] and rsi > 70: return {"action": "SELL", "reason": "[自适应: 震荡] 上轨超买"}
            
    return {"action": "HOLD", "reason": "市场处于混沌波动，无共振信号"}

def format_final_report(df, final_action, reason, alert_prefix):
    latest = df.iloc[-1]
    current_price, atr = latest['close'], latest['ATR_14']
    
    if not pd.isna(atr) and atr > 0:
        sl_dist = 1.5 * atr
        pos_size = (ACCOUNT_BALANCE * RISK_PER_TRADE) / sl_dist
        sl_long, tp_long = current_price - sl_dist, current_price + (sl_dist * RR_RATIO)
        sl_short, tp_short = current_price + sl_dist, current_price - (sl_dist * RR_RATIO)
    else:
        pos_size = 0
        sl_long = tp_long = sl_short = tp_short = 0

    time_str = datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M:%S UTC')
    icon = {"BUY": "🟢 BUY", "SELL": "🔴 SELL", "HOLD": "⚪ HOLD"}.get(final_action, "⚪")
    
    return f"""{alert_prefix}
【BTC {TIMEFRAME} AI/MTF 共振引擎】
时间: {time_str}
闭合确认价: {current_price:.2f}

[⚡ 终极决策]
- 指令: {icon}
- 逻辑: {reason}

[💰 资金管理]
- 建议仓位: {pos_size:.4f} BTC
- 多头: SL {sl_long:.2f} | TP {tp_long:.2f}
- 空头: SL {sl_short:.2f} | TP {tp_short:.2f}
    """

def push_to_dingtalk(message):
    webhook = os.getenv("DINGTALK_WEBHOOK")
    if not webhook: return
    headers = {'Content-Type': 'application/json'}
    payload = {"msgtype": "text", "text": {"content": message}}
    try:
        requests.post(webhook, headers=headers, json=payload, timeout=10)
    except: pass

def run_logic():
    df = fetch_bitget_data(SYMBOL, TIMEFRAME, LIMIT)
    df = calculate_indicators(df)
    
    if os.getenv("OPENAI_API_KEY"):
        decision = call_llm_decision(build_ai_context(df))
    else:
        decision = get_internal_signal(df)
        
    final_action = decision.get("action", "HOLD")
    reason = decision.get("reason", "")
    
    state = load_state()
    last_action = state.get("last_action", "HOLD")
    should_push, prefix = False, ""

    if final_action != "HOLD" and final_action != last_action:
        should_push, prefix = True, "🚨 [强烈信号：MTF 趋势反转]\n"
        state["simulated_position"] = 1 if final_action == "BUY" else -1
        state["entry_price"] = float(df.iloc[-1]['close'])
    elif final_action == "HOLD" and last_action != "HOLD":
        should_push, prefix = True, "⚠️ [状态变更：大周期动力耗尽]\n"
        state["simulated_position"] = 0
    else:
        print(f"[{datetime.datetime.now(datetime.UTC)}] 维持 {final_action}")

    if should_push:
        report = format_final_report(df, final_action, reason, prefix)
        print(report)
        push_to_dingtalk(report)
        state["last_action"] = final_action
        state["last_push_time"] = datetime.datetime.now(datetime.UTC).isoformat()
        save_state(state)

def main():
    if DAEMON_MODE:
        while True:
            wait_for_exact_kline()
            try: run_logic()
            except Exception as e: print(f"异常: {e}")
    else:
        wait_for_exact_kline()
        run_logic()

if __name__ == "__main__":
    main()
