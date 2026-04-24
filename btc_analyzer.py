import requests
import pandas as pd
import pandas_ta as ta
import os
import datetime
import time
import json  # [新增] 用于处理与 LLM 的 JSON 数据交互

# ================= 核心配置 =================
SYMBOL = "BTCUSDT"
TIMEFRAME = "15m"  
LIMIT = 500        
RR_RATIO = 2.0     
ACCOUNT_BALANCE = 10000  
RISK_PER_TRADE = 0.02    

def fetch_binance_data(symbol, timeframe, limit):
    """通过币安公共API获取K线数据，带防屏蔽多节点回退与指数退避机制"""
    endpoints = [
        "https://data-api.binance.vision/api/v3/klines",
        "https://api1.binance.com/api/v3/klines",
        "https://api.binance.com/api/v3/klines"
    ]
    params = {"symbol": symbol, "interval": timeframe, "limit": limit}
    
    for attempt, url in enumerate(endpoints):
        try:
            if attempt > 0:
                wait_time = 2 ** attempt
                print(f"触发速率控制，等待 {wait_time} 秒后尝试节点: {url}...")
                time.sleep(wait_time)
                
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status() 
            
            data = response.json()
            df = pd.DataFrame(data, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset', 'taker_buy_quote_asset', 'ignore'
            ])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"节点 {url} 报错: {e}")
            continue
            
    raise Exception("所有币安 API 节点均拒绝连接或超时，请检查网络或稍后再试。")

def calculate_indicators(df):
    """计算核心技术指标"""
    df['SMA_20'] = ta.sma(df['close'], length=20)
    df['RSI_14'] = ta.rsi(df['close'], length=14)
    df['ATR_14'] = ta.atr(df['high'], df['low'], df['close'], length=14)
    
    macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
    df['MACD'] = macd['MACD_12_26_9']
    df['MACD_Hist'] = macd['MACDh_12_26_9']
    df['MACD_Signal'] = macd['MACDs_12_26_9']
    
    bbands = ta.bbands(df['close'], length=20, std=2)
    df['BB_Lower'] = bbands[[c for c in bbands.columns if c.startswith('BBL')][0]]
    df['BB_Middle'] = bbands[[c for c in bbands.columns if c.startswith('BBM')][0]]
    df['BB_Upper'] = bbands[[c for c in bbands.columns if c.startswith('BBU')][0]]
    df['BB_Width'] = (df['BB_Upper'] - df['BB_Lower']) / df['BB_Middle']
    
    adx = ta.adx(df['high'], df['low'], df['close'], length=14)
    df['ADX'] = adx['ADX_14']
    
    prev_high = df['high'].iloc[-2]
    prev_low = df['low'].iloc[-2]
    prev_close = df['close'].iloc[-2]
    pivot = (prev_high + prev_low + prev_close) / 3
    df['R1'] = (2 * pivot) - prev_low
    df['S1'] = (2 * pivot) - prev_high
    
    df['Vol_SMA_20'] = ta.sma(df['volume'], length=20)
    df['EMA_200'] = ta.ema(df['close'], length=200)
    return df

def safe_fmt(value, fmt="{:.2f}"):
    """安全格式化函数：拦截 NaN"""
    if pd.isna(value):
        return "数据不足"
    return fmt.format(value)

def build_ai_context(df):
    """[Phase 2 新增] 构建供 LLM 分析的结构化 JSON 提示词上下文"""
    latest = df.iloc[-1]
    
    # 提取最近 5 根 K 线的关键数据（用于识别背离和微观形态）
    recent_5 = []
    for i in range(-5, 0):
        row = df.iloc[i]
        recent_5.append({
            "time": str(row['timestamp']),
            "close": round(row['close'], 2),
            "volume": round(row['volume'], 2),
            "macd_hist": round(row['MACD_Hist'], 2) if not pd.isna(row['MACD_Hist']) else 0,
            "rsi": round(row['RSI_14'], 2) if not pd.isna(row['RSI_14']) else 50
        })

    # 当前市场切片
    context = {
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "current_price": latest['close'],
        "macro_trend_ema200": latest['EMA_200'] if not pd.isna(latest['EMA_200']) else None,
        "trend_strength_adx14": latest['ADX'] if not pd.isna(latest['ADX']) else None,
        "volatility_atr14": latest['ATR_14'] if not pd.isna(latest['ATR_14']) else None,
        "support_s1": latest['S1'] if not pd.isna(latest['S1']) else None,
        "resistance_r1": latest['R1'] if not pd.isna(latest['R1']) else None,
        "bbands_lower": latest['BB_Lower'],
        "bbands_upper": latest['BB_Upper'],
        "recent_5_candles": recent_5,
        "vol_sma_20": latest['Vol_SMA_20'] if not pd.isna(latest['Vol_SMA_20']) else None
    }
    return json.dumps(context, ensure_ascii=False)

def call_llm_decision(context_json):
    """[Phase 2 新增] 调用大模型进行逻辑推演并强制输出结构化 JSON 信号"""
    api_key = os.getenv("OPENAI_API_KEY")
    # 支持自定义代理池或第三方兼容API (默认 OpenAI，可改为 DeepSeek: https://api.deepseek.com/v1)
    api_base = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1") 
    model = os.getenv("LLM_MODEL", "gpt-4o-mini") # 默认使用又快又便宜的 4o-mini

    if not api_key:
        return {"action": "HOLD", "confidence": 0, "reason": "未配置 OPENAI_API_KEY，系统已降级为纯量化观望模式。"}

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    system_prompt = """你是一个顶级的量化加密货币交易AI。我将提供包含最新技术指标和最近5根K线序列的JSON数据。
请你基于数据进行判断（寻找背离、突破验证、动能衰竭等）。
请严格遵守以下JSON格式输出，禁止任何多余的文本：
{"action": "BUY" | "SELL" | "HOLD", "confidence": 0.0到1.0的浮点数, "reason": "80字以内的专业分析理由"}
防画门规则：若成交量萎缩或在ADX<20时出现伪突破，请务必返回HOLD。"""

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": context_json}
        ],
        "response_format": {"type": "json_object"}, # 强制 OpenAI 返回 JSON
        "temperature": 0.2 # 降低随机性
    }

    try:
        response = requests.post(f"{api_base}/chat/completions", headers=headers, json=payload, timeout=20)
        response.raise_for_status()
        result_json = response.json()
        ai_msg = result_json['choices'][0]['message']['content']
        # 解析 AI 返回的 JSON 字符串
        decision = json.loads(ai_msg)
        
        # 字段校验与兜底
        action = decision.get("action", "HOLD").upper()
        if action not in ["BUY", "SELL", "HOLD"]:
            action = "HOLD"
        confidence = float(decision.get("confidence", 0.0))
        reason = decision.get("reason", "未提供理由")
        
        return {"action": action, "confidence": confidence, "reason": reason}

    except Exception as e:
        print(f"LLM API 请求或解析失败: {e}")
        return {"action": "HOLD", "confidence": 0.0, "reason": f"AI服务调用异常，强制触发安全降级为观望。({str(e)})"}

def format_final_report(df, ai_decision):
    """生成结合了量化底座与 AI 最终决策的推送报告"""
    latest = df.iloc[-1]
    current_price = latest['close']
    atr = latest['ATR_14']
    
    # 资金管理测算
    if not pd.isna(atr) and atr > 0:
        sl_dist = 1.5 * atr
        risk_amount = ACCOUNT_BALANCE * RISK_PER_TRADE
        pos_size_btc_val = risk_amount / sl_dist
        pos_size_btc = f"{pos_size_btc_val:.4f} BTC"
        
        sl_long_str = safe_fmt(current_price - sl_dist)
        tp_long_str = safe_fmt(current_price + (sl_dist * RR_RATIO))
        sl_short_str = safe_fmt(current_price + sl_dist)
        tp_short_str = safe_fmt(current_price - (sl_dist * RR_RATIO))
    else:
        pos_size_btc = sl_long_str = tp_long_str = sl_short_str = tp_short_str = "数据不足"

    time_str = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    action_icon = {"BUY": "🟢 做多 (BUY)", "SELL": "🔴 做空 (SELL)", "HOLD": "⚪ 观望 (HOLD)"}.get(ai_decision['action'], "⚪ 观望")
    
    report = f"""
【BTC {TIMEFRAME} AI自动驾驶引擎扫描】
时间: {time_str}
当前实时价: {current_price:.2f}

[🧠 AI 最终决策]
- 指令: {action_icon}
- 确定性: {ai_decision['confidence'] * 100:.1f}%
- 逻辑: {ai_decision['reason']}

[💰 资金与风控测算 (基于 {ACCOUNT_BALANCE}U 账户)]
- 建议开仓规模: {pos_size_btc}
- 若做多: 止损 {sl_long_str} | 止盈 {tp_long_str}
- 若做空: 止损 {sl_short_str} | 止盈 {tp_short_str}

[📊 底层数据快照]
- EMA200趋势基准: {safe_fmt(latest['EMA_200'])}
- ADX趋势强度: {safe_fmt(latest['ADX'])}
- 20期均量验证: {safe_fmt(latest['Vol_SMA_20'], "{:.0f}")}
    """
    return report

def push_to_dingtalk(message):
    webhook = os.getenv("DINGTALK_WEBHOOK")
    if not webhook:
        print("未配置 DINGTALK_WEBHOOK，跳过钉钉推送。")
        return
        
    headers = {'Content-Type': 'application/json'}
    payload = {"msgtype": "text", "text": {"content": message}}
    try:
        response = requests.post(webhook, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        print("=> 钉钉消息推送成功！")
    except Exception as e:
        print(f"=> 钉钉推送失败: {e}")

def main():
    try:
        print(f"1. 拉取 {SYMBOL} {TIMEFRAME} 数据...")
        df = fetch_binance_data(SYMBOL, TIMEFRAME, LIMIT)
        print("2. 计算多维核心指标...")
        df = calculate_indicators(df)
        
        print("3. 构建序列化 JSON 喂给 AI...")
        context_json = build_ai_context(df)
        
        print("4. 请求 LLM 进行决策推演...")
        ai_decision = call_llm_decision(context_json)
        
        print("5. 生成最终战报...")
        final_report = format_final_report(df, ai_decision)
        print("\n=== Phase 2 自动驾驶模式输出 ===")
        print(final_report)
        
        push_to_dingtalk(final_report)
        
    except Exception as e:
        print(f"执行出错: {e}")

if __name__ == "__main__":
    main()
