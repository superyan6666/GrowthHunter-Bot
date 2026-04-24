import requests
import pandas as pd
import pandas_ta as ta
import os
import datetime

# ================= 核心配置 =================
SYMBOL = "BTCUSDT"
TIMEFRAME = "15m"  
LIMIT = 500        # 获取前500根K线，以满足长期均线(EMA 200)的计算厚度

def fetch_binance_data(symbol, timeframe, limit):
    """通过币安公共API获取K线数据，带防屏蔽多节点回退机制"""
    endpoints = [
        "https://data-api.binance.vision/api/v3/klines",  # 优先节点：抗Geo-block
        "https://api1.binance.com/api/v3/klines",         # 备选节点 1
        "https://api.binance.com/api/v3/klines"           # 备选节点 2
    ]
    params = {"symbol": symbol, "interval": timeframe, "limit": limit}
    
    for url in endpoints:
        try:
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
            print(f"节点 {url} 报错: {e}，尝试切换备用节点...")
            continue
            
    raise Exception("所有币安 API 节点均拒绝连接或超时，请检查网络或稍后再试。")

def calculate_indicators(df):
    """计算核心技术指标（使用列索引提取实现版本免疫）"""
    # 1. 基础趋势：20周期均线
    df['SMA_20'] = ta.sma(df['close'], length=20)
    
    # 2. 动能：RSI
    df['RSI_14'] = ta.rsi(df['close'], length=14)
    
    # 3. 波动率：ATR (用于动态风控)
    df['ATR_14'] = ta.atr(df['high'], df['low'], df['close'], length=14)
    
    # 4. MACD (12, 26, 9)
    macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
    df['MACD'] = macd.iloc[:, 0]
    df['MACD_Hist'] = macd.iloc[:, 1]
    df['MACD_Signal'] = macd.iloc[:, 2]
    
    # 5. 布林带 (20, 2)
    bbands = ta.bbands(df['close'], length=20, std=2)
    df['BB_Lower'] = bbands.iloc[:, 0]
    df['BB_Middle'] = bbands.iloc[:, 1]
    df['BB_Upper'] = bbands.iloc[:, 2]
    df['BB_Width'] = (df['BB_Upper'] - df['BB_Lower']) / df['BB_Middle']
    
    # 6. 趋势强度：ADX (14)
    adx = ta.adx(df['high'], df['low'], df['close'], length=14)
    df['ADX'] = adx.iloc[:, 0]
    
    # 7. 支撑阻力：传统枢纽点 (Pivot Points)
    prev_high = df['high'].iloc[-2]
    prev_low = df['low'].iloc[-2]
    prev_close = df['close'].iloc[-2]
    pivot = (prev_high + prev_low + prev_close) / 3
    df['R1'] = (2 * pivot) - prev_low
    df['S1'] = (2 * pivot) - prev_high
    
    # 8. 成交量验证：Volume SMA (20)
    df['Vol_SMA_20'] = ta.sma(df['volume'], length=20)
    
    # 9. 大周期基准线：EMA (200)
    df['EMA_200'] = ta.ema(df['close'], length=200)
    
    return df

def generate_report_prompt(df):
    """生成增强版结构化分析提示词（多维过滤体系）"""
    latest = df.iloc[-1]
    
    current_price = latest['close']
    atr = latest['ATR_14']
    s1, r1 = latest['S1'], latest['R1']
    
    # --- 状态判定逻辑 ---
    # 1. MACD 状态
    macd_hist = latest['MACD_Hist']
    macd_status = "金叉/多头增强" if macd_hist > 0 else "死叉/空头增强"
    
    # 2. ADX 市场状态判定
    adx = latest['ADX']
    if adx < 20:
        market_state = "震荡市 (趋势极弱，高抛低吸为主)"
    elif adx > 25:
        market_state = "趋势市 (动能强劲，适合顺势突破)"
    else:
        market_state = "趋势酝酿中 (观察方向选择)"
        
    # 3. 成交量状态判定
    current_vol = latest['volume']
    vol_sma_20 = latest['Vol_SMA_20']
    if pd.isna(vol_sma_20):
        vol_state = "数据不足"
    elif current_vol > vol_sma_20 * 1.5:
        vol_state = "显著放量 (突破/反转有效性极高)"
    elif current_vol > vol_sma_20:
        vol_state = "温和放量 (动能健康)"
    else:
        vol_state = "缩量状态 (警惕假突破/诱多陷阱)"

    # 4. EMA200 大周期判定
    ema_200 = latest['EMA_200']
    if pd.isna(ema_200):
        long_trend = "数据不足"
    else:
        long_trend = "牛市基调 (价格>均线，偏向做多)" if current_price > ema_200 else "熊市基调 (价格<均线，偏向做空)"

    # 布林带位置计算
    bb_upper, bb_lower = latest['BB_Upper'], latest['BB_Lower']
    bb_pos = (current_price - bb_lower) / (bb_upper - bb_lower) * 100 
    
    # 风控测算
    sl_long = current_price - (1.5 * atr)
    tp_long = current_price + (2.5 * atr)
    time_str = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    
    report = f"""
【BTC {TIMEFRAME} 微观量化扫描】
时间: {time_str}
价格: {current_price:.2f}

[大局观与结构 (Macro & Structure)]
- 大周期基准 (EMA200): {ema_200:.2f} => {long_trend}
- 成交量验证: 最新 {current_vol:.0f} vs 20均量 {vol_sma_20:.0f} ({vol_state})
- 枢纽点 S1/R1: {s1:.2f} / {r1:.2f}
- 布林带位置: {bb_pos:.1f}% (区间: {bb_lower:.2f} - {bb_upper:.2f})

[动能与强度 (Momentum & Volatility)]
- MACD (12,26,9): 柱状图 {macd_hist:.2f} ({macd_status})
- 趋势强度 (ADX14): {adx:.2f} => {market_state}
- RSI (14): {latest['RSI_14']:.2f}
- 波动率 ATR: {atr:.2f}

[系统测算风控 (仅供参考)]
- 机械止损 (SL): {sl_long:.2f}
- 目标止盈 (TP): {tp_long:.2f}

[待AI处理任务]
1. 观察MACD直方图衰竭情况。
2. 规则A(防震荡): 若ADX<20，忽略MACD交叉，仅依布林带及S1/R1高抛低吸。
3. 规则B(防画门): 若缩量状态，对任何突破保持克制，提示陷阱风险。
4. 规则C(防逆势): 遵循EMA200基调，不轻易摸顶或抄底。
5. 综合输出80字入场建议。
    """
    return report

def push_to_dingtalk(message):
    """推送到钉钉自定义机器人"""
    webhook = os.getenv("DINGTALK_WEBHOOK")
    if not webhook:
        print("未配置 DINGTALK_WEBHOOK，跳过钉钉推送。")
        return
        
    headers = {'Content-Type': 'application/json'}
    payload = {
        "msgtype": "text",
        "text": {"content": message}
    }
    try:
        response = requests.post(webhook, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        print("=> 钉钉消息推送成功！")
    except Exception as e:
        print(f"=> 钉钉推送失败: {e}")

def main():
    try:
        print(f"正在拉取 {SYMBOL} {TIMEFRAME} 数据并进行全景扫描...")
        df = fetch_binance_data(SYMBOL, TIMEFRAME, LIMIT)
        df = calculate_indicators(df)
        
        report_prompt = generate_report_prompt(df)
        print("\n=== Phase 1 终态合并：三级风控扫描仪 ===")
        print(report_prompt)
        
        push_to_dingtalk(report_prompt)
    except Exception as e:
        print(f"执行出错: {e}")

if __name__ == "__main__":
    main()
