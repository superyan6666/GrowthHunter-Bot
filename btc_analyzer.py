import requests
import pandas as pd
import pandas_ta as ta
import os
import datetime

# 核心配置
SYMBOL = "BTCUSDT"
TIMEFRAME = "15m"  # 可随时改为 "1h"
LIMIT = 100        # 获取前100根K线足够计算常用指标

def fetch_binance_data(symbol, timeframe, limit):
    """通过币安公共API获取K线数据"""
    url = f"https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": timeframe, "limit": limit}
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    df = pd.DataFrame(data, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_asset', 'taker_buy_quote_asset', 'ignore'
    ])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    # 转换数值类型
    numeric_cols = ['open', 'high', 'low', 'close', 'volume']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
    return df

def calculate_indicators(df):
    """计算核心技术指标（包含MACD与布林带）"""
    # 1. 趋势：20周期均线
    df['SMA_20'] = ta.sma(df['close'], length=20)
    
    # 2. 动能：RSI
    df['RSI_14'] = ta.rsi(df['close'], length=14)
    
    # 3. 波动率：ATR (用于计算止损)
    df['ATR_14'] = ta.atr(df['high'], df['low'], df['close'], length=14)
    
    # 4. MACD (12, 26, 9)
    macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
    # 分别提取 MACD线, 信号线, 直方图
    df['MACD'] = macd['MACD_12_26_9']
    df['MACD_Signal'] = macd['MACDs_12_26_9']
    df['MACD_Hist'] = macd['MACDh_12_26_9']
    
    # 5. 布林带 (20, 2)
    bbands = ta.bbands(df['close'], length=20, std=2)
    df['BB_Upper'] = bbands['BBU_20_2.0']
    df['BB_Middle'] = bbands['BBM_20_2.0']
    df['BB_Lower'] = bbands['BBL_20_2.0']
    # 计算布林带宽度 (Bandwidth) 识别挤压
    df['BB_Width'] = (df['BB_Upper'] - df['BB_Lower']) / df['BB_Middle']
    
    # 6. 支撑阻力：枢纽点 (Pivot Points)
    prev_high = df['high'].iloc[-2]
    prev_low = df['low'].iloc[-2]
    prev_close = df['close'].iloc[-2]
    pivot = (prev_high + prev_low + prev_close) / 3
    df['R1'] = (2 * pivot) - prev_low
    df['S1'] = (2 * pivot) - prev_high
    
    return df

def generate_report_prompt(df):
    """生成包含MACD和布林带的增强版结构化分析提示词"""
    latest = df.iloc[-1]
    
    current_price = latest['close']
    sma_20 = latest['SMA_20']
    rsi = latest['RSI_14']
    atr = latest['ATR_14']
    s1 = latest['S1']
    r1 = latest['R1']
    
    # MACD 数据
    macd_val = latest['MACD']
    macd_hist = latest['MACD_Hist']
    macd_status = "金叉/多头增强" if macd_hist > 0 else "死叉/空头增强"
    
    # 布林带数据
    bb_upper = latest['BB_Upper']
    bb_lower = latest['BB_Lower']
    bb_pos = (current_price - bb_lower) / (bb_upper - bb_lower) * 100 # 价格在带内的百分比位置
    
    # 止损止盈
    sl_long = current_price - (1.5 * atr)
    tp_long = current_price + (2.5 * atr)
    
    time_str = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    
    report = f"""
【BTC {TIMEFRAME} 核心动能与结构扫描】
时间: {time_str}
当前价格: {current_price:.2f}

[动能指标 (Momentum)]
- MACD (12,26,9): {macd_val:.2f} | 柱状图: {macd_hist:.2f} ({macd_status})
- RSI (14): {rsi:.2f} ({'超买' if rsi > 70 else ('超卖' if rsi < 30 else '中性')})

[结构与波动 (Structure & Volatility)]
- 布林带位置: {bb_pos:.1f}% (0%=下轨, 100%=上轨)
- 布林带区间: [{bb_lower:.2f} - {bb_upper:.2f}]
- 枢纽点 S1/R1: {s1:.2f} / {r1:.2f}
- 波动率 ATR: {atr:.2f}

[系统测算风控]
- 建议买入参考: {max(s1, bb_lower):.2f} (结合支撑与布林下轨)
- 机械止损 (SL): {sl_long:.2f}
- 目标止盈 (TP): {tp_long:.2f}

[待AI处理任务]
1. 观察MACD直方图是否正在缩短或增长，判断动能衰竭情况。
2. 结合价格在布林带的位置与S1/R1，给出80字以内的专业入场建议。
    """
    return report

def main():
    try:
        print(f"正在拉取 {SYMBOL} {TIMEFRAME} 数据并计算增强指标...")
        df = fetch_binance_data(SYMBOL, TIMEFRAME, LIMIT)
        df = calculate_indicators(df)
        
        report_prompt = generate_report_prompt(df)
        print("\n=== Phase 1 升级完成：新增MACD与布林带数据 ===")
        print(report_prompt)
        
    except Exception as e:
        print(f"执行出错: {e}")

if __name__ == "__main__":
    main()
