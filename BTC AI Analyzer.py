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
    """计算核心技术指标（严格数学计算，拒绝AI幻觉）"""
    # 1. 趋势：20周期均线
    df['SMA_20'] = ta.sma(df['close'], length=20)
    
    # 2. 动能：RSI
    df['RSI_14'] = ta.rsi(df['close'], length=14)
    
    # 3. 波动率：ATR (用于计算止损)
    df['ATR_14'] = ta.atr(df['high'], df['low'], df['close'], length=14)
    
    # 4. 支撑阻力：传统枢纽点 (Pivot Points) 基于前一根K线
    prev_high = df['high'].iloc[-2]
    prev_low = df['low'].iloc[-2]
    prev_close = df['close'].iloc[-2]
    
    pivot = (prev_high + prev_low + prev_close) / 3
    df['R1'] = (2 * pivot) - prev_low
    df['S1'] = (2 * pivot) - prev_high
    
    return df

def generate_report_prompt(df):
    """生成结构化的分析提示词（提供给LLM或直接推送到Telegram）"""
    latest = df.iloc[-1]
    
    current_price = latest['close']
    sma_20 = latest['SMA_20']
    rsi = latest['RSI_14']
    atr = latest['ATR_14']
    s1 = latest['S1']
    r1 = latest['R1']
    
    # 机械止损止盈位计算 (例如：止损 1.5 ATR，止盈 2.5 ATR)
    sl_long = current_price - (1.5 * atr)
    tp_long = current_price + (2.5 * atr)
    
    # 趋势判定
    trend = "多头" if current_price > sma_20 else "空头"
    rsi_status = "超买" if rsi > 70 else ("超卖" if rsi < 30 else "中性")
    
    time_str = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    
    report = f"""
【BTC {TIMEFRAME} 级别市场微观扫描】
时间: {time_str}
当前价格: {current_price:.2f}

[纯量化数据面板]
- 短期趋势 (SMA20): {trend} ({sma_20:.2f})
- 动量状态 (RSI14): {rsi_status} ({rsi:.2f})
- 下方支撑 (S1): {s1:.2f}
- 上方阻力 (R1): {r1:.2f}
- 波动率 (ATR14): {atr:.2f}

[系统测算机械风控 (仅供多头参考)]
- 建议买入区: 接近 {s1:.2f}
- 严格止损 (SL): {sl_long:.2f} (1.5 ATR)
- 预期止盈 (TP): {tp_long:.2f} (2.5 ATR)

[待AI处理的任务]
请根据以上硬指标，结合当前是否在支撑/阻力位附近，给出一段80字以内的专业交易决策建议，包括是否适合此时入场。
    """
    return report

def main():
    try:
        print(f"正在拉取 {SYMBOL} {TIMEFRAME} 数据...")
        df = fetch_binance_data(SYMBOL, TIMEFRAME, LIMIT)
        df = calculate_indicators(df)
        
        # 获取基础报告框架
        report_prompt = generate_report_prompt(df)
        print("\n=== Phase 1 跑通：生成基础分析框架 ===")
        print(report_prompt)
        
        # 此处预留 Webhook 推送代码 (如 Telegram/Discord)
        # 预留 Phase 2 的 LLM API 调用代码
        
    except Exception as e:
        print(f"执行出错: {e}")

if __name__ == "__main__":
    main()
