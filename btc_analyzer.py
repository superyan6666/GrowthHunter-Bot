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
    """通过币安公共API获取K线数据，带防屏蔽回退机制"""
    # 采用官方数据专用节点与主节点备用机制，绕过美国IP的451封锁
    endpoints = [
        "https://data-api.binance.vision/api/v3/klines",  # 优先：公共数据节点，对受限区域更友好
        "https://api1.binance.com/api/v3/klines",         # 备选节点 1
        "https://api.binance.com/api/v3/klines"           # 备选节点 2
    ]
    
    params = {"symbol": symbol, "interval": timeframe, "limit": limit}
    
    for url in endpoints:
        try:
            # 增加 timeout 防止卡死
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status() # 如果返回非 200 状态码，会抛出 HTTPError
            
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
            
        except requests.exceptions.RequestException as e:
            # 静默捕捉异常并尝试下一个节点（仅保留简单调试输出以供查阅日志）
            print(f"节点 {url} 报错: {e}，尝试切换备用节点...")
            continue
            
    # 如果所有节点都失败，则终止程序
    raise Exception("所有币安 API 节点均拒绝连接或超时，请检查网络、IP 限制或稍后再试。")

def calculate_indicators(df):
    """计算核心技术指标（包含MACD与布林带），使用列索引提取实现版本兼容"""
    # 1. 趋势：20周期均线
    df['SMA_20'] = ta.sma(df['close'], length=20)
    
    # 2. 动能：RSI
    df['RSI_14'] = ta.rsi(df['close'], length=14)
    
    # 3. 波动率：ATR (用于计算止损)
    df['ATR_14'] = ta.atr(df['high'], df['low'], df['close'], length=14)
    
    # 4. MACD (12, 26, 9)
    macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
    # 使用位置提取防列名不一致: 0=MACD线, 1=直方图, 2=信号线
    df['MACD'] = macd.iloc[:, 0]
    df['MACD_Hist'] = macd.iloc[:, 1]
    df['MACD_Signal'] = macd.iloc[:, 2]
    
    # 5. 布林带 (20, 2)
    bbands = ta.bbands(df['close'], length=20, std=2)
    # 使用位置提取防列名不一致: 0=下轨, 1=中轨, 2=上轨
    df['BB_Lower'] = bbands.iloc[:, 0]
    df['BB_Middle'] = bbands.iloc[:, 1]
    df['BB_Upper'] = bbands.iloc[:, 2]
    
    # 计算布林带宽度 (Bandwidth) 识别挤压
    df['BB_Width'] = (df['BB_Upper'] - df['BB_Lower']) / df['BB_Middle']
    
    # [新增] 6. 趋势强度：ADX (14)
    adx = ta.adx(df['high'], df['low'], df['close'], length=14)
    df['ADX'] = adx.iloc[:, 0] # 提取 ADX 线以兼容不同版本的列名
    
    # 7. 支撑阻力：枢纽点 (Pivot Points)
    prev_high = df['high'].iloc[-2]
    prev_low = df['low'].iloc[-2]
    prev_close = df['close'].iloc[-2]
    pivot = (prev_high + prev_low + prev_close) / 3
    df['R1'] = (2 * pivot) - prev_low
    df['S1'] = (2 * pivot) - prev_high
    
    return df

def generate_report_prompt(df):
    """生成包含MACD、布林带和ADX的增强版结构化分析提示词"""
    latest = df.iloc[-1]
    
    current_price = latest['close']
    sma_20 = latest['SMA_20']
    rsi = latest['RSI_14']
    atr = latest['ATR_14']
    adx = latest['ADX']  # [新增提取 ADX 数据]
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
    
    # [新增] 市场状态判定 (基于ADX)
    if adx < 20:
        market_state = "震荡市 (趋势极弱，高抛低吸为主)"
    elif adx > 25:
        market_state = "趋势市 (动能强劲，适合顺势突破)"
    else:
        market_state = "趋势酝酿中 (观察方向选择)"

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
- 趋势强度 (ADX14): {adx:.2f} => {market_state}

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
2. 过滤规则：如果目前处于震荡市(ADX<20)，请忽略所有MACD交叉信号，仅依靠布林带上下轨和S1/R1进行反转操作。
3. 结合以上结构与数据，给出80字以内的专业入场建议。
    """
    return report

def push_to_dingtalk(message):
    """将生成的报告推送到钉钉自定义机器人"""
    webhook = os.getenv("DINGTALK_WEBHOOK")
    if not webhook:
        print("未配置 DINGTALK_WEBHOOK 环境变量，跳过钉钉推送。")
        return
        
    headers = {'Content-Type': 'application/json'}
    payload = {
        "msgtype": "text",
        "text": {
            "content": message
        }
    }
    try:
        response = requests.post(webhook, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        print("=> 钉钉消息推送成功！")
    except Exception as e:
        print(f"=> 钉钉推送失败: {e}")

def main():
    try:
        print(f"正在拉取 {SYMBOL} {TIMEFRAME} 数据并计算增强指标...")
        df = fetch_binance_data(SYMBOL, TIMEFRAME, LIMIT)
        df = calculate_indicators(df)
        
        report_prompt = generate_report_prompt(df)
        print("\n=== Phase 1 升级完成：新增MACD与布林带数据 ===")
        print(report_prompt)
        
        # 执行钉钉推送
        push_to_dingtalk(report_prompt)
        
    except Exception as e:
        print(f"执行出错: {e}")

if __name__ == "__main__":
    main()
