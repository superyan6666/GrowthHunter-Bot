"""
🚀 GrowthHunter V3.1 - 10倍股猎手 (量化重构版 + 精确推送日志)
依赖库安装: 
pip install yfinance pandas pandas-ta requests
"""

import yfinance as yf
import pandas as pd
import pandas_ta as ta
from datetime import datetime
import os
import sys
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

# 忽略 pandas 和 yfinance 的一些常规警告
warnings.filterwarnings('ignore')

def get_small_cap_tickers():
    """获取小盘股代码：优先缓存 -> 实时 Russell 2000 -> 备用 S&P 600"""
    cache_path = 'small_cap_cache.csv'
    
    if os.path.exists(cache_path):
        cache_age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_path))).days
        if cache_age < 7:
            df = pd.read_csv(cache_path)
            print(f"✅ 使用本地缓存小盘股池（{len(df)} 只）")
            return df['Symbol'].tolist()

    sources = [
        'https://stockanalysis.com/list/russell-2000/',
        'https://www.marketbeat.com/russell-2000/'
    ]
    
    for url in sources:
        try:
            tables = pd.read_html(url)
            df = tables[0]
            tickers = df['Symbol'].tolist()
            pd.DataFrame(tickers, columns=['Symbol']).to_csv(cache_path, index=False)
            print(f"✅ 成功加载 Russell 2000 共 {len(tickers)} 只")
            return tickers
        except Exception:
            continue

    # 逻辑修复：回退方案改为 S&P 600 小盘股，而不是 S&P 500 大盘股
    print("⚠️ Russell 2000 源失效，自动回退抓取 S&P 600 小盘股...")
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_600_companies'
        table = pd.read_html(url)[0]
        tickers = table['Symbol'].tolist()
        pd.DataFrame(tickers, columns=['Symbol']).to_csv(cache_path, index=False)
        return tickers
    except Exception as e:
        print(f"❌ 所有股票池获取失败: {e}")
        return []

def batch_technical_screen(tickers):
    """
    第一阶段漏斗：极速批量技术面筛选 (淘汰 90% 劣质标的)
    逻辑：超级趋势向上 + 近期出现异动爆量
    """
    print(f"⏳ 开始第一阶段：批量下载 {len(tickers)} 只股票日线数据进行技术面扫描...")
    
    data = yf.download(tickers, period="1y", group_by="ticker", threads=True, show_errors=False)
    passed_tickers = []
    
    for sym in tickers:
        try:
            if len(tickers) == 1:
                df = data.copy()
            else:
                if sym not in data.columns.levels[0]:
                    continue
                df = data[sym].copy()
                
            df = df.dropna()
            if len(df) < 150: 
                continue
                
            df.ta.supertrend(length=7, multiplier=3.0, append=True)
            vol_95th = df['Volume'].rolling(window=60).quantile(0.95)
            
            current_close = df['Close'].iloc[-1]
            current_vol = df['Volume'].iloc[-1]
            last_vol_95 = vol_95th.iloc[-1]
            
            st_dir_col = [col for col in df.columns if col.startswith('SUPERTd_')][0]
            st_dir = df[st_dir_col].iloc[-1]
            
            if st_dir == 1 and current_vol > last_vol_95:
                passed_tickers.append(sym)
                
        except Exception:
            continue
            
    print(f"🎯 第一阶段完成：技术面初筛保留 {len(passed_tickers)} 只异动标的")
    return passed_tickers

def analyze_fundamentals(symbol):
    """
    第二阶段漏斗：精准财务基本面过滤 (只对初筛通过的股票执行)
    """
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        if not info or 'longName' not in info:
            return None

        market_cap = info.get('marketCap', 0)
        if not (5e7 < market_cap < 5e9): 
            return None

        name = info.get('longName', symbol)
        sector = info.get('sector', '')

        revenue_growth = info.get('revenueGrowth')
        if revenue_growth is None or revenue_growth < 0.20:
            return None

        gross_margin = info.get('grossMargins')
        if gross_margin is None or gross_margin < 0.20:
            return None

        total_revenue = info.get('totalRevenue', 0)
        rd_expense = info.get('researchAndDevelopment', 0)
        rd_ratio = rd_expense / total_revenue if total_revenue > 0 else 0
        
        if sector in {'Healthcare', 'Technology'} and rd_ratio < 0.08:
            return None

        reasons = (
            f"市值: {market_cap/1e8:.1f}亿美元 | "
            f"营收增速: {revenue_growth:.1%} | "
            f"毛利率: {gross_margin:.1%} | "
            f"研发占比: {rd_ratio:.1%} | "
            f"异动信号: 趋势向上且巨量突破"
        )

        return {
            '股票代码': symbol,
            '公司名称': name,
            '所属行业': sector,
            '市值(亿美元)': round(market_cap / 1e8, 2),
            '营收增速': f"{revenue_growth:.1%}",
            '筛选理由': reasons,
            '链接': f"https://finance.yahoo.com/quote/{symbol}"
        }

    except Exception:
        return None

def send_notifications(df):
    """多平台推送模块 (加入精确的报错日志)"""
    if df.empty:
        print("📭 今日无符合双重严苛条件的标的，不发送通知。")
        return
        
    summary = f"🚀 AI 驱动：GrowthHunter V3.1 异动播报\n\n共捕获 {len(df)} 只高潜小盘股！\n\n"
    for _, row in df.head(10).iterrows():
        summary += f"• [{row['股票代码']}] {row['公司名称']} ({row['市值(亿美元)']}亿)\n  └ {row['筛选理由']}\n\n"
    
    # 微信 (Server酱)
    serverchan_key = os.getenv('SERVERCHAN_KEY')
    if serverchan_key:
        try:
            res = requests.get(f"https://sctapi.ftqq.com/{serverchan_key}.send",
                         params={"title": f"🚀 AI 发现 {len(df)} 只异动 10 倍候选股", "desp": summary})
            if res.status_code == 200:
                print("✅ 微信(Server酱)推送已发送")
            else:
                print(f"❌ 微信推送返回异常: {res.text}")
        except Exception as e:
            print(f"❌ 微信请求失败: {e}")
            
    # Telegram
    token = os.getenv('TELEGRAM_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    if token and chat_id:
        try:
            res = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          json={"chat_id": chat_id, "text": summary, "parse_mode": "Markdown"})
            if res.status_code == 200:
                print("✅ Telegram推送已发送")
            else:
                print(f"❌ Telegram推送异常: {res.text}")
        except Exception as e:
            print(f"❌ Telegram请求失败: {e}")

    # 飞书
    feishu_webhook = os.getenv('FEISHU_WEBHOOK')
    if feishu_webhook:
        try:
            res = requests.post(feishu_webhook, json={"msg_type": "text", "content": {"text": summary}})
            if res.status_code == 200:
                print("✅ 飞书推送已发送")
            else:
                print(f"❌ 飞书推送异常: {res.text}")
        except Exception as e:
            print(f"❌ 飞书请求失败: {e}")

    # 钉钉
    dingtalk_webhook = os.getenv('DINGTALK_WEBHOOK')
    if dingtalk_webhook:
        try:
            res = requests.post(dingtalk_webhook, json={"msgtype": "text", "text": {"content": summary}})
            # 钉钉正常返回是 200 且含有 errcode: 0
            if res.status_code == 200 and res.json().get('errcode') == 0:
                print("✅ 钉钉推送已发送")
            else:
                print(f"❌ 钉钉拒绝推送 (请检查机器人安全关键词设置): {res.text}")
        except Exception as e:
            print(f"❌ 钉钉请求报错: {e}")

def test_notifications():
    """用于独立测试推送配置是否成功"""
    print("🔧 启动推送测试模式...")
    # 这里的文本包含了常见自定义关键词，如：测试、通知、10倍股、播报、AI
    mock_data = [{
        '股票代码': 'TEST',
        '公司名称': '配置测试专用股',
        '市值(亿美元)': 88.8,
        '筛选理由': 'AI 测试通知：如果你能看到这条消息，说明你的配置正确！(模拟播报 10倍股)'
    }]
    df = pd.DataFrame(mock_data)
    send_notifications(df)
    print("✅ 测试推送指令发送完毕，请查看上方是否有 ❌ 报错。")

def main():
    print("="*40)
    print(" 🚀 启动 GrowthHunter 量化重构版")
    print("="*40)
    
    tickers = get_small_cap_tickers()
    if not tickers:
        return

    passed_tech_tickers = batch_technical_screen(tickers)
    
    if not passed_tech_tickers:
        print("🤷‍♂️ 第一阶段无任何标的满足趋势与异动要求，流程结束。")
        return

    results = []
    print(f"⏳ 开始第二阶段：对初筛通过的 {len(passed_tech_tickers)} 只股票进行财务深挖...")
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_symbol = {executor.submit(analyze_fundamentals, sym): sym for sym in passed_tech_tickers}
        for future in as_completed(future_to_symbol):
            result = future.result()
            if result:
                results.append(result)

    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values(by='市值(亿美元)') 
        df.to_csv('growth_hunter_results.csv', index=False, encoding='utf-8')
        
        with open('growth_hunter_results.md', 'w', encoding='utf-8') as f:
            f.write("# 🚀 GrowthHunter 严选报告\n\n")
            f.write(f"**生成时间**：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("**量化策略**：SuperTrend右侧 + 95分位爆量突破 + >20%营收增长 + 高研发驱动\n\n")
            f.write(df.to_markdown(index=False))
            
        print(f"\n🎉 筛选大功告成！最终捕获 {len(results)} 只硬核标的。")
    else:
        print("\n📉 遗憾：财务数据(增速/毛利)未达标，今日空仓。")

    send_notifications(df)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        test_notifications()
    else:
        main()
