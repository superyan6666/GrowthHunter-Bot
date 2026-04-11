"""
🚀 GrowthHunter V3.4 - 10倍股猎手 (第1步进化 + 稳定性优化版)
依赖库安装: 
pip install yfinance pandas pandas-ta requests tabulate
"""

import yfinance as yf
import pandas as pd
import pandas_ta as ta
from datetime import datetime
import os
import sys
import requests
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

# 忽略 pandas 和 yfinance 的一些常规警告
warnings.filterwarnings('ignore')

def get_small_cap_tickers():
    """获取小盘股代码：优先缓存 -> 实时 Russell 2000 -> 备用 S&P 600"""
    cache_path = 'small_cap_cache.csv'
    
    # 【修正7】：缓存读取增加异常处理，防崩溃
    if os.path.exists(cache_path):
        try:
            cache_age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_path))).days
            if cache_age < 7:
                df = pd.read_csv(cache_path)
                print(f"✅ 使用本地缓存小盘股池（{len(df)} 只）")
                return df['Symbol'].tolist()
        except Exception as e:
            print(f"⚠️ 缓存文件损坏或读取失败 ({e})，将重新拉取实时数据...")

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
    第一阶段漏斗：极速批量技术面筛选 + RS相对强度过滤
    加入基准 IWM (Russell 2000 ETF) 计算相对强度
    """
    print(f"⏳ 开始第一阶段：批量下载 {len(tickers)} 只股票及大盘基准(IWM)数据...")
    
    download_list = tickers + ['IWM']
    data = yf.download(download_list, period="1y", group_by="ticker", threads=True, show_errors=False)
    
    iwm_close = None
    if isinstance(data.columns, pd.MultiIndex) and 'IWM' in data.columns.get_level_values(0):
        iwm_close = data['IWM']['Close']
        if iwm_close is not None:
            iwm_close.index = iwm_close.index.tz_localize(None)
    else:
        print("⚠️ 无法获取大盘基准数据，跳过相对强度过滤。")

    passed_tickers = []
    
    for sym in tickers:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                if sym not in data.columns.get_level_values(0):
                    continue
                df = data[sym].copy()
            else:
                continue
                
            df = df.dropna(subset=['Close', 'Volume'])
            if len(df) < 150: 
                continue
            
            df.index = df.index.tz_localize(None)
            
            # 计算超级趋势与爆量
            df.ta.supertrend(length=7, multiplier=3.0, append=True)
            vol_95th = df['Volume'].rolling(window=60).quantile(0.95)
            
            # RS Line 相对强度计算
            rs_condition = True
            if iwm_close is not None:
                aligned_iwm = iwm_close.reindex(df.index).ffill()
                rs_line = df['Close'] / aligned_iwm
                rs_sma50 = rs_line.rolling(window=50).mean()
                if not pd.isna(rs_sma50.iloc[-1]):
                    rs_condition = rs_line.iloc[-1] > rs_sma50.iloc[-1]

            current_vol = df['Volume'].iloc[-1]
            last_vol_95 = vol_95th.iloc[-1]
            
            st_dir_cols = [col for col in df.columns if col.startswith('SUPERTd_')]
            if not st_dir_cols:
                continue
            st_dir_col = st_dir_cols[0]
            st_dir = df[st_dir_col].iloc[-1]
            
            if st_dir == 1 and current_vol > last_vol_95 and rs_condition:
                passed_tickers.append(sym)
                
        except Exception:
            continue
            
    print(f"🎯 第一阶段完成：技术面与相对强度(RS)初筛保留 {len(passed_tickers)} 只硬核标的")
    return passed_tickers

def analyze_fundamentals(symbol):
    """
    第二阶段漏斗：精准财务基本面过滤 (只对初筛通过的股票执行)
    """
    # 【修正6】：防并发封禁，加入 0.5 ~ 2 秒随机休眠
    time.sleep(random.uniform(0.5, 2.0))
    
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        if not info or 'longName' not in info:
            return None

        market_cap = info.get('marketCap', 0)
        if not (5e7 < market_cap < 2e9): 
            return None

        name = info.get('longName', symbol)
        sector = info.get('sector', '')

        revenue_growth = info.get('revenueGrowth')
        if revenue_growth is not None and revenue_growth < 0.20:
            return None

        gross_margin = info.get('grossMargins')
        if gross_margin is not None and gross_margin < 0.20:
            return None

        total_revenue = info.get('totalRevenue', 0)
        rd_expense = info.get('researchAndDevelopment', 0)
        rd_ratio = rd_expense / total_revenue if total_revenue > 0 else 0
        
        if sector in {'Healthcare', 'Technology'} and rd_ratio < 0.08:
            return None

        rg_str = f"{revenue_growth:.1%}" if revenue_growth is not None else "N/A"
        gm_str = f"{gross_margin:.1%}" if gross_margin is not None else "N/A"

        reasons = (
            f"市值: {market_cap/1e8:.1f}亿 | "
            f"营收增速: {rg_str} | "
            f"毛利率: {gm_str} | "
            f"研发: {rd_ratio:.1%} | "
            f"异动信号: 趋势向上 + 爆量 + RS跑赢大盘"
        )

        return {
            '股票代码': symbol,
            '公司名称': name,
            '所属行业': sector,
            '市值(亿美元)': round(market_cap / 1e8, 2),
            '营收增速': rg_str,
            '筛选理由': reasons,
            '链接': f"https://finance.yahoo.com/quote/{symbol}"
        }

    except Exception:
        return None

def send_notifications(df):
    """多平台推送模块 (含推送状态汇总)"""
    if df.empty:
        print("📭 今日无符合双重严苛条件的标的，不发送通知。")
        return
        
    summary = f"🚀 AI 驱动：GrowthHunter V3.4 异动播报\n\n共捕获 {len(df)} 只跑赢大盘的高潜股！\n\n"
    for _, row in df.head(10).iterrows():
        summary += f"• [{row['股票代码']}] {row['公司名称']} ({row['市值(亿美元)']}亿)\n  └ {row['筛选理由']}\n\n"
    
    # 【修正8】：推送状态汇总清单
    status_report = []
    
    # 微信 (Server酱)
    serverchan_key = os.getenv('SERVERCHAN_KEY')
    if serverchan_key:
        try:
            res = requests.get(f"https://sctapi.ftqq.com/{serverchan_key}.send",
                         params={"title": f"🚀 AI 发现 {len(df)} 只异动 10 倍候选股", "desp": summary})
            if res.status_code == 200:
                status_report.append("✅ 微信 (Server酱): 推送成功")
            else:
                status_report.append(f"❌ 微信 (Server酱): 推送异常 ({res.text})")
        except Exception as e:
            status_report.append(f"❌ 微信 (Server酱): 请求失败 ({e})")
    else:
        status_report.append("⚪ 微信 (Server酱): 未配置，跳过")
            
    # Telegram
    token = os.getenv('TELEGRAM_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    if token and chat_id:
        try:
            res = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          json={"chat_id": chat_id, "text": summary, "parse_mode": "Markdown"})
            if res.status_code == 200:
                status_report.append("✅ Telegram: 推送成功")
            else:
                status_report.append(f"❌ Telegram: 推送异常 ({res.text})")
        except Exception as e:
            status_report.append(f"❌ Telegram: 请求失败 ({e})")
    else:
        status_report.append("⚪ Telegram: 未配置，跳过")

    # 飞书
    feishu_webhook = os.getenv('FEISHU_WEBHOOK')
    if feishu_webhook:
        try:
            res = requests.post(feishu_webhook, json={"msg_type": "text", "content": {"text": summary}})
            if res.status_code == 200:
                status_report.append("✅ 飞书: 推送成功")
            else:
                status_report.append(f"❌ 飞书: 推送异常 ({res.text})")
        except Exception as e:
            status_report.append(f"❌ 飞书: 请求失败 ({e})")
    else:
        status_report.append("⚪ 飞书: 未配置，跳过")

    # 钉钉
    dingtalk_webhook = os.getenv('DINGTALK_WEBHOOK')
    if dingtalk_webhook:
        try:
            res = requests.post(dingtalk_webhook, json={"msgtype": "text", "text": {"content": summary}})
            if res.status_code == 200 and res.json().get('errcode') == 0:
                status_report.append("✅ 钉钉: 推送成功")
            else:
                status_report.append(f"❌ 钉钉: 被拒绝 (可能缺少关键词 'AI') - {res.text}")
        except Exception as e:
            status_report.append(f"❌ 钉钉: 请求失败 ({e})")
    else:
        status_report.append("⚪ 钉钉: 未配置，跳过")

    # 打印最终的推送汇总面板
    print("\n" + "="*35)
    print(" 📢 推送状态汇总")
    print("="*35)
    for status in status_report:
        print(status)
    print("="*35 + "\n")

def test_notifications():
    """用于独立测试推送配置是否成功"""
    print("🔧 启动推送测试模式...")
    mock_data = [{
        '股票代码': 'TEST',
        '公司名称': '配置测试专用股',
        '市值(亿美元)': 88.8,
        '筛选理由': 'AI 测试通知：反爬与稳定性Bug修补完成！如果你能看到这条消息，说明你的配置正确。'
    }]
    df = pd.DataFrame(mock_data)
    send_notifications(df)

def main():
    print("="*40)
    print(" 🚀 启动 GrowthHunter V3.4 (稳定性增强版)")
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
    
    # 【修正6】：将并发数降至 4，配合前面的 random sleep，避免触发封禁
    with ThreadPoolExecutor(max_workers=4) as executor:
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
            f.write("**量化策略**：SuperTrend右侧 + 95分位爆量突破 + RS跑赢大盘 + >20%营收增长 + 高研发驱动\n\n")
            f.write(df.to_markdown(index=False))
            
        print(f"\n🎉 筛选大功告成！最终捕获 {len(results)} 只硬核标的。")
    else:
        print("\n📉 遗憾：财务数据未达标或大盘太弱，今日空仓。")

    send_notifications(df)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        test_notifications()
    else:
        main()
