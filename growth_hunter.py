"""
🚀 GrowthHunter V4.0 - 10倍股猎手 (第4步：PEAD跳空漂移 + 终极工程优化版)
策略进化：加入向上跳空缺口(Gap Up)识别，锁定机构事件驱动标的
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
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

warnings.filterwarnings('ignore')

# 随机 User-Agent 池
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
]

# 【优化9】：配置线程本地存储 (Thread Local)，为每个线程分配独享的 Session
thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        thread_local.session.headers.update({'User-Agent': random.choice(USER_AGENTS)})
    return thread_local.session

def get_small_cap_tickers():
    """获取小盘股代码：优先缓存 -> 实时 Russell 2000 -> 备用 S&P 600"""
    cache_path = 'small_cap_cache.csv'
    
    if os.path.exists(cache_path):
        try:
            cache_age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_path))).days
            if cache_age < 7:
                df = pd.read_csv(cache_path)
                print(f"✅ 使用本地缓存小盘股池（{len(df)} 只）")
                return df['Symbol'].tolist()
        except Exception as e:
            print(f"⚠️ 缓存文件损坏或读取失败 ({e})，将重新拉取...")

    sources = [
        'https://stockanalysis.com/list/russell-2000/',
        'https://www.marketbeat.com/russell-2000/'
    ]
    
    for url in sources:
        try:
            headers = {'User-Agent': random.choice(USER_AGENTS)}
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            tables = pd.read_html(response.text)
            df = tables[0]
            tickers = df['Symbol'].tolist()
            pd.DataFrame(tickers, columns=['Symbol']).to_csv(cache_path, index=False)
            print(f"✅ 成功加载 Russell 2000 共 {len(tickers)} 只")
            return tickers
        except Exception as e:
            print(f"⚠️ 从 {url} 获取源数据失败: {e}")
            continue

    print("⚠️ Russell 2000 所有源失效，自动回退抓取 S&P 600 小盘股...")
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_600_companies'
        headers = {'User-Agent': random.choice(USER_AGENTS)}
        response = requests.get(url, headers=headers, timeout=15)
        
        table = pd.read_html(response.text)[0]
        tickers = table['Symbol'].tolist()
        pd.DataFrame(tickers, columns=['Symbol']).to_csv(cache_path, index=False)
        print(f"✅ 成功加载 S&P 600 共 {len(tickers)} 只")
        return tickers
    except Exception as e:
        print(f"❌ 所有股票池获取失败: {e}")
        return []

def check_unfilled_gap(df, lookback=10):
    """
    PEAD 严格跳空缺口校验：不仅要跳空，还要后续从未回补。
    ⚠️ 核心假设说明 (防盘中误判)：
    若策略在盘中运行，当日的跳空(即 len(post_gap_lows) == 0)暂视为未回补。
    因 df['Low'] 会随盘中价格实时更新，若盘中回补缺口，gap_up 将在底层变为 False。
    为确保数据绝对沉淀，本策略最佳运行时间为【每日收盘后】。
    """
    if len(df) < lookback + 2: 
        return False
        
    # 1. 识别跳空日：当日最低价 > 前日最高价，且开盘跳空幅度 > 2%
    gap_up = (df['Low'] > df['High'].shift(1)) & (df['Open'] > df['Close'].shift(1) * 1.02)
    recent_gaps = gap_up.iloc[-lookback:]
    gap_indices = recent_gaps[recent_gaps].index
    
    for gd in gap_indices:
        gap_idx = df.index.get_loc(gd)
        if gap_idx < 1: 
            continue
            
        pre_gap_high = df['High'].iloc[gap_idx - 1]
        post_gap_lows = df['Low'].iloc[gap_idx + 1:]
        
        # 2. 若跳空发生在最后一条 K 线 (今日)，且 gap_up 依然成立，视为尚未回补
        if len(post_gap_lows) == 0:
            return True
            
        # 3. 检查跳空日之后的所有最低价，是否从未跌破跳空前一日的最高价
        if (post_gap_lows > pre_gap_high).all():
            return True
            
    return False

def batch_technical_screen(tickers):
    """
    第一阶段漏斗：技术面 + RS相对强度 + Squeeze + CMF + PEAD跳空识别
    """
    print(f"⏳ 开始第一阶段：批量下载 {len(tickers)} 只股票及大盘基准(IWM)数据...")
    
    download_list = tickers + ['IWM']
    
    # 【优化10】：增加 yfinance 网络波动重试机制 (最多 3 次)
    data = None
    for attempt in range(3):
        data = yf.download(download_list, period="1y", group_by="ticker", threads=True, show_errors=False)
        if data is not None and not data.empty:
            break
        print(f"⚠️ Yahoo Finance 接口返回空数据，等待 3 秒后重试 ({attempt+1}/3)...")
        time.sleep(3)
    
    if data is None or data.empty:
        print("❌ Yahoo Finance 接口请求失败或重试耗尽，本次筛选终止。")
        return []
    
    # 提取 IWM 基准并处理时区
    iwm_close = None
    if isinstance(data.columns, pd.MultiIndex) and 'IWM' in data.columns.get_level_values(0):
        iwm_close = data['IWM']['Close']
    elif not isinstance(data.columns, pd.MultiIndex) and 'Close' in data.columns and 'IWM' in tickers:
        iwm_close = data['Close']
        
    if iwm_close is not None:
        if getattr(iwm_close.index, 'tz', None) is not None:
            iwm_close.index = iwm_close.index.tz_localize(None)
    else:
        # 【终极优化6】：IWM 缺失则快速失败
        print("❌ 核心基准(IWM)获取失败，无法计算相对强度，终止流程。")
        return []

    passed_tickers = []
    
    for sym in tickers:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                if sym not in data.columns.get_level_values(0): continue
                df = data[sym].copy()
            else:
                if 'Close' not in data.columns: continue
                df = data.copy()
                
            df = df.dropna(subset=['Close', 'Volume'])
            if len(df) < 150: continue
            if getattr(df.index, 'tz', None) is not None:
                df.index = df.index.tz_localize(None)
            
            # 1. 指标计算
            df.ta.supertrend(length=7, multiplier=3.0, append=True)
            df.ta.atr(length=20, append=True) 
            
            # 【撤销错误补丁】：完全移除直接修改原始 High 价格的逻辑，信任 pandas_ta 内部安全的防除零机制
            df.ta.cmf(length=20, append=True)
            
            vol_95th = df['Volume'].rolling(window=60).quantile(0.95)
            vol_ma20 = df['Volume'].rolling(window=20).mean()
            
            atr_cols = [col for col in df.columns if col.startswith('ATRr_20')]
            cmf_cols = [col for col in df.columns if col.startswith('CMF')]
            if not atr_cols or not cmf_cols: continue
            
            atr_col, cmf_col = atr_cols[0], cmf_cols[0]
            
            # 【优化8】：提前提取[-1]最后一行索引值，减少重复底层 iloc 开销
            idx = -1
            current_close = df['Close'].iloc[idx]
            current_vol = df['Volume'].iloc[idx]
            
            # 2. Squeeze 挤压突破 (昨日仍挤压，今日突破)
            sma20 = df['Close'].rolling(window=20).mean()
            std20 = df['Close'].rolling(window=20).std()
            bb_upper, bb_lower = sma20 + 2 * std20, sma20 - 2 * std20
            kc_upper, kc_lower = sma20 + 1.5 * df[atr_col], sma20 - 1.5 * df[atr_col]
            squeeze_on = (bb_upper < kc_upper) & (bb_lower > kc_lower)
            
            # 【逻辑修正2】：防 NaN 与索引越界，安全获取昨日挤压状态
            yesterday_squeeze = squeeze_on.iloc[-2] if len(squeeze_on) >= 2 else False
            is_squeeze_break = (current_close > kc_upper.iloc[idx]) and (yesterday_squeeze == True)

            # 3. RS 相对强度 (跑赢大盘)
            rs_condition = True
            # 【逻辑修正3】：使用线性插值并辅以 bfill() 和 ffill()，彻底抹平两端任何可能的前导或滞后 NaN
            aligned_iwm = iwm_close.reindex(df.index).interpolate(method='linear').ffill().bfill()
            rs_line = df['Close'] / aligned_iwm
            rs_sma50 = rs_line.rolling(window=50).mean()
            if not pd.isna(rs_sma50.iloc[idx]):
                rs_condition = rs_line.iloc[idx] > rs_sma50.iloc[idx]

            # 4. 【逻辑修正1】：PEAD 严格跳空缺口识别 (调用刚刚补回来的辅助函数)
            has_unfilled_gap = check_unfilled_gap(df, lookback=10)

            # 提取关键值
            last_vol_95, last_vol_ma20 = vol_95th.iloc[idx], vol_ma20.iloc[idx]
            current_cmf = df[cmf_col].iloc[idx]
            
            if pd.isna(current_vol) or pd.isna(last_vol_95) or pd.isna(current_cmf): continue
            
            st_dir_cols = [col for col in df.columns if col.startswith('SUPERTd_')]
            if not st_dir_cols: continue
            st_dir = df[st_dir_cols[0]].iloc[idx]
            
            vol_threshold = max(last_vol_ma20 * 1.5, last_vol_95 * 0.8)
            
            # 综合漏斗判定
            if (st_dir == 1 and 
                current_vol > vol_threshold and 
                rs_condition and 
                (is_squeeze_break or has_unfilled_gap) and # Squeeze 突破或近期有强力跳空
                current_cmf > 0):
                
                passed_tickers.append(sym)
                
        except Exception:
            continue
            
    print(f"🎯 第一阶段完成：PEAD + 资金追踪漏斗保留 {len(passed_tickers)} 只标的")
    return passed_tickers

def analyze_fundamentals(symbol):
    """
    第二阶段漏斗：精准财务基本面过滤 (线程安全 TLS 链接池 + 回退计算)
    """
    time.sleep(random.uniform(0.2, 1.0))
    
    try:
        # 【优化9】：利用 Thread Local 安全获取线程独占的 Session
        local_session = get_session()
        ticker = yf.Ticker(symbol, session=local_session)
        
        info = ticker.info
        if not info or 'longName' not in info: return None

        # 市值严控 20 亿以内
        market_cap = info.get('marketCap', 0)
        if not (5e7 < market_cap < 2e9): return None

        # 营收增速回退计算 (YoY)
        revenue_growth = info.get('revenueGrowth')
        if revenue_growth is None:
            try:
                income_stmt = ticker.income_stmt
                if not income_stmt.empty and 'Total Revenue' in income_stmt.index:
                    revs = income_stmt.loc['Total Revenue'].dropna()
                    if len(revs) >= 2 and revs.iloc[1] > 0:
                        revenue_growth = (revs.iloc[0] - revs.iloc[1]) / revs.iloc[1]
            except Exception: pass
                
        if revenue_growth is None or revenue_growth < 0.20: return None

        # 毛利率回退计算 (Gross Margin)
        # 【终极优化5】：毛利率缺失回退计算
        gross_margin = info.get('grossMargins')
        if gross_margin is None:
            try:
                income_stmt = ticker.income_stmt
                if not income_stmt.empty and 'Gross Profit' in income_stmt.index and 'Total Revenue' in income_stmt.index:
                    gp = income_stmt.loc['Gross Profit'].dropna()
                    revs_gm = income_stmt.loc['Total Revenue'].dropna()
                    if len(gp) > 0 and len(revs_gm) > 0 and revs_gm.iloc[0] > 0:
                        gross_margin = gp.iloc[0] / revs_gm.iloc[0]
            except Exception: pass

        if gross_margin is None or gross_margin < 0.20: return None

        total_revenue = info.get('totalRevenue', 0)
        rd_expense = info.get('researchAndDevelopment')
        rd_ratio = (rd_expense / total_revenue) if (rd_expense and total_revenue > 0) else 0
        
        sector = info.get('sector', '')
        if sector in {'Healthcare', 'Technology'} and rd_ratio < 0.08: return None

        rg_str = f"{revenue_growth:.1%}" if revenue_growth is not None else "N/A"
        gm_str = f"{gross_margin:.1%}" if gross_margin is not None else "N/A"
        rd_str = f"{rd_ratio:.1%}" if rd_ratio > 0 else "N/A"

        reasons = (
            f"市值: {market_cap/1e8:.1f}亿 | "
            f"增速: {rg_str} | "
            f"毛利: {gm_str} | "
            f"研发: {rd_str} | "
            f"信号: 资金流入 + PEAD跳空/挤压起爆"
        )

        return {
            '股票代码': symbol,
            '公司名称': info.get('longName', symbol),
            '所属行业': sector,
            '市值(亿美元)': round(market_cap / 1e8, 2),
            '营收增速': rg_str,
            '筛选理由': reasons,
            '链接': f"https://finance.yahoo.com/quote/{symbol}"
        }

    except Exception:
        return None

def send_notifications(df):
    """多平台推送模块 (安全截断)"""
    if df.empty:
        print("📭 今日无符合双重严苛条件的标的。")
        return
        
    summary = f"🚀 AI 驱动：GrowthHunter V4.0 异动播报\n\n捕获 {len(df)} 只底池起爆/跳空高潜股！\n\n"
    max_len = 3500
    for _, row in df.head(10).iterrows():
        item_text = f"• [{row['股票代码']}] {row['公司名称']} ({row['市值(亿美元)']}亿)\n  └ {row['筛选理由']}\n\n"
        if len(summary) + len(item_text) > max_len:
            summary += "...（内容过长已自动截断）"
            break
        summary += item_text
        
    status_report = []
    # 推送逻辑 (恢复 Telegram 并优化报错详情)
    platforms = [
        ('微信', 'SERVERCHAN_KEY'), 
        ('飞书', 'FEISHU_WEBHOOK'), 
        ('钉钉', 'DINGTALK_WEBHOOK'),
        ('Telegram', 'TELEGRAM_TOKEN')
    ]
    
    for platform, env_key in platforms:
        val = os.getenv(env_key)
        if not val:
            status_report.append(f"⚪ {platform}: 未配置")
            continue
        try:
            if platform == '微信':
                res = requests.get(f"https://sctapi.ftqq.com/{val}.send", params={"title": "🚀 PEAD跳空起爆预警", "desp": summary})
            elif platform == 'Telegram':
                chat_id = os.getenv('TELEGRAM_CHAT_ID')
                if not chat_id:
                    status_report.append(f"⚪ Telegram: 未配置 CHAT_ID")
                    continue
                # 【优化12】：转义 Telegram Markdown 特殊字符（如下划线、星号），防止解析崩溃
                tg_summary = summary.replace('_', '\\_').replace('*', '\\*')
                res = requests.post(f"https://api.telegram.org/bot{val}/sendMessage", json={"chat_id": chat_id, "text": tg_summary, "parse_mode": "Markdown"})
            else:
                payload = {"msg_type": "text", "content": {"text": summary}} if platform == '飞书' else {"msgtype": "text", "text": {"content": summary}}
                res = requests.post(val, json=payload)
                
            if res.status_code == 200:
                status_report.append(f"✅ {platform}: 成功")
            else:
                status_report.append(f"❌ {platform}: 异常 ({res.text})")
        except Exception as e: 
            status_report.append(f"❌ {platform}: 失败 ({str(e)})")

    print("\n" + "="*30 + "\n 📢 推送汇总\n" + "="*30)
    for s in status_report: print(s)
    print("="*30 + "\n")

def test_notifications():
    print("🔧 启动推送测试模式...")
    mock_data = [{'股票代码': 'TEST', '公司名称': '配置测试股', '市值(亿美元)': 8.8, '筛选理由': 'PEAD & Squeeze 引擎双重就绪！'}]
    send_notifications(pd.DataFrame(mock_data))

def main():
    print("="*40 + "\n 🚀 GrowthHunter V4.0 (PEAD进化版)\n" + "="*40)
    tickers = get_small_cap_tickers()
    if not tickers: return

    passed_tech_tickers = batch_technical_screen(tickers)
    if not passed_tech_tickers: return

    results = []
    print(f"⏳ 第二阶段：深挖 {len(passed_tech_tickers)} 只股票财务数据...")
    with ThreadPoolExecutor(max_workers=4) as executor:
        # 移除了原有的 session 参数传递，直接调用
        futures = {executor.submit(analyze_fundamentals, sym): sym for sym in passed_tech_tickers}
        for f in as_completed(futures):
            res = f.result()
            if res: results.append(res)

    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values(by='市值(亿美元)')
        df.to_csv('growth_hunter_results.csv', index=False)
        try: md_table = df.to_markdown(index=False)
        except: md_table = df.to_string(index=False)
        with open('growth_hunter_results.md', 'w', encoding='utf-8') as f:
            f.write(f"# 🚀 GrowthHunter 严选报告\n\n**生成时间**：{datetime.now()}\n\n{md_table}")
        print(f"\n🎉 大功告成！捕获 {len(results)} 只硬核标的。")
    else: print("\n📉 今日无达标标的。")
    send_notifications(df)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--test': test_notifications()
    else: main()
