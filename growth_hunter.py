"""
🚀 GrowthHunter V5.0 - 10倍股猎手 (最终旗舰版：Piotroski F-Score 财务体检)
策略进化：加入 F-Score 9分制财务健康评分，确保异动标的底盘扎实。
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
import re
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

# 配置线程本地存储 (Thread Local)，为每个线程分配独享的 Session
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
    """
    if len(df) < lookback + 2: 
        return False
        
    gap_up = (df['Low'] > df['High'].shift(1)) & (df['Open'] > df['Close'].shift(1) * 1.02)
    recent_gaps = gap_up.iloc[-lookback:]
    gap_indices = recent_gaps[recent_gaps].index
    
    for gd in gap_indices:
        gap_idx = df.index.get_loc(gd)
        if gap_idx < 1: 
            continue
            
        pre_gap_high = df['High'].iloc[gap_idx - 1]
        post_gap_lows = df['Low'].iloc[gap_idx + 1:]
        
        if len(post_gap_lows) == 0:
            # 盘中实时防护：即使没有后续天数，也需再次确认最新 Low 未在日内跌破前高
            if df['Low'].iloc[-1] > pre_gap_high:
                return True
            continue
            
        if (post_gap_lows > pre_gap_high).all():
            return True
            
    return False

def batch_technical_screen(tickers):
    """
    第一阶段漏斗：技术面 + RS相对强度 + Squeeze + CMF + PEAD跳空识别
    """
    print(f"⏳ 开始第一阶段：批量下载 {len(tickers)} 只股票及大盘基准(IWM)数据...")
    
    download_list = tickers + ['IWM']
    
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
    
    iwm_close = None
    if isinstance(data.columns, pd.MultiIndex) and 'IWM' in data.columns.get_level_values(0):
        iwm_close = data['IWM']['Close']
    elif not isinstance(data.columns, pd.MultiIndex) and 'Close' in data.columns and 'IWM' in tickers:
        iwm_close = data['Close']
        
    if iwm_close is not None:
        if getattr(iwm_close.index, 'tz', None) is not None:
            iwm_close.index = iwm_close.index.tz_localize(None)
    else:
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
            
            df.ta.supertrend(length=7, multiplier=3.0, append=True)
            df.ta.atr(length=20, append=True) 
            df.ta.cmf(length=20, append=True)
            
            vol_95th = df['Volume'].rolling(window=60).quantile(0.95)
            vol_ma20 = df['Volume'].rolling(window=20).mean()
            
            atr_cols = [col for col in df.columns if col.startswith('ATRr_20')]
            cmf_cols = [col for col in df.columns if col.startswith('CMF')]
            if not atr_cols or not cmf_cols: continue
            
            atr_col, cmf_col = atr_cols[0], cmf_cols[0]
            
            idx = -1
            current_close = df['Close'].iloc[idx]
            current_vol = df['Volume'].iloc[idx]
            
            sma20 = df['Close'].rolling(window=20).mean()
            std20 = df['Close'].rolling(window=20).std()
            bb_upper, bb_lower = sma20 + 2 * std20, sma20 - 2 * std20
            kc_upper, kc_lower = sma20 + 1.5 * df[atr_col], sma20 - 1.5 * df[atr_col]
            squeeze_on = (bb_upper < kc_upper) & (bb_lower > kc_lower)
            
            yesterday_squeeze = squeeze_on.iloc[-2] if len(squeeze_on) >= 2 else False
            is_squeeze_break = (current_close > kc_upper.iloc[idx]) and (yesterday_squeeze == True)

            # 默认 RS 不合格，严防缺数据导致的放行
            rs_condition = False
            aligned_iwm = iwm_close.reindex(df.index).interpolate(method='linear').ffill()
            rs_line = df['Close'] / aligned_iwm
            rs_sma50 = rs_line.rolling(window=50).mean()
            if not pd.isna(rs_sma50.iloc[idx]) and not pd.isna(rs_line.iloc[idx]):
                rs_condition = rs_line.iloc[idx] > rs_sma50.iloc[idx]

            has_unfilled_gap = check_unfilled_gap(df, lookback=10)

            last_vol_95, last_vol_ma20 = vol_95th.iloc[idx], vol_ma20.iloc[idx]
            current_cmf = df[cmf_col].iloc[idx]
            
            if pd.isna(current_vol) or pd.isna(last_vol_95) or pd.isna(current_cmf): continue
            
            st_dir_cols = [col for col in df.columns if col.startswith('SUPERTd_')]
            if not st_dir_cols: continue
            st_dir = df[st_dir_cols[0]].iloc[idx]
            
            vol_threshold = max(last_vol_ma20 * 1.5, last_vol_95 * 0.8)
            
            if (st_dir == 1 and 
                current_vol > vol_threshold and 
                rs_condition and 
                (is_squeeze_break or has_unfilled_gap) and 
                current_cmf > 0):
                
                passed_tickers.append(sym)
                
        except Exception:
            continue
            
    print(f"🎯 第一阶段完成：PEAD + 资金追踪漏斗保留 {len(passed_tickers)} 只标的")
    return passed_tickers

def calculate_piotroski_f_score(bs, cf, inc):
    """
    【进化5】：Piotroski F-Score 复合财务健康评分 (0-9分)
    参数已修改为直接传入财务表 DataFrame，避免重复请求
    """
    f_score = 0
    try:
        if bs is None or cf is None or inc is None:
            return "N/A"
        if bs.empty or cf.empty or inc.empty or len(bs.columns) < 2 or len(inc.columns) < 2:
            return "N/A"
            
        def get_val(df, keys, idx=0):
            if df.empty or len(df.columns) <= idx:
                return 0
            try:
                sorted_cols = sorted(df.columns, reverse=True)
                df = df[sorted_cols]
            except Exception:
                pass
            df_idx_lower = {str(k).lower().strip(): k for k in df.index}
            for key in keys:
                key_lower = key.lower().strip()
                # 仅保留严格的脱敏匹配，杜绝意外包含导致的错误挂载
                if key_lower in df_idx_lower:
                    val = df.loc[df_idx_lower[key_lower]].iloc[idx]
                    if not pd.isna(val): return val
            return 0

        # 当前期 (全面扩充常见字段别名以防漏判)
        ni = get_val(inc, ['Net Income Common Stockholders', 'Net Income', 'Net Income From Continuing And Discontinued Operation', 'Net Income Including Noncontrolling Interests'])
        ta_cur = get_val(bs, ['Total Assets'])
        cfo = get_val(cf, ['Operating Cash Flow', 'Cash Flow From Operating Activities', 'Total Cash From Operating Activities', 'Net Cash Provided By Operating Activities'])
        lt_debt = get_val(bs, ['Long Term Debt', 'Total Long Term Debt', 'Long Term Debt And Capital Lease Obligation', 'Long Term Debt Noncurrent'])
        cur_assets = get_val(bs, ['Current Assets', 'Total Current Assets'])
        cur_liab = get_val(bs, ['Current Liabilities', 'Total Current Liabilities'])
        shares = get_val(bs, ['Ordinary Shares Number', 'Share Issued', 'Basic Average Shares', 'Diluted Average Shares'])
        gp = get_val(inc, ['Gross Profit', 'Total Gross Profit'])
        rev = get_val(inc, ['Total Revenue', 'Operating Revenue', 'Revenue'])
        
        # 上一期
        ni_prev = get_val(inc, ['Net Income Common Stockholders', 'Net Income', 'Net Income From Continuing And Discontinued Operation', 'Net Income Including Noncontrolling Interests'], 1)
        ta_prev = get_val(bs, ['Total Assets'], 1)
        lt_debt_prev = get_val(bs, ['Long Term Debt', 'Total Long Term Debt', 'Long Term Debt And Capital Lease Obligation', 'Long Term Debt Noncurrent'], 1)
        cur_assets_prev = get_val(bs, ['Current Assets', 'Total Current Assets'], 1)
        cur_liab_prev = get_val(bs, ['Current Liabilities', 'Total Current Liabilities'], 1)
        shares_prev = get_val(bs, ['Ordinary Shares Number', 'Share Issued', 'Basic Average Shares', 'Diluted Average Shares'], 1)
        gp_prev = get_val(inc, ['Gross Profit', 'Total Gross Profit'], 1)
        rev_prev = get_val(inc, ['Total Revenue', 'Operating Revenue', 'Revenue'], 1)

        # 盈利能力
        roa = ni / ta_cur if ta_cur else 0
        roa_prev = ni_prev / ta_prev if ta_prev else 0
        if roa > 0: f_score += 1                     # 1. ROA 为正
        if cfo > 0: f_score += 1                     # 2. CFO 为正
        if roa > roa_prev: f_score += 1              # 3. ROA 增长
        if cfo > ni: f_score += 1                    # 4. 盈余质量 (CFO > NI)

        # 杠杆、流动性与资金来源
        lev = lt_debt / ta_cur if ta_cur else 0
        lev_prev = lt_debt_prev / ta_prev if ta_prev else 0
        if lev < lev_prev: f_score += 1              # 5. 长期债务杠杆降低
        
        cr = cur_assets / cur_liab if cur_liab else 0
        cr_prev = cur_assets_prev / cur_liab_prev if cur_liab_prev else 0
        if cr > cr_prev: f_score += 1                # 6. 流动比率改善
        
        if shares > 0 and shares_prev > 0 and shares <= shares_prev: f_score += 1 # 7. 未增发新股

        # 运营效率
        gm = gp / rev if rev else 0
        gm_prev = gp_prev / rev_prev if rev_prev else 0
        if gm > gm_prev: f_score += 1                # 8. 毛利率提升
        
        ato = rev / ta_cur if ta_cur else 0
        ato_prev = rev_prev / ta_prev if ta_prev else 0
        if ato > ato_prev: f_score += 1              # 9. 资产周转率提升

        return f_score
    except Exception:
        return "N/A"

def analyze_fundamentals(symbol):
    """
    第二阶段漏斗：精准财务基本面过滤 (增加 F-Score 体检)
    """
    time.sleep(random.uniform(0.2, 1.0))
    
    for attempt in range(2):
        try:
            local_session = get_session()
            ticker = yf.Ticker(symbol, session=local_session)
            
            info = ticker.info
            if not info or 'longName' not in info: return None

            market_cap = info.get('marketCap', 0)
            if not (5e7 < market_cap < 2e9): return None

            # 统一提前提取财务报表，大幅减少重复的 HTTP 请求
            inc = ticker.income_stmt
            bs = ticker.balance_sheet
            cf = ticker.cashflow

            revenue_growth = info.get('revenueGrowth')
            
            # 抽取营收数据，加入常见别名防漏判
            rev_row = None
            if not inc.empty:
                for k in ['Total Revenue', 'Operating Revenue', 'Revenue']:
                    if k in inc.index:
                        rev_row = inc.loc[k].dropna()
                        break

            if revenue_growth is None and rev_row is not None:
                try:
                    if len(rev_row) >= 2 and rev_row.iloc[1] > 0:
                        revenue_growth = (rev_row.iloc[0] - rev_row.iloc[1]) / rev_row.iloc[1]
                except Exception: pass
                    
            if revenue_growth is None or revenue_growth < 0.20: return None

            gross_margin = info.get('grossMargins')
            if gross_margin is None:
                try:
                    gp_row = None
                    if not inc.empty:
                        for k in ['Gross Profit', 'Total Gross Profit']:
                            if k in inc.index:
                                gp_row = inc.loc[k].dropna()
                                break
                    if gp_row is not None and rev_row is not None and len(gp_row) > 0 and len(rev_row) > 0 and rev_row.iloc[0] > 0:
                        gross_margin = gp_row.iloc[0] / rev_row.iloc[0]
                except Exception: pass

            if gross_margin is None or gross_margin < 0.20: return None
            
            # 传入已获取的报表参数，防止 F-Score 内部重复发起请求
            f_score = calculate_piotroski_f_score(bs, cf, inc)
            
            # 剔除 F-Score 低于 5 分的财务弱势股，以及数据严重缺失(N/A)无法完成体检的标的
            if not isinstance(f_score, int) or f_score < 5:
                return None

            total_revenue = info.get('totalRevenue', 0)
            rd_expense = info.get('researchAndDevelopment')
            rd_ratio = (rd_expense / total_revenue) if (rd_expense and total_revenue > 0) else 0
            
            sector = info.get('sector', '')
            if sector in {'Healthcare', 'Technology'} and rd_ratio < 0.08: return None

            rg_str = f"{revenue_growth:.1%}" if revenue_growth is not None else "N/A"
            gm_str = f"{gross_margin:.1%}" if gross_margin is not None else "N/A"
            rd_str = f"{rd_ratio:.1%}" if rd_ratio > 0 else "N/A"
            f_score_str = f"{f_score}/9" if isinstance(f_score, int) else "N/A"

            reasons = (
                f"市值: {market_cap/1e8:.1f}亿 | "
                f"增速: {rg_str} | "
                f"毛利: {gm_str} | "
                f"F-Score: {f_score_str} | "
                f"信号: 资金流入 + PEAD跳空/挤压起爆"
            )

            return {
                '股票代码': symbol,
                '公司名称': info.get('longName', symbol),
                '所属行业': sector,
                '市值(亿美元)': round(market_cap / 1e8, 2),
                '营收增速': rg_str,
                'F-Score': f_score_str,
                '筛选理由': reasons,
                '链接': f"https://finance.yahoo.com/quote/{symbol}"
            }

        except Exception:
            if hasattr(thread_local, "session"):
                try:
                    thread_local.session.close()
                except Exception:
                    pass
                del thread_local.session
            if attempt == 1: 
                return None

def send_notifications(df):
    """多平台推送模块"""
    if df.empty:
        print("📭 今日无符合双重严苛条件的标的。")
        return
        
    summary = f"🚀 AI 驱动：GrowthHunter V5.0 异动播报\n\n捕获 {len(df)} 只财务满级的高潜起爆股！\n\n"
    max_len = 3500
    for _, row in df.head(10).iterrows():
        item_text = f"• [{row['股票代码']}] {row['公司名称']} ({row['市值(亿美元)']}亿) 🌟 F-Score: {row.get('F-Score', 'N/A')}\n  └ {row['筛选理由']}\n\n"
        if len(summary) + len(item_text) > max_len:
            summary += "...（内容过长已自动截断）"
            break
        summary += item_text
        
    status_report = []
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
                res = requests.get(f"https://sctapi.ftqq.com/{val}.send", params={"title": "🚀 F-Score及格起爆预警", "desp": summary})
            elif platform == 'Telegram':
                chat_id = os.getenv('TELEGRAM_CHAT_ID')
                if not chat_id:
                    status_report.append(f"⚪ Telegram: 未配置 CHAT_ID")
                    continue
                # 升级为 MarkdownV2 并使用正则全量转义特殊字符，杜绝解析崩溃
                tg_summary = re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', summary)
                res = requests.post(f"https://api.telegram.org/bot{val}/sendMessage", json={"chat_id": chat_id, "text": tg_summary, "parse_mode": "MarkdownV2"})
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
    mock_data = [{'股票代码': 'TEST', '公司名称': '配置测试股', '市值(亿美元)': 8.8, 'F-Score': '9/9', '筛选理由': 'F-Score 财务体检系统全线就绪！'}]
    send_notifications(pd.DataFrame(mock_data))

def main():
    print("="*40 + "\n 🚀 GrowthHunter V5.0 (旗舰大结局版)\n" + "="*40)
    tickers = get_small_cap_tickers()
    if not tickers: return

    passed_tech_tickers = batch_technical_screen(tickers)
    if not passed_tech_tickers: return

    results = []
    print(f"⏳ 第二阶段：深挖 {len(passed_tech_tickers)} 只股票财务与 F-Score 体检...")
    with ThreadPoolExecutor(max_workers=4) as executor:
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
