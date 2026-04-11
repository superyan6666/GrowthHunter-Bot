"""
🚀 GrowthHunter V6.0 - 10倍股猎手 (霸王龙全能版)
策略进化：全量集成期权异动(Gamma Squeeze)、NLP新闻催化剂与 SQLite 本地数据库归档
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
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import warnings

warnings.filterwarnings('ignore')

# 随机 User-Agent 池
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
]

# 配置线程本地存储 (Thread Local)
thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        thread_local.session.headers.update({'User-Agent': random.choice(USER_AGENTS)})
    return thread_local.session

# ==========================================
# 终极模块 C：SQLite 本地信号归档数据库
# ==========================================
def init_db():
    """初始化本地 SQLite 数据库，建立信号档案库"""
    conn = sqlite3.connect('growth_hunter_signals.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS signals (
            date TEXT,
            symbol TEXT,
            name TEXT,
            market_cap REAL,
            revenue_growth TEXT,
            f_score TEXT,
            options_flow TEXT,
            catalyst TEXT,
            close_price REAL,
            UNIQUE(date, symbol)
        )
    ''')
    conn.commit()
    conn.close()

def save_signals_to_db(df):
    """将当天的有效信号保存进数据库，用于后期自动复盘"""
    try:
        conn = sqlite3.connect('growth_hunter_signals.db')
        today = datetime.now().strftime('%Y-%m-%d')
        for _, row in df.iterrows():
            conn.execute('''
                INSERT OR IGNORE INTO signals 
                (date, symbol, name, market_cap, revenue_growth, f_score, options_flow, catalyst, close_price)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (today, row['股票代码'], row['公司名称'], row['市值(亿美元)'], 
                  row['营收增速'], row['F-Score'], row['期权异动'], row['催化剂'], row.get('最新收盘价', 0.0)))
        conn.commit()
        conn.close()
        print("💾 信号已成功归档至本地 SQLite 数据库 (growth_hunter_signals.db)")
    except Exception as e:
        print(f"⚠️ 数据库保存失败: {e}")

# ==========================================
# 终极模块 B：期权异动与 Gamma 挤压扫描
# ==========================================
def analyze_options_flow(ticker):
    """拉取最近一期期权链，扫描看跌看涨比(PCR)与异常爆单（带超时防护）"""
    def _fetch_options():
        opts = ticker.options
        if not opts: return None, "无期权"
        return ticker.option_chain(opts[0]), None

    try:
        # 增加超时控制，防止 yfinance 请求无限挂起拖死整个线程池
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_fetch_options)
            try:
                # 设定 8 秒强行超时限制
                chain, err_msg = future.result(timeout=8)
            except TimeoutError:
                return "请求超时"
            except Exception:
                return "获取失败"
                
        if err_msg: return err_msg
        if chain is None: return "获取失败"
        
        calls = chain.calls
        puts = chain.puts
        
        if calls.empty or puts.empty: return "数据不全"
        
        call_vol = calls['volume'].fillna(0).sum()
        put_vol = puts['volume'].fillna(0).sum()
        
        if put_vol == 0 and call_vol == 0: return "交投清淡"
        
        pcr = put_vol / call_vol if call_vol > 0 else 9.9
        
        # 寻找看涨期权异常爆单：成交量(volume)大于未平仓量(openInterest)
        unusual_calls = calls[(calls['volume'] > 0) & (calls['openInterest'] > 0) & (calls['volume'] > calls['openInterest'] * 1.5)]
        
        if not unusual_calls.empty and pcr < 0.7:
            return f"🔥 看涨爆单 (PCR: {pcr:.2f})"
        elif pcr > 1.5:
            return f"⚠️ 偏空防御 (PCR: {pcr:.2f})"
        else:
            return f"中性 (PCR: {pcr:.2f})"
    except Exception:
        return "获取失败"

# ==========================================
# 终极模块 A：NLP 新闻情绪与催化剂提取
# ==========================================
def analyze_catalyst(ticker):
    """基于内置词典的极速 NLP 新闻标题情绪打分"""
    try:
        news = ticker.news
        if not news: return "无最新消息"
        
        # 轻量级情绪词库
        pos_words = ['beat', 'surge', 'upgrade', 'fda', 'acquire', 'buy', 'profit', 'record', 'high', 'breakout', 'partner', 'approval']
        neg_words = ['miss', 'downgrade', 'lawsuit', 'investigate', 'offering', 'dilution', 'decline', 'drop', 'cut', 'warning', 'reject']
        
        score = 0
        latest_title = news[0].get('title', '无标题')
        
        # 扫描最近 3 条新闻判定综合情绪
        for n in news[:3]:
            title = n.get('title', '').lower()
            if any(w in title for w in pos_words): score += 1
            if any(w in title for w in neg_words): score -= 1
        
        short_title = latest_title[:25] + "..." if len(latest_title) > 25 else latest_title
        
        if score > 0: return f"🚀 利好 ({short_title})"
        elif score < 0: return f"⚠️ 偏空 ({short_title})"
        else: return f"中性 ({short_title})"
    except Exception:
        return "获取失败"


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
    """PEAD 严格跳空缺口校验：不仅要跳空，还要后续从未回补"""
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
        return [], {}
    
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
        return [], {}

    passed_tickers = []
    close_prices = {} # 用于保存收盘价供数据库存储
    
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
                close_prices[sym] = current_close
                
        except Exception:
            continue
            
    print(f"🎯 第一阶段完成：技术与资金面保留 {len(passed_tickers)} 只标的")
    return passed_tickers, close_prices

def calculate_piotroski_f_score(bs, cf, inc):
    """Piotroski F-Score 复合财务健康评分 (0-9分)"""
    f_score = 0
    try:
        if bs is None or cf is None or inc is None: return "N/A"
        if bs.empty or cf.empty or inc.empty or len(bs.columns) < 2 or len(inc.columns) < 2: return "N/A"
            
        def get_val(df, keys, idx=0):
            if df.empty or len(df.columns) <= idx: return 0
            try:
                sorted_cols = sorted(df.columns, reverse=True)
                df = df[sorted_cols]
            except Exception: pass
            df_idx_lower = {str(k).lower().strip(): k for k in df.index}
            for key in keys:
                key_lower = key.lower().strip()
                if key_lower in df_idx_lower:
                    val = df.loc[df_idx_lower[key_lower]].iloc[idx]
                    if not pd.isna(val): return val
            return 0

        ni = get_val(inc, ['Net Income Common Stockholders', 'Net Income', 'Net Income From Continuing And Discontinued Operation', 'Net Income Including Noncontrolling Interests'])
        ta_cur = get_val(bs, ['Total Assets'])
        cfo = get_val(cf, ['Operating Cash Flow', 'Cash Flow From Operating Activities', 'Total Cash From Operating Activities', 'Net Cash Provided By Operating Activities'])
        lt_debt = get_val(bs, ['Long Term Debt', 'Total Long Term Debt', 'Long Term Debt And Capital Lease Obligation', 'Long Term Debt Noncurrent'])
        cur_assets = get_val(bs, ['Current Assets', 'Total Current Assets'])
        cur_liab = get_val(bs, ['Current Liabilities', 'Total Current Liabilities'])
        shares = get_val(bs, ['Ordinary Shares Number', 'Share Issued', 'Basic Average Shares', 'Diluted Average Shares'])
        gp = get_val(inc, ['Gross Profit', 'Total Gross Profit'])
        rev = get_val(inc, ['Total Revenue', 'Operating Revenue', 'Revenue'])
        
        ni_prev = get_val(inc, ['Net Income Common Stockholders', 'Net Income', 'Net Income From Continuing And Discontinued Operation', 'Net Income Including Noncontrolling Interests'], 1)
        ta_prev = get_val(bs, ['Total Assets'], 1)
        lt_debt_prev = get_val(bs, ['Long Term Debt', 'Total Long Term Debt', 'Long Term Debt And Capital Lease Obligation', 'Long Term Debt Noncurrent'], 1)
        cur_assets_prev = get_val(bs, ['Current Assets', 'Total Current Assets'], 1)
        cur_liab_prev = get_val(bs, ['Current Liabilities', 'Total Current Liabilities'], 1)
        shares_prev = get_val(bs, ['Ordinary Shares Number', 'Share Issued', 'Basic Average Shares', 'Diluted Average Shares'], 1)
        gp_prev = get_val(inc, ['Gross Profit', 'Total Gross Profit'], 1)
        rev_prev = get_val(inc, ['Total Revenue', 'Operating Revenue', 'Revenue'], 1)

        roa = ni / ta_cur if ta_cur else 0
        roa_prev = ni_prev / ta_prev if ta_prev else 0
        if roa > 0: f_score += 1
        if cfo > 0: f_score += 1
        if roa > roa_prev: f_score += 1
        if cfo > ni: f_score += 1

        lev = lt_debt / ta_cur if ta_cur else 0
        lev_prev = lt_debt_prev / ta_prev if ta_prev else 0
        if lev < lev_prev: f_score += 1
        
        cr = cur_assets / cur_liab if cur_liab else 0
        cr_prev = cur_assets_prev / cur_liab_prev if cur_liab_prev else 0
        if cr > cr_prev: f_score += 1
        
        if shares > 0 and shares_prev > 0 and shares <= shares_prev: f_score += 1

        gm = gp / rev if rev else 0
        gm_prev = gp_prev / rev_prev if rev_prev else 0
        if gm > gm_prev: f_score += 1
        
        ato = rev / ta_cur if ta_cur else 0
        ato_prev = rev_prev / ta_prev if ta_prev else 0
        if ato > ato_prev: f_score += 1

        return f_score
    except Exception:
        return "N/A"

def analyze_fundamentals(symbol, close_price=0.0):
    """
    第二阶段：财务基本面过滤 + 期权异动探测 + 新闻催化剂探测
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

            inc = ticker.income_stmt
            bs = ticker.balance_sheet
            cf = ticker.cashflow

            revenue_growth = info.get('revenueGrowth')
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
            
            f_score = calculate_piotroski_f_score(bs, cf, inc)
            if not isinstance(f_score, int) or f_score < 5: return None

            total_revenue = info.get('totalRevenue', 0)
            rd_expense = info.get('researchAndDevelopment')
            rd_ratio = (rd_expense / total_revenue) if (rd_expense and total_revenue > 0) else 0
            
            sector = info.get('sector', '')
            if sector in {'Healthcare', 'Technology'} and rd_ratio < 0.08: return None

            # ==============================
            # 调用 V6.0 终极外挂
            # ==============================
            options_flow = analyze_options_flow(ticker)
            catalyst = analyze_catalyst(ticker)

            rg_str = f"{revenue_growth:.1%}" if revenue_growth is not None else "N/A"
            gm_str = f"{gross_margin:.1%}" if gross_margin is not None else "N/A"
            f_score_str = f"{f_score}/9" if isinstance(f_score, int) else "N/A"

            reasons = (
                f"F-Score: {f_score_str} | 增速: {rg_str} | 毛利: {gm_str}\n"
                f"  └ 🎲 期权: {options_flow}\n"
                f"  └ 📰 消息: {catalyst}"
            )

            return {
                '股票代码': symbol,
                '公司名称': info.get('longName', symbol),
                '所属行业': sector,
                '市值(亿美元)': round(market_cap / 1e8, 2),
                '营收增速': rg_str,
                'F-Score': f_score_str,
                '期权异动': options_flow,
                '催化剂': catalyst,
                '最新收盘价': round(close_price, 2),
                '筛选理由': reasons,
                '链接': f"https://finance.yahoo.com/quote/{symbol}"
            }

        except Exception:
            if hasattr(thread_local, "session"):
                try: thread_local.session.close()
                except: pass
                del thread_local.session
            if attempt == 1: 
                return None

def send_notifications(df):
    """多平台推送模块 (安全截断与 Markdown 适配)"""
    if df.empty:
        print("📭 今日无符合严苛条件的标的。")
        return
        
    summary = f"🚀 GrowthHunter V6.0 (霸王龙版) 异动播报\n\n捕获 {len(df)} 只底盘扎实且量价齐升的起爆股！\n\n"
    max_len = 3500
    for _, row in df.head(10).iterrows():
        item_text = f"• [{row['股票代码']}] {row['公司名称']} ({row['市值(亿美元)']}亿)\n  └ {row['筛选理由']}\n\n"
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
                res = requests.get(f"https://sctapi.ftqq.com/{val}.send", params={"title": "🚀 V6 终极起爆预警", "desp": summary})
            elif platform == 'Telegram':
                chat_id = os.getenv('TELEGRAM_CHAT_ID')
                if not chat_id:
                    status_report.append(f"⚪ Telegram: 未配置 CHAT_ID")
                    continue
                tg_summary = re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', summary)
                res = requests.post(f"https://api.telegram.org/bot{val}/sendMessage", json={"chat_id": chat_id, "text": tg_summary, "parse_mode": "MarkdownV2"})
            else:
                payload = {"msg_type": "text", "content": {"text": summary}} if platform == '飞书' else {"msgtype": "text", "text": {"content": summary}}
                res = requests.post(val, json=payload)
                
            if res.status_code == 200: status_report.append(f"✅ {platform}: 成功")
            else: status_report.append(f"❌ {platform}: 异常 ({res.text})")
        except Exception as e: 
            status_report.append(f"❌ {platform}: 失败 ({str(e)})")

    print("\n" + "="*30 + "\n 📢 推送汇总\n" + "="*30)
    for s in status_report: print(s)
    print("="*30 + "\n")

def test_notifications():
    print("🔧 启动推送测试模式...")
    init_db() # 测试顺便初始化数据库
    mock_data = [{'股票代码': 'TEST', '公司名称': '配置测试股', '市值(亿美元)': 8.8, '营收增速': '50%', 'F-Score': '9/9', '期权异动': '🔥 看涨爆单', '催化剂': '🚀 利好 (TEST Q3 Beat...)', '筛选理由': 'V6.0 霸王龙全栈系统就绪！'}]
    df = pd.DataFrame(mock_data)
    save_signals_to_db(df)
    send_notifications(df)

def main():
    print("="*40 + "\n 🚀 GrowthHunter V6.0 (霸王龙全能版)\n" + "="*40)
    
    # 1. 初始化归档数据库
    init_db()
    
    tickers = get_small_cap_tickers()
    if not tickers: return

    # 2. 技术面筛查
    passed_tech_tickers, close_prices_dict = batch_technical_screen(tickers)
    if not passed_tech_tickers: return

    results = []
    print(f"⏳ 第二阶段：深挖 {len(passed_tech_tickers)} 只股票财务与期权消息面...")
    
    # 3. 基本面、期权、新闻多线程穿透
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(analyze_fundamentals, sym, close_prices_dict.get(sym, 0.0)): sym for sym in passed_tech_tickers}
        for f in as_completed(futures):
            res = f.result()
            if res: results.append(res)

    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values(by='市值(亿美元)')
        
        # 4. 保存为本地 csv 和 markdown 报告
        df.to_csv('growth_hunter_results.csv', index=False)
        try: md_table = df.to_markdown(index=False)
        except: md_table = df.to_string(index=False)
        with open('growth_hunter_results.md', 'w', encoding='utf-8') as f:
            f.write(f"# 🚀 GrowthHunter 严选报告\n\n**生成时间**：{datetime.now()}\n\n{md_table}")
        
        print(f"\n🎉 大功告成！捕获 {len(results)} 只硬核标的。")
        
        # 5. 【新增】写入 SQLite 数据库档案
        save_signals_to_db(df)
        
    else: print("\n📉 今日无达标标的。")
    
    # 6. 发送多平台通知
    send_notifications(df)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--test': test_notifications()
    else: main()
