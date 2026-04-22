"""
🚀 GrowthHunter V10.0 - 终极量化引擎 (Alternative Data Edition)
新增能力：Reddit 另类数据捕捉 (r/wallstreetbets) + 散户狂热指数
进阶能力：全生命周期资金闭环 (动态止盈止损 + 资金复利滚动)
行情适配：维基百科稳定股票池 + 极强动能免检特权
深度打磨：引入 Rule of 40、合并情绪因子降噪、动态ATR、消除回测前视偏差
极致强化：严苛版营收加速度 (QoQ) + 毛利连续扩张 + 机构流向 + IBD式3重RS
精准点火：强制要求 🎯枢轴突破 / 🌊缩量反弹 / 🚀强势缺口 入场触发器
阶梯捕获：【新增观察池逻辑】，防止因条件过严导致信号窒息，兼顾深度与广度
"""

import yfinance as yf
import pandas as pd
import pandas_ta as ta
from datetime import datetime
import os
import requests
import time
import random
import threading
import re
import sqlite3
import logging
import atexit
import argparse
import json
from io import StringIO
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from google import genai
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

try:
    import praw
    HAS_PRAW = True
except ImportError:
    HAS_PRAW = False

import warnings
warnings.filterwarnings('ignore')

# ==========================================
# 核心配置参数中心
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class Config:
    MARKET_CAP_MIN = 5e7
    MARKET_CAP_MAX = 3e9       
    REVENUE_GROWTH_MIN = 0.20
    HYPER_GROWTH_THRESHOLD = 0.80 
    F_SCORE_MIN = 5
    SHORT_FLOAT_MIN = 0.20  
    SLEEP_MIN = 0.3
    SLEEP_MAX = 1.2
    CACHE_DAYS = 7             
    INSIDER_DAYS = 30       
    THREAD_WORKERS = 4      
    IO_WORKERS = 20         
    BENCHMARK_TICKER = 'IWM' 
    
    # 风控与组合资金管理
    PORTFOLIO_VALUE = 100000 
    RISK_PER_TRADE = 0.01    
    ATR_MULTIPLIER = 1.5     
    MAX_SECTOR_WEIGHT = 0.30 

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
]

thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    thread_local.session.headers.update({'User-Agent': random.choice(USER_AGENTS)})
    return thread_local.session

# ==========================================
# 模块 D：大盘政权与熔断体系 (多维宏观流矩阵)
# ==========================================
def check_macro_regime():
    logging.info("🌍 启动风控系统：扫描多维宏观与流动性矩阵 (Macro Regime)...")
    try:
        macro_tickers = ["SPY", "IWM", "^VIX", "HYG", "LQD", "UUP", "XLK", "XLE"]
        macro_data = yf.download(macro_tickers, period="1y", group_by="ticker", progress=False)
        
        if macro_data.empty: 
            return 'GREEN', "无法获取大盘数据，默认放行"
            
        def get_close(ticker, default_val=None):
            try:
                if isinstance(macro_data.columns, pd.MultiIndex):
                    if ticker in macro_data.columns.levels[0]:
                        return macro_data[ticker]['Close'].ffill()
                elif 'Close' in macro_data.columns and ticker in macro_tickers:
                    return macro_data['Close'].ffill()
            except Exception: pass
            return pd.Series([default_val]*200) if default_val else None

        vix = get_close('^VIX', 20.0)
        spy = get_close('SPY')
        iwm = get_close('IWM')
        hyg = get_close('HYG')
        lqd = get_close('LQD')
        uup = get_close('UUP')
        xlk = get_close('XLK')
        xle = get_close('XLE')
        
        last_vix = float(vix.iloc[-1])
        
        if spy is not None:
            spy_ma200 = float(spy.rolling(200).mean().iloc[-1])
            last_spy = float(spy.iloc[-1])
            if last_vix > 25.0:
                return 'RED', f"VIX ({last_vix:.2f}) > 25，市场恐慌蔓延，触发红灯熔断"
            if last_spy < spy_ma200:
                return 'RED', f"SPY ({last_spy:.2f}) < 200MA ({spy_ma200:.2f})，熊市防御，触发红灯熔断"
                
        yellow_reasons = []
        
        if iwm is not None:
            iwm_ma20 = float(iwm.rolling(20).mean().iloc[-1])
            iwm_ma50 = float(iwm.rolling(50).mean().iloc[-1])
            last_iwm = float(iwm.iloc[-1])
            if pd.notna(iwm_ma50) and (iwm_ma20 < iwm_ma50 or last_iwm < iwm_ma50):
                yellow_reasons.append("小盘股趋势走弱(IWM破位)")

        if last_vix > 20.0:
            yellow_reasons.append(f"波动率升温(VIX>20)")

        if hyg is not None and lqd is not None:
            cred_spread = hyg / lqd
            cred_ma50 = float(cred_spread.rolling(50).mean().iloc[-1])
            if cred_spread.iloc[-1] < cred_ma50:
                yellow_reasons.append("高收益债利差走阔(资金避险)")

        if uup is not None:
            uup_ma50 = float(uup.rolling(50).mean().iloc[-1])
            if uup.iloc[-1] > uup_ma50 and uup.iloc[-1] > uup.iloc[-20]:
                yellow_reasons.append("美元指数走强(杀估值)")

        if xlk is not None and xle is not None:
            growth_value = xlk / xle
            gv_ma50 = float(growth_value.rolling(50).mean().iloc[-1])
            if growth_value.iloc[-1] < gv_ma50:
                yellow_reasons.append("科技成长跑输能源价值(风格逆风)")

        if yellow_reasons:
            reason_str = " | ".join(yellow_reasons)
            return 'YELLOW', f"宏观逆风警告: {reason_str} -> 触发黄灯 (止损收紧，仓位减半)"

        logging.info(f"✅ 多维大盘环境安全 (VIX: {last_vix:.1f}, 流动性充裕, 成长占优)")
        return 'GREEN', "宏观与流动性环境健康，绿灯亮起，全面进攻"
    except Exception as e:
        logging.debug(f"大盘校验异常: {e}")
        return 'GREEN', "大盘校验异常，默认放行"

# ==========================================
# 模块 C：SQLite 数据库与持仓管理
# ==========================================
def init_db():
    conn = sqlite3.connect('growth_hunter_signals.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS signals (
            date TEXT, symbol TEXT, name TEXT, market_cap REAL,
            revenue_growth TEXT, f_score TEXT, options_flow TEXT,
            catalyst TEXT, close_price REAL, UNIQUE(date, symbol)
        )
    ''')
    for col in ['insider_trading', 'short_squeeze', 'reddit_sentiment']:
        try: c.execute(f'ALTER TABLE signals ADD COLUMN {col} TEXT')
        except sqlite3.OperationalError: pass
            
    c.execute('''
        CREATE TABLE IF NOT EXISTS daily_stats (
            date TEXT PRIMARY KEY, tickers_scanned INTEGER,
            passed_tech INTEGER, final_selected INTEGER, avg_score REAL
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_date ON signals (date)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON signals (symbol)')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS portfolio_holdings (
            symbol TEXT PRIMARY KEY, shares INTEGER,
            cost_basis REAL, purchase_date TEXT
        )
    ''')
    try: c.execute('ALTER TABLE portfolio_holdings ADD COLUMN sector TEXT')
    except sqlite3.OperationalError: pass
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS portfolio_summary (
            id INTEGER PRIMARY KEY,
            realized_pnl REAL
        )
    ''')
    c.execute('INSERT OR IGNORE INTO portfolio_summary (id, realized_pnl) VALUES (1, 0.0)')
    
    conn.commit()
    conn.close()

def save_signals_to_db(df, tickers_scanned_count, passed_tech_count):
    try:
        conn = sqlite3.connect('growth_hunter_signals.db')
        today = datetime.now().strftime('%Y-%m-%d')
        
        if not df.empty:
            for _, row in df.iterrows():
                conn.execute('''
                    INSERT INTO signals 
                    (date, symbol, name, market_cap, revenue_growth, f_score, options_flow, catalyst, close_price, insider_trading, short_squeeze, reddit_sentiment)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(date, symbol) DO UPDATE SET
                        market_cap=excluded.market_cap, options_flow=excluded.options_flow,
                        catalyst=excluded.catalyst, close_price=excluded.close_price,
                        insider_trading=excluded.insider_trading, short_squeeze=excluded.short_squeeze,
                        reddit_sentiment=excluded.reddit_sentiment
                ''', (today, row['股票代码'], row['公司名称'], row['市值(亿美元)'], 
                      row['营收增速'], row['F-Score'], row['期权异动'], row['催化剂'], 
                      row.get('最新收盘价', 0.0), row.get('内幕交易', '未知'), row.get('轧空雷达', '未知'), row.get('散户情绪', '冷门')))
                  
        avg_score = round(df['综合得分'].mean(), 2) if not df.empty else 0.0
        conn.execute('''
            INSERT INTO daily_stats (date, tickers_scanned, passed_tech, final_selected, avg_score)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                tickers_scanned=excluded.tickers_scanned, passed_tech=excluded.passed_tech,
                final_selected=excluded.final_selected, avg_score=excluded.avg_score
        ''', (today, tickers_scanned_count, passed_tech_count, len(df), avg_score))
        
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"⚠️ 数据库保存失败: {e}")

def get_portfolio_state():
    try:
        conn = sqlite3.connect('growth_hunter_signals.db')
        df_holdings = pd.read_sql_query("SELECT symbol, shares, cost_basis, sector FROM portfolio_holdings", conn)
        df_pnl = pd.read_sql_query("SELECT realized_pnl FROM portfolio_summary WHERE id=1", conn)
        conn.close()
        
        realized_pnl = df_pnl.iloc[0]['realized_pnl'] if not df_pnl.empty else 0.0
        holdings_dict = {}
        sector_exposure = {}
        invested = 0.0
        
        if not df_holdings.empty:
            for _, row in df_holdings.iterrows():
                val = row['shares'] * row['cost_basis']
                invested += val
                sec = row['sector'] if row['sector'] else 'Unknown'
                holdings_dict[row['symbol']] = {'shares': row['shares'], 'cost_basis': row['cost_basis'], 'sector': sec}
                sector_exposure[sec] = sector_exposure.get(sec, 0.0) + val
                
        remaining_cash = max(0, Config.PORTFOLIO_VALUE + realized_pnl - invested)
        return remaining_cash, holdings_dict, sector_exposure
    except Exception:
        return Config.PORTFOLIO_VALUE, {}, {}

def update_portfolio(selected_rows):
    try:
        conn = sqlite3.connect('growth_hunter_signals.db')
        today = datetime.now().strftime('%Y-%m-%d')
        updated_count = 0
        for row in selected_rows:
            shares = row.get('建议股数', 0)
            if isinstance(shares, int) and shares > 0:
                try:
                    conn.execute('''
                        INSERT INTO portfolio_holdings (symbol, shares, cost_basis, purchase_date, sector)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(symbol) DO UPDATE SET
                            cost_basis = ((cost_basis * shares) + (excluded.cost_basis * excluded.shares)) / (shares + excluded.shares),
                            shares = shares + excluded.shares,
                            purchase_date = excluded.purchase_date,
                            sector = excluded.sector
                    ''', (row['股票代码'], shares, row['最新收盘价'], today, row.get('行业', 'Unknown')))
                except sqlite3.OperationalError:
                    conn.execute('''
                        INSERT INTO portfolio_holdings (symbol, shares, cost_basis, purchase_date)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(symbol) DO UPDATE SET
                            cost_basis = ((cost_basis * shares) + (excluded.cost_basis * excluded.shares)) / (shares + excluded.shares),
                            shares = shares + excluded.shares,
                            purchase_date = excluded.purchase_date
                    ''', (row['股票代码'], shares, row['最新收盘价'], today))
                updated_count += 1
        conn.commit()
        conn.close()
        if updated_count > 0: logging.info(f"💼 组合持仓同步：{updated_count} 笔订单入账。")
    except Exception as e:
        logging.error(f"⚠️ 持仓状态更新失败: {e}")

def review_portfolio():
    logging.info("🧐 开始执行盘前持仓体检与动态退出逻辑 (Dynamic Exit Logic)...")
    try:
        conn = sqlite3.connect('growth_hunter_signals.db')
        holdings = pd.read_sql_query("SELECT * FROM portfolio_holdings", conn)
        if holdings.empty:
            conn.close()
            logging.info("📉 当前无持仓，跳过退出审查。")
            return ""

        sell_report = []
        tickers = holdings['symbol'].tolist()
        dl_list = tickers + [Config.BENCHMARK_TICKER]
        
        data = yf.download(dl_list, period="1y", group_by="ticker", progress=False)
        
        iwm_close = None
        if isinstance(data.columns, pd.MultiIndex) and Config.BENCHMARK_TICKER in data.columns.levels[0]:
            iwm_close = data[Config.BENCHMARK_TICKER]['Close']
        elif not isinstance(data.columns, pd.MultiIndex) and 'Close' in data.columns and Config.BENCHMARK_TICKER in dl_list:
            iwm_close = data['Close']
        if iwm_close is not None and getattr(iwm_close.index, 'tz', None) is not None:
            iwm_close.index = iwm_close.index.tz_localize(None)

        pnl_update = 0.0
        symbols_to_remove = []

        for _, row in holdings.iterrows():
            sym = row['symbol']
            try:
                if isinstance(data.columns, pd.MultiIndex):
                    if sym not in data.columns.levels[0]: continue
                    df = data[sym].copy()
                else:
                    df = data.copy()

                df = df.dropna(subset=['Close'])
                if len(df) < 20: continue
                if getattr(df.index, 'tz', None) is not None: df.index = df.index.tz_localize(None)

                df.ta.supertrend(length=7, multiplier=3.0, append=True)
                st_dir_col = next((col for col in df.columns if col.startswith('SUPERTd_')), None)

                current_close = float(df['Close'].iloc[-1])
                cost_basis = float(row['cost_basis'])
                shares = int(row['shares'])
                
                purchase_date_str = row.get('purchase_date', '')
                try:
                    p_date = pd.to_datetime(purchase_date_str).tz_localize(None)
                    df_post = df[df.index >= p_date]
                    max_high = float(df_post['High'].max()) if not df_post.empty else current_close
                    trading_days_held = len(df_post)
                except Exception:
                    max_high = current_close
                    trading_days_held = 0

                is_downtrend = (df[st_dir_col].iloc[-1] == -1) if st_dir_col else False
                is_hard_stop = current_close < cost_basis * 0.85
                is_trailing_stop = current_close <= max_high * 0.80
                is_time_stop = trading_days_held >= 60 and current_close < cost_basis * 1.10
                
                is_rs_deteriorated = False
                if iwm_close is not None:
                    aligned_iwm = iwm_close.reindex(df.index).interpolate(method='linear').ffill()
                    rs_line = df['Close'] / aligned_iwm
                    rs_sma50 = rs_line.rolling(window=50).mean()
                    if pd.notna(rs_sma50.iloc[-1]) and rs_line.iloc[-1] < rs_sma50.iloc[-1]:
                        if current_close < cost_basis * 1.20:
                            is_rs_deteriorated = True

                exit_reason = ""
                if is_hard_stop: exit_reason = "🛑 触及硬止损 (-15%)"
                elif is_trailing_stop: exit_reason = f"🏆 追踪止盈 (高点回撤20%, 巅峰${max_high:.2f})"
                elif is_rs_deteriorated: exit_reason = "⚠️ RS恶化 (跑输大盘，提前清仓)"
                elif is_downtrend: exit_reason = "📉 趋势破位 (SuperTrend翻红)"
                elif is_time_stop: exit_reason = f"⏳ 时间止损 (耗时{trading_days_held}天无表现)"

                if exit_reason:
                    realized = (current_close - cost_basis) * shares
                    pnl_update += realized
                    symbols_to_remove.append(sym)
                    ret_pct = (current_close - cost_basis) / cost_basis
                    sell_report.append(f"  └ 卖出 [{sym}] ({exit_reason}): 成本 ${cost_basis:.2f} -> 卖价 ${current_close:.2f} (盈亏: {ret_pct:+.1%}, 回笼: ${current_close * shares:.2f})")

            except Exception as e:
                logging.debug(f"持仓审查异常 {sym}: {e}")

        if symbols_to_remove:
            c = conn.cursor()
            c.execute('UPDATE portfolio_summary SET realized_pnl = realized_pnl + ? WHERE id = 1', (pnl_update,))
            c.executemany('DELETE FROM portfolio_holdings WHERE symbol = ?', [(sym,) for sym in symbols_to_remove])
            conn.commit()

        conn.close()

        if sell_report:
            logging.info(f"♻️ 完成清仓退出，释放资金，总盈亏更新: ${pnl_update:+.2f}")
            return "♻️ **【组合动态调仓与止盈防线】**\n" + "\n".join(sell_report) + "\n\n"
        return ""
    except Exception as e:
        logging.error(f"持仓审查发生致命错误: {e}")
        return ""

# ==========================================
# 模块 B：异动面引擎 (内幕、期权、新闻、Reddit)
# ==========================================
def analyze_social_sentiment(symbol, io_executor):
    def _fetch_reddit():
        if not HAS_PRAW: return 0, "未装配 PRAW 库"
        client_id = os.getenv("REDDIT_CLIENT_ID")
        client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        if not client_id or not client_secret: return 0, "未配置 API 密钥"
        
        try:
            reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent="GrowthHunter_V10/1.0"
            )
            subs = reddit.subreddit("wallstreetbets+stocks+pennystocks")
            mentions = 0
            for submission in subs.search(f"{symbol}", sort="new", time_filter="week", limit=30):
                text = f"{submission.title} {submission.selftext}".upper()
                if re.search(rf'\b{symbol}\b', text):
                    mentions += 1
                    
            if mentions >= 10: return 3, f"🦍 极度狂热 ({mentions}+ 贴)"
            elif mentions >= 3: return 1, f"👀 散户异动 ({mentions} 贴)"
            return 0, "论坛冷门"
        except Exception as e:
            return 0, "解析报错"

    try:
        score, text = io_executor.submit(_fetch_reddit).result(timeout=6)
        return score, text
    except Exception: return 0, "请求超时"

def analyze_insider_trading(symbol, io_executor):
    def _fetch_insider():
        local_ticker = yf.Ticker(symbol, session=get_session())
        return local_ticker.insider_transactions
    try:
        df = io_executor.submit(_fetch_insider).result(timeout=5)
        if df is None or df.empty: return "无近期记录"
        cutoff = datetime.now() - pd.Timedelta(days=Config.INSIDER_DAYS)
        if isinstance(df.index, pd.DatetimeIndex): df = df[df.index.tz_localize(None) >= cutoff]
        elif 'Start Date' in df.columns: df = df[pd.to_datetime(df['Start Date']).dt.tz_localize(None) >= cutoff]
        if df.empty: return "无近期记录"
        
        buy_kw = ['Buy', 'Purchase', 'P - Purchase']
        sell_kw = ['Sale', 'Sell', 'S - Sale', 'Disposition']
        text_col = next((col for col in df.columns if 'transaction' in str(col).lower() or 'text' in str(col).lower() or 'action' in str(col).lower()), None)
        if not text_col: return "格式不支持"
            
        ts = df[text_col].astype(str)
        is_buy = ts.str.contains('|'.join(buy_kw), case=False, na=False)
        is_sell = ts.str.contains('|'.join(sell_kw), case=False, na=False)
        buys = df[is_buy & ~is_sell]
        return f"🚨 高管净买入 ({len(buys)}笔)" if len(buys) > 0 else "无高管买入"
    except Exception: return "获取失败"

def analyze_options_flow(symbol, io_executor):
    def _fetch_options():
        local_ticker = yf.Ticker(symbol, session=get_session())
        opts = local_ticker.options
        if not opts: return None, "无期权"
        return local_ticker.option_chain(opts[0]), None
    try:
        chain, err_msg = io_executor.submit(_fetch_options).result(timeout=8)
        if err_msg or chain is None: return err_msg or "获取失败"
        if chain.calls.empty or chain.puts.empty: return "数据不全"
        
        call_vol, put_vol = chain.calls['volume'].fillna(0).sum(), chain.puts['volume'].fillna(0).sum()
        if put_vol == 0 and call_vol == 0: return "交投清淡"
        if call_vol == 0: return "⚠️ 无看涨成交 (极空)"
        
        pcr = put_vol / call_vol
        unusual_calls = chain.calls[(chain.calls['volume'] > 100) & (chain.calls['openInterest'] > 0) & (chain.calls['volume'] > chain.calls['openInterest'] * 1.5)]
        if not unusual_calls.empty and pcr < 0.7: return f"🔥 看涨爆单 (PCR: {pcr:.2f})"
        elif pcr > 1.5: return f"⚠️ 偏空防御 (PCR: {pcr:.2f})"
        else: return f"中性 (PCR: {pcr:.2f})"
    except Exception: return "获取失败"

def analyze_catalyst(symbol, io_executor):
    try:
        news = io_executor.submit(lambda: yf.Ticker(symbol, session=get_session()).news).result(timeout=5)
        if not news: return "无最新消息"
        latest_title = news[0].get('title', '无标题')
        short_title = latest_title[:25] + "..." if len(latest_title) > 25 else latest_title

        api_key = os.getenv("GEMINI_API_KEY")
        if api_key and HAS_GENAI:
            for attempt in range(3):
                try:
                    client = genai.Client(api_key=api_key)
                    news_text = "\n".join([f"- {n.get('title', '')}: {n.get('summary', '')}" for n in news[:5]])
                    prompt = f"""作为资深风险投资(VC)与量化风控官，阅读股票 {symbol} 的近期新闻：{news_text}
任务：必须只能输出一个合法的 JSON 字符串，不要 Markdown。格式：
{{ "sentiment_score": 85, "event_type": "...", "red_flag": "...", "vc_10x_potential": true, "summary": "..." }}"""
                    response = client.models.generate_content(model='gemini-1.5-flash', contents=prompt)
                    res_text = response.text.strip()
                    bt = '`' * 3
                    if res_text.startswith(bt): res_text = re.sub(rf'^{bt}(?:json)?\n?|{bt}$', '', res_text).strip()
                    data = json.loads(res_text)
                    score, event, risk, vc_10x, summary = data.get("sentiment_score", 50), data.get("event_type", "其他"), data.get("red_flag"), data.get("vc_10x_potential", False), data.get("summary", "")
                    if risk and str(risk).lower() != 'null' and str(risk).strip() and risk != '无': return f"⛔ 致命警报 (风险: {risk})"
                    emoji = "🦄" if vc_10x else ("🚀" if score >= 60 else "⚠️" if score <= 40 else "⚖️")
                    vc_tag = " [10X潜力叙事]" if vc_10x else ""
                    return f"{emoji} 情绪{score} [{event}]{vc_tag} {summary}"
                except Exception:
                    if attempt < 2: time.sleep(2); continue
                    break
        return f"中性 ({short_title})"
    except Exception: return "获取失败"

# ==========================================
# 模块 A：技术面与基本面核心流水线
# ==========================================
def get_small_cap_tickers(input_file=None):
    if input_file and os.path.exists(input_file):
        try:
            df = pd.read_csv(input_file)
            tickers = df['Symbol'].dropna().astype(str).tolist() if 'Symbol' in df.columns else pd.read_csv(input_file, header=None).iloc[:, 0].dropna().astype(str).tolist()
            return [t.strip().upper() for t in tickers if t.strip()]
        except Exception: pass
    cache_path = 'small_cap_cache.csv'
    if os.path.exists(cache_path) and (datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_path))).days < Config.CACHE_DAYS:
        try: return pd.read_csv(cache_path)['Symbol'].tolist()
        except Exception: pass
    tickers = set()
    def fetch_wiki(url):
        try:
            r = requests.get(url, headers={'User-Agent': random.choice(USER_AGENTS)}, timeout=15)
            return pd.read_html(StringIO(r.text))[0]['Symbol'].tolist()
        except Exception: return []
    with ThreadPoolExecutor(max_workers=3) as executor:
        for f in as_completed([executor.submit(fetch_wiki, u) for u in ['https://en.wikipedia.org/wiki/List_of_S%26P_600_companies', 'https://en.wikipedia.org/wiki/List_of_S%26P_400_companies', 'https://stockanalysis.com/list/russell-2000/']]):
            tickers.update(f.result())
    tickers = list(tickers)
    if tickers:
        tickers = list(set([str(t).replace('.', '-') for t in tickers]))
        pd.DataFrame(tickers, columns=['Symbol']).to_csv(cache_path, index=False)
        return tickers
    return ['LUNR', 'BBAI', 'SOUN', 'GCT', 'VLD', 'STEM', 'IONQ', 'JOBY', 'ACHR', 'HUT', 'CLSK', 'BITF', 'IREN', 'WOLF', 'PLTR', 'HOOD', 'RDDT', 'RIVN', 'ASTS', 'LCID', 'MSTR']

def check_unfilled_gap(df, lookback=5):
    if len(df) < lookback + 2: return False
    gap_up = (df['Low'] > df['High'].shift(1)) & (df['Open'] > df['Close'].shift(1) * 1.02)
    gap_indices = gap_up.iloc[-lookback:].index[gap_up.iloc[-lookback:]]
    for gd in gap_indices:
        gap_idx = df.index.get_loc(gd)
        if (df['Low'].iloc[gap_idx + 1:] > df['High'].iloc[gap_idx - 1]).all(): return True
    return False

def batch_technical_screen(tickers):
    logging.info(f"⏳ 批量下载 {len(tickers)} 只股票及大盘基准...")
    download_list = tickers + [Config.BENCHMARK_TICKER]
    data = None
    for _ in range(3):
        data = yf.download(download_list, period="1y", group_by="ticker", threads=True)
        if data is not None and not data.empty: break
        time.sleep(3)
    if data is None or data.empty: return [], {}
    iwm_close = data[Config.BENCHMARK_TICKER]['Close'].ffill() if Config.BENCHMARK_TICKER in data.columns.levels[0] else None
    passed_tickers, tech_data = [], {}
    for sym in tqdm(tickers, desc="🎯 技术筛选", disable=(os.getenv('GITHUB_ACTIONS') == 'true')):
        try:
            df = data[sym].copy().dropna(subset=['Close', 'Volume'])
            if len(df) < 150: continue
            df.ta.supertrend(length=7, multiplier=3.0, append=True)
            df.ta.atr(length=20, append=True)
            df.ta.cmf(length=20, append=True)
            vol_ma20 = df['Volume'].rolling(window=20).mean()
            atr_col, cmf_col, st_dir_col = next(c for c in df.columns if c.startswith('ATRr_20')), next(c for c in df.columns if c.startswith('CMF')), next(c for c in df.columns if c.startswith('SUPERTd_'))
            idx, current_close, current_vol = -1, df['Close'].iloc[-1], df['Volume'].iloc[-1]
            if df[st_dir_col].iloc[-1] != 1: continue

            active_triggers = []
            if current_close > df['High'].shift(1).rolling(window=25).max().iloc[-1]: active_triggers.append("🎯枢轴突破")
            if df['Volume'].iloc[-4:-1].mean() < vol_ma20.iloc[-1] * 0.8 and current_vol > vol_ma20.iloc[-1] * 1.5 and current_close > df['Close'].rolling(20).mean().iloc[-1]: active_triggers.append("🌊缩量反弹")
            if check_unfilled_gap(df, 5): active_triggers.append("🚀强势缺口")

            # IBD式 3重相对强度
            rs_line = df['Close'] / iwm_close.reindex(df.index).ffill()
            rs_sma50, rs_sma65, rs_sma130 = rs_line.rolling(50).mean(), rs_line.rolling(65).mean(), rs_line.rolling(130).mean()
            rs_pass = rs_line.iloc[-1] > rs_sma50.iloc[-1] and rs_line.iloc[-1] > rs_sma65.iloc[-1] and rs_line.iloc[-1] > rs_sma130.iloc[-1]

            tech_score = (1 if current_vol > vol_ma20.iloc[-1] * 1.5 else 0) + (1 if rs_pass else 0) + (1 if df[cmf_col].iloc[-1] > 0 else 0)
            
            # 【阶梯捕获】：哪怕今天没点火，只要技术分满分且大趋势极强，也放入待点火名单
            is_elite_trend = tech_score >= 3 and rs_pass
            
            if active_triggers or is_elite_trend:
                passed_tickers.append(sym)
                tech_data[sym] = {'close': float(current_close), 'atr': float(df[atr_col].iloc[-1]), 'tech_score': tech_score, 'triggers': " | ".join(active_triggers) if active_triggers else "⏳ 趋势待点火", 'is_warmup': not bool(active_triggers)}
        except Exception: continue
    return passed_tickers, tech_data

def analyze_fundamentals(symbol, tech_info, regime, io_executor, portfolio_state):
    close_price, atr, entry_trigger, is_warmup = tech_info.get('close', 0.0), tech_info.get('atr', 0.0), tech_info.get('triggers', ''), tech_info.get('is_warmup', False)
    time.sleep(random.uniform(Config.SLEEP_MIN, Config.SLEEP_MAX))
    for attempt in range(2):
        try:
            ticker = yf.Ticker(symbol, session=get_session())
            mcap = float(ticker.fast_info.market_cap)
            if not (Config.MARKET_CAP_MIN < mcap < Config.MARKET_CAP_MAX): return None
            info = ticker.info
            sector, revenue_growth, gross_margin = info.get('sector', 'Unknown'), info.get('revenueGrowth'), info.get('grossMargins')
            inst_held = (info.get('heldPercentInstitutions') or 0.0) + (info.get('heldPercentInsiders') or 0.0)
            op_margin = info.get('operatingMargins') or ((gross_margin - 0.20) if gross_margin else 0.0)
            rule_of_40 = ((revenue_growth or 0) + op_margin) * 100
            is_hyper, is_r40, tech_score = (revenue_growth and revenue_growth >= Config.HYPER_GROWTH_THRESHOLD), (rule_of_40 >= 40.0), tech_info.get('tech_score', 0)
            
            # 动能豁免底线
            req_growth = 0.0 if (is_hyper or is_r40 or tech_score >= 3) else Config.REVENUE_GROWTH_MIN
            if revenue_growth is None or revenue_growth < req_growth: return None

            # 抓取季度加速度
            q_inc = ticker.quarterly_income_stmt
            rev_accel, margin_exp = False, False
            if q_inc is not None and not q_inc.empty:
                try:
                    revs = q_inc.loc[['Total Revenue', 'Operating Revenue', 'Revenue']].dropna().iloc[0].values
                    if len(revs) >= 3 and (revs[0]-revs[1])/revs[1] > (revs[1]-revs[2])/revs[2]: rev_accel = True
                except Exception: pass

            f_score = calculate_piotroski_f_score(ticker.balance_sheet, ticker.cashflow, ticker.income_stmt)
            if not (is_hyper or is_r40 or tech_score >= 3) and (not isinstance(f_score, int) or f_score < Config.F_SCORE_MIN): return None

            options_flow, catalyst, insider, reddit_score, reddit_signal = analyze_options_flow(symbol, io_executor), analyze_catalyst(symbol, io_executor), analyze_insider_trading(symbol, io_executor), *analyze_social_sentiment(symbol, io_executor)
            if '⛔' in catalyst: return None

            comp_score = tech_score + (2 if is_r40 else 0) + (2 if rev_accel else 0) + (1 if inst_held > 0.20 else 0) + max(reddit_score, (2 if '🔥' in options_flow else 0))
            
            # 仓位计算 (针对 warmup 标的不计算建议股数)
            shares_to_buy, action_tag, stop_loss = 0, "🔭 观察等待点火", close_price - (Config.ATR_MULTIPLIER * atr)
            if not is_warmup:
                target_raw = int((Config.PORTFOLIO_VALUE * Config.RISK_PER_TRADE) / (close_price - stop_loss))
                shares_to_buy = int(target_raw * (0.5 if symbol not in portfolio_state['holdings'] else 0.3))
                action_tag = "🆕建仓(首发50%)" if symbol not in portfolio_state['holdings'] else "🔼加仓确认"
                # 行业集中度拦截
                if portfolio_state['sector_exposure'].get(sector, 0.0) >= Config.PORTFOLIO_VALUE * Config.MAX_SECTOR_WEIGHT: return None

            return {
                '股票代码': symbol, '公司名称': info.get('longName', symbol), '行业': sector, '市值(亿)': round(mcap/1e8, 2),
                '综合得分': comp_score, '最新收盘价': round(close_price, 2), '建议股数': shares_to_buy, '交易动作': action_tag,
                '买入触发': entry_trigger, '核心质地': f"R40: {rule_of_40:.1f}% | 加速: {rev_accel} | 机构: {inst_held:.1%}",
                '筛选理由': f"得分:{comp_score} | {reddit_signal} | {options_flow} | {catalyst}", 'is_warmup': is_warmup
            }
        except Exception:
            if attempt == 1: return None

# ==========================================
# 自动复盘与推送系统
# ==========================================
def run_auto_backtest():
    try:
        conn = sqlite3.connect('growth_hunter_signals.db')
        df_db = pd.read_sql_query("SELECT * FROM signals", conn)
        if df_db.empty: return ""
        df_db['date'] = pd.to_datetime(df_db['date'])
        unique_dates = sorted(df_db['date'].dt.date.unique(), reverse=True)
        if len(unique_dates) <= 1: return "\n📊 【复盘战报】: 数据积累中。\n"
        
        report, tickers_to_fetch = "\n📊 【系统真实回测战报 (排除前视偏差)】\n", set(df_db['symbol'].tolist())
        current_data = yf.download(list(tickers_to_fetch), period="1mo", group_by="ticker", progress=False)
        
        for label, n in {'T+1': 1, 'T+5': 5, 'T+20': 20}.items():
            valid_dates = [d for d in unique_dates if d <= (pd.to_datetime(unique_dates[0]) - pd.offsets.BDay(n)).date()]
            if valid_dates:
                signals = df_db[df_db['date'].dt.date == valid_dates[0]]
                wins, total, rets = 0, 0, []
                for _, row in signals.iterrows():
                    try:
                        df_post = current_data[row['symbol']][pd.to_datetime(current_data[row['symbol']].index).tz_localize(None) >= row['date']]
                        if df_post.empty: continue
                        ret = -0.15 if df_post['Low'].min() <= row['close_price']*0.85 else (df_post['Close'].iloc[-1]-row['close_price'])/row['close_price']
                        rets.append(ret); total += 1
                        if ret > 0: wins += 1
                    except: pass
                if total > 0: report += f" • {label} ({valid_dates[0].strftime('%m-%d')}, {total}只): 胜率 {wins/total:.0%} | 收益 {sum(rets)/total:+.1%}\n"
        return report
    except Exception: return ""

def send_notifications(df, sell_report="", backtest_report=""):
    if df.empty and not backtest_report and not sell_report: return
    summary = f"🚀 [AI] GrowthHunter V10.0 (阶梯捕获闭环)\n\n" 
    if sell_report: summary += sell_report
    
    # 拆分 精选买入 vs 潜力观察
    buy_df = df[df['is_warmup'] == False]
    watch_df = df[df['is_warmup'] == True]
    
    if not buy_df.empty:
        summary += "🔥 **【精选买入指令】**\n"
        for _, row in buy_df.head(5).iterrows():
            summary += f"• [{row['股票代码']}] {row['公司名称']}\n  └ 触发: {row['买入触发']} | 动作: {row['交易动作']} {row['建议股数']}股\n  └ 理由: {row['核心质地']}\n\n"
            
    if not watch_df.empty:
        summary += "🔭 **【潜力观察名单 (等待点火)】**\n"
        for _, row in watch_df.head(5).iterrows():
            summary += f"• [{row['股票代码']}] RS极强且质地优异，待点火信号\n"
            
    summary += backtest_report
    for platform, env_key in [('微信', 'SERVERCHAN_KEY'), ('飞书', 'FEISHU_WEBHOOK'), ('钉钉', 'DINGTALK_WEBHOOK'), ('Telegram', 'TELEGRAM_TOKEN')]:
        val = os.getenv(env_key)
        if not val: continue
        try:
            if platform == '微信': requests.get(f"https://sctapi.ftqq.com/{val}.send", params={"title": "🚀 [AI] 起爆预警", "desp": summary}, timeout=10)
            elif platform == 'Telegram': requests.post(f"https://api.telegram.org/bot{val}/sendMessage", json={"chat_id": os.getenv('TELEGRAM_CHAT_ID'), "text": summary.replace('_', '\\_'), "parse_mode": "Markdown"}, timeout=10)
            else: requests.post(val, json=({"msg_type": "text", "content": {"text": summary}} if platform == '飞书' else {"msgtype": "text", "text": {"content": summary}}), timeout=10)
        except: pass

# ==========================================
# 系统主干
# ==========================================
def main(dry_run=False, input_file=None):
    logging.info("="*40 + " 🚀 GrowthHunter V10.0 (阶梯捕获版) " + "="*40)
    init_db()
    sell_report = "" if dry_run else review_portfolio()
    regime, macro_reason = check_macro_regime()
    if regime == 'RED':
        if not dry_run: send_notifications(pd.DataFrame(), sell_report, f"🚨 **大盘红灯熔断**\n{macro_reason}\n\n{run_auto_backtest()}")
        return

    tickers = get_small_cap_tickers(input_file=input_file)
    passed_tech_tickers, tech_data_dict = batch_technical_screen(tickers)
    
    if not passed_tech_tickers:
        df = pd.DataFrame()
    else:
        results = []
        rem_cash, cur_h, sec_e = get_portfolio_state()
        portfolio_state = {'holdings': cur_h, 'sector_exposure': sec_e}
        with ThreadPoolExecutor(max_workers=Config.IO_WORKERS) as io_exec:
            with ThreadPoolExecutor(max_workers=Config.THREAD_WORKERS) as executor:
                futures = {executor.submit(analyze_fundamentals, sym, tech_data_dict.get(sym), regime, io_exec, portfolio_state): sym for sym in passed_tech_tickers}
                for f in as_completed(futures):
                    res = f.result()
                    if res: results.append(res)
        df = pd.DataFrame(results)

    if not df.empty:
        df = df.sort_values(by=['is_warmup', '综合得分'], ascending=[True, False])
        md_df = df.copy()
        md_df['股票代码'] = md_df['股票代码'].apply(lambda x: f"[{x}](https://finance.yahoo.com/quote/{x})")
        df.to_csv('growth_hunter_results.csv', index=False)
        with open('growth_hunter_results.md', 'w', encoding='utf-8') as f: f.write(f"# 🚀 GrowthHunter 阶梯研报\n\n{md_df.to_markdown(index=False)}")
        if not dry_run:
            save_signals_to_db(df[df['is_warmup']==False], len(tickers), len(passed_tech_tickers))
            update_portfolio(df[df['is_warmup']==False])
            send_notifications(df, sell_report, run_auto_backtest())
    else:
        logging.info("📭 今日无任何新增或观察标的。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--input', type=str, default=None)
    args, _ = parser.parse_known_args()
    main(dry_run=args.dry_run, input_file=args.input)
