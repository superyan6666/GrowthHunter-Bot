"""
🚀 GrowthHunter V10.0 - 终极量化引擎 (Alternative Data Edition)
新增能力：Reddit 另类数据捕捉 (r/wallstreetbets) + 散户狂热指数
进阶能力：全生命周期资金闭环 (动态止盈止损 + 资金复利滚动)
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
    HYPER_GROWTH_THRESHOLD = 0.80 # 极速扩张豁免线 (同比增速 > 80%)
    F_SCORE_MIN = 5
    SHORT_FLOAT_MIN = 0.20  
    SLEEP_MIN = 0.3
    SLEEP_MAX = 1.2
    CACHE_DAYS = 7             
    INSIDER_DAYS = 30       
    THREAD_WORKERS = 4      
    IO_WORKERS = 20         
    BENCHMARK_TICKER = 'IWM' 
    
    # 风控与资金管理
    PORTFOLIO_VALUE = 100000 
    RISK_PER_TRADE = 0.01    
    ATR_MULTIPLIER = 1.5     

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
]

thread_local = threading.local()
shared_io_executor = ThreadPoolExecutor(max_workers=Config.IO_WORKERS)
atexit.register(lambda: shared_io_executor.shutdown(wait=False))

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        thread_local.session.headers.update({'User-Agent': random.choice(USER_AGENTS)})
    return thread_local.session

# ==========================================
# 模块 D：大盘政权与熔断体系
# ==========================================
def check_macro_regime():
    logging.info("🌍 启动风控系统：扫描宏观大盘政权 (Macro Regime)...")
    try:
        macro_data = yf.download(["SPY", "IWM", "^VIX"], period="1y", group_by="ticker", progress=False)
        if macro_data.empty: 
            return 'GREEN', "无法获取大盘数据，默认放行"
            
        last_vix = float(macro_data['^VIX']['Close'].iloc[-1]) if '^VIX' in macro_data.columns.levels[0] else 20.0
        
        iwm_close = macro_data['IWM']['Close']
        iwm_ma20 = float(iwm_close.rolling(20).mean().iloc[-1])
        iwm_ma50 = float(iwm_close.rolling(50).mean().iloc[-1])
        last_iwm = float(iwm_close.iloc[-1])
        
        spy_close = macro_data['SPY']['Close']
        last_spy = float(spy_close.iloc[-1])
        spy_ma200 = float(spy_close.rolling(200).mean().iloc[-1])
        
        if last_vix > 25.0:
            return 'RED', f"VIX ({last_vix:.2f}) > 25，市场恐慌蔓延，触发红灯熔断"
        if last_spy < spy_ma200:
            return 'RED', f"SPY ({last_spy:.2f}) < 200MA ({spy_ma200:.2f})，熊市防御，触发红灯熔断"
            
        if pd.notna(iwm_ma50) and (iwm_ma20 < iwm_ma50 or last_iwm < iwm_ma50):
            return 'YELLOW', f"小盘股趋势走坏 (IWM 20MA < 50MA 或价格跌破 50MA)，触发黄灯 (止损收紧，仓位减半)"
        if last_vix > 20.0:
            return 'YELLOW', f"VIX ({last_vix:.2f}) > 20，市场波动加剧，触发黄灯 (止损收紧，仓位减半)"

        logging.info(f"✅ 大盘环境安全 (VIX: {last_vix:.1f}, IWM 强势, SPY > 200MA)")
        return 'GREEN', "宏观环境健康，绿灯亮起，全面进攻"
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
    
    # 真实持仓表
    c.execute('''
        CREATE TABLE IF NOT EXISTS portfolio_holdings (
            symbol TEXT PRIMARY KEY, shares INTEGER,
            cost_basis REAL, purchase_date TEXT
        )
    ''')
    
    # 组合资金汇总表，记录已实现盈亏 (Realized PnL)，实现真正的资金滚动
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

def get_remaining_cash():
    try:
        conn = sqlite3.connect('growth_hunter_signals.db')
        df_holdings = pd.read_sql_query("SELECT shares, cost_basis FROM portfolio_holdings", conn)
        df_pnl = pd.read_sql_query("SELECT realized_pnl FROM portfolio_summary WHERE id=1", conn)
        conn.close()
        
        realized_pnl = df_pnl.iloc[0]['realized_pnl'] if not df_pnl.empty else 0.0
        invested = (df_holdings['shares'] * df_holdings['cost_basis']).sum() if not df_holdings.empty else 0.0
        
        # 剩余可用资金 = 初始总净值 + 已实现盈亏 - 当前持仓占用资金
        return max(0, Config.PORTFOLIO_VALUE + realized_pnl - invested)
    except Exception:
        return Config.PORTFOLIO_VALUE

def update_portfolio(selected_rows):
    try:
        conn = sqlite3.connect('growth_hunter_signals.db')
        today = datetime.now().strftime('%Y-%m-%d')
        updated_count = 0
        for row in selected_rows:
            shares = row.get('建议股数', 0)
            if isinstance(shares, int) and shares > 0:
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
        if updated_count > 0: logging.info(f"💼 组合持仓同步：{updated_count} 只标的入账。")
    except Exception as e:
        logging.error(f"⚠️ 持仓状态更新失败: {e}")

def review_portfolio():
    """盘前持仓巡检，自动卖出止盈止损释放资金"""
    logging.info("🧐 开始执行盘前持仓体检与自动退出逻辑 (Exit Logic)...")
    try:
        conn = sqlite3.connect('growth_hunter_signals.db')
        holdings = pd.read_sql_query("SELECT * FROM portfolio_holdings", conn)
        if holdings.empty:
            conn.close()
            logging.info("📉 当前无持仓，跳过退出审查。")
            return ""

        sell_report = []
        tickers = holdings['symbol'].tolist()
        
        data = yf.download(tickers, period="1y", group_by="ticker", progress=False)
        pnl_update = 0.0
        symbols_to_remove = []

        for _, row in holdings.iterrows():
            sym = row['symbol']
            try:
                if len(tickers) == 1:
                    df = data.copy()
                else:
                    if sym not in data.columns.levels[0] and sym not in data.columns.levels[1]:
                        continue
                    df = data[sym].copy()

                df = df.dropna(subset=['Close'])
                if len(df) < 50: continue

                df.ta.supertrend(length=7, multiplier=3.0, append=True)
                st_dir_col = next((col for col in df.columns if col.startswith('SUPERTd_')), None)

                current_close = float(df['Close'].iloc[-1])
                cost_basis = float(row['cost_basis'])
                shares = int(row['shares'])

                is_downtrend = (df[st_dir_col].iloc[-1] == -1) if st_dir_col else False
                is_hard_stop = current_close < cost_basis * 0.85
                is_take_profit = current_close >= cost_basis * 2.0

                if is_downtrend or is_hard_stop or is_take_profit:
                    realized = (current_close - cost_basis) * shares
                    pnl_update += realized
                    symbols_to_remove.append(sym)

                    reason = "📈 翻倍止盈" if is_take_profit else ("📉 趋势破位" if is_downtrend else "🛑 触及硬止损")
                    ret_pct = (current_close - cost_basis) / cost_basis
                    sell_report.append(f"  └ 卖出 [{sym}] ({reason}): 成本 ${cost_basis:.2f} -> 卖出价 ${current_close:.2f} (盈亏: {ret_pct:+.1%}, 释放回笼: ${current_close * shares:.2f})")

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
            return "♻️ **【组合自动调仓 (卖出与退出)】**\n" + "\n".join(sell_report) + "\n\n"
        return ""
    except Exception as e:
        logging.error(f"持仓审查发生致命错误: {e}")
        return ""

# ==========================================
# 模块 B：异动面引擎 (内幕、期权、新闻、Reddit)
# ==========================================
def analyze_social_sentiment(symbol):
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
        score, text = shared_io_executor.submit(_fetch_reddit).result(timeout=6)
        return score, text
    except Exception: return 0, "请求超时"

def analyze_insider_trading(symbol):
    def _fetch_insider():
        local_ticker = yf.Ticker(symbol, session=get_session())
        return local_ticker.insider_transactions
    try:
        df = shared_io_executor.submit(_fetch_insider).result(timeout=5)
        if df is None or df.empty: return "无近期记录"
        try:
            cutoff = datetime.now() - pd.Timedelta(days=Config.INSIDER_DAYS)
            if isinstance(df.index, pd.DatetimeIndex): df = df[df.index.tz_localize(None) >= cutoff]
            elif 'Start Date' in df.columns: df = df[pd.to_datetime(df['Start Date']).dt.tz_localize(None) >= cutoff]
        except Exception: pass
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

def analyze_options_flow(symbol):
    def _fetch_options():
        local_ticker = yf.Ticker(symbol, session=get_session())
        opts = local_ticker.options
        if not opts: return None, "无期权"
        return local_ticker.option_chain(opts[0]), None
    try:
        chain, err_msg = shared_io_executor.submit(_fetch_options).result(timeout=8)
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

def analyze_catalyst(symbol):
    try:
        news = shared_io_executor.submit(lambda: yf.Ticker(symbol, session=get_session()).news).result(timeout=5)
        if not news: return "无最新消息"
        
        latest_title = news[0].get('title', '无标题')
        short_title = latest_title[:25] + "..." if len(latest_title) > 25 else latest_title

        api_key = os.getenv("GEMINI_API_KEY")
        if api_key and HAS_GENAI:
            for attempt in range(3):
                try:
                    client = genai.Client(api_key=api_key)
                    news_text = "\n".join([f"- {n.get('title', '')}: {n.get('summary', '')}" for n in news[:5]])
                    prompt = f"""作为资深风险投资(VC)与量化风控官，阅读股票 {symbol} 的近期新闻：
{news_text}

任务：提取关键信息，必须且只能输出一个合法的 JSON 字符串，不要任何 Markdown 标记。格式如下：
{{
  "sentiment_score": 85,
  "event_type": "财报超预期/并购重组/新产品发布/其他",
  "red_flag": "发现的具体致命风险(如增发/诉讼/退市等)，若无请严格填 null",
  "vc_10x_potential": true, 
  "summary": "限15个汉字的核心逻辑总结"
}}
注："vc_10x_potential" 布尔值，判定其是否具备“颠覆性技术/FDA突破/指数级爆发/散户高度狂热”等十倍股早期叙事特征。
"""
                    response = client.models.generate_content(
                        model='gemini-1.5-flash',
                        contents=prompt
                    )
                    res_text = response.text.strip()
                    
                    bt = '`' * 3
                    if res_text.startswith(bt):
                        res_text = re.sub(rf'^{bt}(?:json)?\n?|{bt}$', '', res_text).strip()
                    
                    data = json.loads(res_text)
                    score = data.get("sentiment_score", 50)
                    event = data.get("event_type", "其他")
                    risk = data.get("red_flag")
                    vc_10x = data.get("vc_10x_potential", False)
                    summary = data.get("summary", "")
                    
                    if risk and str(risk).lower() != 'null' and str(risk).strip() and risk != '无':
                        return f"⛔ 致命警报 (风险: {risk})"
                        
                    emoji = "🦄" if vc_10x else ("🚀" if score >= 60 else "⚠️" if score <= 40 else "⚖️")
                    vc_tag = " [10X潜力叙事]" if vc_10x else ""
                    return f"{emoji} 情绪{score} [{event}]{vc_tag} {summary}"
                except Exception as e: 
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                        continue
                    break
        
        pos_words = ['beat', 'surge', 'upgrade', 'fda', 'acquire', 'buy', 'profit', 'record', 'breakout', 'partner', 'approval']
        neg_words = ['miss', 'downgrade', 'lawsuit', 'investigate', 'offering', 'dilution', 'decline', 'warning', 'reject', 'fail', 'penalty']
        score = 0
        
        for n in news[:3]:
            title = n.get('title', '').lower()
            has_pos = any(w in title for w in pos_words)
            has_negation = any(re.search(rf'\b{neg}\b', title) for neg in ['not', 'fail', 'miss'])
            if has_pos and not has_negation: score += 1
            if any(w in title for w in neg_words): score -= 1
        
        if score > 0: return f"🚀 利好 ({short_title})"
        elif score < 0: return f"⚠️ 偏空 ({short_title})"
        return f"中性 ({short_title})"
    except Exception: return "获取失败"

# ==========================================
# 模块 A：技术面与基本面核心流水线
# ==========================================
def get_small_cap_tickers(input_file=None):
    if input_file and os.path.exists(input_file):
        try:
            df = pd.read_csv(input_file)
            if 'Symbol' in df.columns:
                tickers = df['Symbol'].dropna().astype(str).tolist()
            else:
                df = pd.read_csv(input_file, header=None)
                tickers = df.iloc[:, 0].dropna().astype(str).tolist()
            tickers = [t.strip().upper() for t in tickers if t.strip()]
            logging.info(f"📂 读取自定义股票池: {input_file} ({len(tickers)} 只)")
            return tickers
        except Exception: pass

    cache_path = 'small_cap_cache.csv'
    if os.path.exists(cache_path) and (datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_path))).days < Config.CACHE_DAYS:
        try: return pd.read_csv(cache_path)['Symbol'].tolist()
        except Exception: pass

    for url in ['https://stockanalysis.com/list/russell-2000/', 'https://www.marketbeat.com/russell-2000/']:
        try:
            response = requests.get(url, headers={'User-Agent': random.choice(USER_AGENTS)}, timeout=15)
            response.raise_for_status()
            tickers = pd.read_html(response.text)[0]['Symbol'].tolist()
            pd.DataFrame(tickers, columns=['Symbol']).to_csv(cache_path, index=False)
            logging.info(f"✅ 加载 Russell 2000 共 {len(tickers)} 只")
            return tickers
        except Exception: continue
    return ['LUNR', 'BBAI', 'SOUN', 'GCT', 'VLD', 'STEM', 'IONQ', 'JOBY', 'ACHR', 'HUT', 'CLSK', 'BITF', 'IREN', 'WOLF']

def check_unfilled_gap(df, lookback=10):
    if len(df) < lookback + 2: return False
    gap_up = (df['Low'] > df['High'].shift(1)) & (df['Open'] > df['Close'].shift(1) * 1.02)
    gap_indices = gap_up.iloc[-lookback:].index[gap_up.iloc[-lookback:]]
    for gd in gap_indices:
        gap_idx = df.index.get_loc(gd)
        if gap_idx < 1: continue
        pre_gap_high = df['High'].iloc[gap_idx - 1]
        post_gap_lows = df['Low'].iloc[gap_idx + 1:]
        if len(post_gap_lows) == 0:
            if df['Low'].iloc[-1] > pre_gap_high: return True
            continue
        if (post_gap_lows > pre_gap_high).all(): return True
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
    
    iwm_close = None
    if isinstance(data.columns, pd.MultiIndex) and Config.BENCHMARK_TICKER in data.columns.get_level_values(0):
        iwm_close = data[Config.BENCHMARK_TICKER]['Close']
    elif not isinstance(data.columns, pd.MultiIndex) and 'Close' in data.columns and Config.BENCHMARK_TICKER in download_list:
        iwm_close = data['Close']
        
    if iwm_close is not None:
        if getattr(iwm_close.index, 'tz', None) is not None: iwm_close.index = iwm_close.index.tz_localize(None)

    passed_tickers, tech_data = [], {}
    is_github_actions = os.getenv('GITHUB_ACTIONS') == 'true'
    
    for sym in tqdm(tickers, desc="🎯 技术筛选", disable=is_github_actions):
        try:
            if isinstance(data.columns, pd.MultiIndex):
                if sym not in data.columns.levels[0] and sym not in data.columns.levels[1]: continue
                df = data[sym].copy()
            else:
                if len(download_list) > 1 and sym not in data.columns: continue
                df = data.copy()
                
            df = df.dropna(subset=['Close', 'Volume'])
            if len(df) < 150: continue
            if getattr(df.index, 'tz', None) is not None: df.index = df.index.tz_localize(None)
            
            df.ta.supertrend(length=7, multiplier=3.0, append=True)
            df.ta.atr(length=20, append=True) 
            df.ta.cmf(length=20, append=True)
            
            vol_95th = df['Volume'].rolling(window=60).quantile(0.95)
            vol_ma20 = df['Volume'].rolling(window=20).mean()
            
            atr_col = next((col for col in df.columns if col.startswith('ATRr_20')), None)
            cmf_col = next((col for col in df.columns if col.startswith('CMF')), None)
            st_dir_col = next((col for col in df.columns if col.startswith('SUPERTd_')), None)
            if not all([atr_col, cmf_col, st_dir_col]): continue
            
            idx = -1
            current_close, current_vol = df['Close'].iloc[idx], df['Volume'].iloc[idx]
            
            sma20 = df['Close'].rolling(window=20).mean()
            std20 = df['Close'].rolling(window=20).std()
            
            bb_upper = sma20 + 2 * std20
            bb_lower = sma20 - 2 * std20
            kc_upper = sma20 + 1.5 * df[atr_col]
            kc_lower = sma20 - 1.5 * df[atr_col]
            
            squeeze_on = (bb_upper < kc_upper) & (bb_lower > kc_lower)
            is_squeeze_break = (current_close > kc_upper.iloc[idx]) and (squeeze_on.iloc[-2] if len(squeeze_on) >= 2 else False)
            
            aligned_iwm = iwm_close.reindex(df.index).interpolate(method='linear').ffill() if iwm_close is not None else df['Close'] * 0
            rs_line = df['Close'] / aligned_iwm
            rs_sma50 = rs_line.rolling(window=50).mean()
            rs_condition = rs_line.iloc[idx] > rs_sma50.iloc[idx] if not pd.isna(rs_sma50.iloc[idx]) else False

            has_unfilled_gap = check_unfilled_gap(df, lookback=10)
            
            is_uptrend = (df[st_dir_col].iloc[idx] == 1)
            if not is_uptrend: continue
                
            tech_score = 0
            if current_vol > max(vol_ma20.iloc[idx] * 1.5, vol_95th.iloc[idx] * 0.8): tech_score += 1
            if rs_condition: tech_score += 1
            if is_squeeze_break or has_unfilled_gap: tech_score += 1
            if df[cmf_col].iloc[idx] > 0: tech_score += 1
            
            if tech_score >= 2:
                passed_tickers.append(sym)
                tech_data[sym] = {'close': float(current_close), 'atr': float(df[atr_col].iloc[idx]), 'tech_score': tech_score}
        except Exception: continue
            
    logging.info(f"🎯 阶段一完成：技术面保留 {len(passed_tickers)} 只标的")
    return passed_tickers, tech_data

def calculate_piotroski_f_score(bs, cf, inc):
    f_score = 0
    try:
        if bs is None or cf is None or inc is None or bs.empty or cf.empty or inc.empty: return "N/A"
        def get_val(df, keys, idx=0):
            try:
                str_idx = df.index.astype(str).str.lower()
                for key in keys:
                    mask = str_idx.str.contains(key.lower(), regex=False, na=False)
                    if mask.any():
                        val = df[mask].iloc[0, idx]
                        if pd.notna(val): return float(val)
            except Exception: pass
            return 0

        ni = get_val(inc, ['Net Income'])
        ta_cur = get_val(bs, ['Total Assets'])
        cfo = get_val(cf, ['Operating Cash Flow', 'Cash Flow From Operating Activities', 'Net Cash Provided By Operating Activities'])
        lt_debt = get_val(bs, ['Long Term Debt'])
        cur_assets = get_val(bs, ['Current Assets', 'Total Current Assets'])
        cur_liab = get_val(bs, ['Current Liabilities', 'Total Current Liabilities'])
        shares = get_val(bs, ['Ordinary Shares Number', 'Basic Average Shares', 'Share Issued'])
        gp = get_val(inc, ['Gross Profit', 'Total Gross Profit'])
        rev = get_val(inc, ['Total Revenue', 'Operating Revenue', 'Revenue'])
        
        ni_prev = get_val(inc, ['Net Income'], 1)
        ta_prev = get_val(bs, ['Total Assets'], 1)
        lt_debt_prev = get_val(bs, ['Long Term Debt'], 1)
        cur_assets_prev = get_val(bs, ['Current Assets', 'Total Current Assets'], 1)
        cur_liab_prev = get_val(bs, ['Current Liabilities', 'Total Current Liabilities'], 1)
        shares_prev = get_val(bs, ['Ordinary Shares Number', 'Basic Average Shares', 'Share Issued'], 1)
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
    except Exception: return "N/A"

def analyze_fundamentals(symbol, tech_info=None, regime='GREEN'):
    if tech_info is None: tech_info = {'close': 0.0, 'atr': 0.0, 'tech_score': 0}
    close_price = tech_info.get('close', 0.0)
    atr = tech_info.get('atr', 0.0)
    
    time.sleep(random.uniform(Config.SLEEP_MIN, Config.SLEEP_MAX))
    for attempt in range(2):
        try:
            ticker = yf.Ticker(symbol, session=get_session())
            
            try:
                market_cap = float(ticker.fast_info.market_cap)
            except Exception:
                market_cap = 0
                
            if not market_cap or not (Config.MARKET_CAP_MIN < market_cap < Config.MARKET_CAP_MAX): 
                return None

            info = {}
            for _ in range(2):
                try:
                    info = ticker.info
                    if info: break
                except Exception:
                    time.sleep(random.uniform(1.0, 2.5))
                    
            if not info or 'longName' not in info: return None

            revenue_growth = info.get('revenueGrowth')
            gross_margin = info.get('grossMargins')
            
            is_hyper_growth = revenue_growth is not None and revenue_growth >= Config.HYPER_GROWTH_THRESHOLD
            req_growth = 0.0 if is_hyper_growth else Config.REVENUE_GROWTH_MIN
            req_margin = 0.05 if is_hyper_growth else 0.20
            req_f_score = 2 if is_hyper_growth else Config.F_SCORE_MIN
            
            if revenue_growth is not None and revenue_growth < req_growth: return None
            if gross_margin is not None and gross_margin < req_margin: return None

            inc, bs, cf = ticker.income_stmt, ticker.balance_sheet, ticker.cashflow
            
            if (revenue_growth is None or gross_margin is None) and not inc.empty:
                try:
                    rev_row = next((inc.loc[k] for k in ['Total Revenue', 'Operating Revenue', 'Revenue'] if k in inc.index), None)
                    gp_row = next((inc.loc[k] for k in ['Gross Profit', 'Total Gross Profit'] if k in inc.index), None)
                    
                    if revenue_growth is None and rev_row is not None and len(rev_row.dropna()) >= 2:
                        rv = rev_row.dropna()
                        try: 
                            rv.index = pd.to_datetime(rv.index)
                            rv = rv.sort_index(ascending=False)
                        except Exception: pass
                        if rv.iloc[1] > 0: revenue_growth = (rv.iloc[0] - rv.iloc[1]) / rv.iloc[1]
                    
                    if gross_margin is None and gp_row is not None and rev_row is not None:
                        gp, rv = gp_row.dropna(), rev_row.dropna()
                        if len(gp) > 0 and len(rv) > 0 and rv.iloc[0] > 0: gross_margin = gp.iloc[0] / rv.iloc[0]
                except Exception: pass

            if revenue_growth is None or revenue_growth < req_growth: return None
            if gross_margin is None or gross_margin < req_margin: return None
            
            f_score = calculate_piotroski_f_score(bs, cf, inc)
            if not isinstance(f_score, int) or f_score < req_f_score: return None

            options_flow = analyze_options_flow(symbol)
            catalyst = analyze_catalyst(symbol)
            insider_trading = analyze_insider_trading(symbol)
            reddit_score, reddit_signal = analyze_social_sentiment(symbol)
            
            if '⛔ 致命警报' in catalyst: 
                logging.info(f"🚫 触发单票熔断: [{symbol}] {catalyst}")
                return None

            short_float = info.get('shortPercentOfFloat')
            if short_float is None:
                short_int, shares_out = info.get('shortInterest'), info.get('sharesOutstanding')
                if short_int and shares_out and shares_out > 0: short_float = short_int / shares_out

            squeeze_signal = "无"
            if short_float is not None:
                if short_float >= Config.SHORT_FLOAT_MIN: squeeze_signal = f"🩸 世纪轧空 ({short_float:.1%})"
                elif short_float >= 0.10: squeeze_signal = f"⚠️ 高度做空 ({short_float:.1%})"

            comp_score = (f_score - req_f_score) if isinstance(f_score, int) else 0
            comp_score += tech_info.get('tech_score', 0)
            comp_score += reddit_score # 【V10.0】散户狂热分数计入总分
            
            if is_hyper_growth: comp_score += 3  
            if '🦄' in catalyst: comp_score += 4  
            if '🔥' in options_flow: comp_score += 2
            
            if '🚀' in catalyst: comp_score += 1
            try:
                if '情绪' in catalyst:
                    score_match = re.search(r'情绪(\d+)', catalyst)
                    if score_match and int(score_match.group(1)) >= 80: 
                        comp_score += 1
            except Exception: pass
                    
            if '🚨' in insider_trading: comp_score += 2
            if '🩸' in squeeze_signal: comp_score += 2
            elif '⚠️' in squeeze_signal: comp_score += 1

            actual_atr_mult = 1.0 if regime == 'YELLOW' else Config.ATR_MULTIPLIER
            actual_risk_pct = (Config.RISK_PER_TRADE / 2.0) if regime == 'YELLOW' else Config.RISK_PER_TRADE
            
            stop_loss = close_price - (actual_atr_mult * atr)
            risk_amount = Config.PORTFOLIO_VALUE * actual_risk_pct
            max_position_value = Config.PORTFOLIO_VALUE * (0.125 if regime == 'YELLOW' else 0.25)
            
            stop_loss_dist = close_price - stop_loss
            if atr > 0 and close_price > 0 and stop_loss_dist > 0.01:
                raw_shares = int(risk_amount / stop_loss_dist)
                if (raw_shares * close_price) > max_position_value:
                    raw_shares = int(max_position_value / close_price)
            else:
                raw_shares = 0

            return {
                '股票代码': symbol, '公司名称': info.get('longName', symbol),
                '市值(亿美元)': round(market_cap / 1e8, 2), '营收增速': f"{revenue_growth:.1%}",
                'F-Score': f"{f_score}/9", '综合得分': comp_score, '期权异动': options_flow,
                '催化剂': catalyst, '散户情绪': reddit_signal, '内幕交易': insider_trading, '轧空雷达': squeeze_signal,
                '最新收盘价': round(close_price, 2), '止损价': round(stop_loss, 2),
                '建议股数': raw_shares, 'F_Score_raw': f_score, 'gross_margin': gross_margin,
                'revenue_growth_raw': revenue_growth
            }
        except Exception:
            if hasattr(thread_local, "session"):
                try: thread_local.session.close()
                except: pass
                del thread_local.session
            if attempt == 1: return None

# ==========================================
# 自动复盘与主干逻辑
# ==========================================
def run_auto_backtest():
    try:
        if not os.path.exists('growth_hunter_signals.db'): return ""
        df_db = pd.read_sql_query("SELECT * FROM signals", sqlite3.connect('growth_hunter_signals.db'))
        if df_db.empty: return ""
        
        df_db['date'] = pd.to_datetime(df_db['date'])
        unique_dates = sorted(df_db['date'].dt.date.unique(), reverse=True)
        if len(unique_dates) <= 1: return "\n📊 【复盘战报】: 数据积累中。\n"
        
        report, backtest_tasks, tickers_to_fetch = "\n📊 【系统自动复盘战报】\n", {}, set()
        for label, n in {'T+1': 1, 'T+5': 5, 'T+20': 20}.items():
            valid_dates = [d for d in unique_dates if d <= (pd.to_datetime(unique_dates[0]) - pd.offsets.BDay(n)).date()]
            if valid_dates:
                signals = df_db[df_db['date'].dt.date == valid_dates[0]]
                if not signals.empty:
                    backtest_tasks[label] = signals
                    tickers_to_fetch.update(signals['symbol'].tolist())
                    
        if not tickers_to_fetch: return ""
        
        current_data = yf.download(list(tickers_to_fetch), period="1d", group_by="ticker", threads=True)
        current_prices = {}
        for t in tickers_to_fetch:
            try:
                s = current_data[t]['Close'].dropna() if isinstance(current_data.columns, pd.MultiIndex) else current_data['Close'].dropna()
                if not s.empty: current_prices[t] = float(s.iloc[-1])
            except: pass
        
        for label, signals in backtest_tasks.items():
            wins, total, returns = 0, 0, []
            for _, row in signals.iterrows():
                if row['symbol'] in current_prices and row['close_price'] > 0:
                    ret = (current_prices[row['symbol']] - row['close_price']) / row['close_price']
                    returns.append(ret)
                    if ret > 0: wins += 1
                    total += 1
            if total > 0:
                target_date_str = signals['date'].iloc[0].strftime('%m-%d')
                actual_days = (datetime.now().date() - pd.to_datetime(signals['date'].iloc[0]).date()).days
                report += f" • {label} ({target_date_str}推, 距今{actual_days}天, {total}只): 胜率 {wins/total:.0%} | 平均收益 {sum(returns)/total:+.1%}\n"
        return report
    except Exception as e: return f"\n⚠️ 复盘异常: {e}\n"

def send_notifications(df, sell_report="", backtest_report=""):
    if df.empty and not backtest_report and not sell_report: return logging.info("📭 今日无任何异动或复盘。")
        
    summary = f"🚀 GrowthHunter V10.0 (持仓调配全闭环)\n\n" 
    if sell_report:
        summary += sell_report
        
    summary += (f"【新增捕获】 {len(df)} 只底盘扎实且量价齐升的起爆股：\n\n" if not df.empty else "今日无新增达标买入标的。\n\n")
    for _, row in df.head(10).iterrows():
        item = f"• [{row['股票代码']}] {row['公司名称']} ({row['市值(亿美元)']}亿)\n  └ {row['筛选理由']}\n\n"
        if len(summary) + len(item) > 3500: summary += "...（已自动截断）\n\n"; break
        summary += item
    summary += backtest_report
        
    status_report = []
    for platform, env_key in [('微信', 'SERVERCHAN_KEY'), ('飞书', 'FEISHU_WEBHOOK'), ('钉钉', 'DINGTALK_WEBHOOK'), ('Telegram', 'TELEGRAM_TOKEN')]:
        val = os.getenv(env_key)
        if not val: status_report.append(f"⚪ {platform}: 未配置"); continue
        try:
            if platform == '微信': res = requests.get(f"https://sctapi.ftqq.com/{val}.send", params={"title": "🚀 V10.0 起爆预警", "desp": summary}, timeout=10)
            elif platform == 'Telegram':
                chat_id = os.getenv('TELEGRAM_CHAT_ID')
                if not chat_id: status_report.append(f"⚪ Telegram: 缺 CHAT_ID"); continue
                links = re.findall(r'\[.*?\]\(.*?\)', summary)
                temp = summary
                for i, link in enumerate(links): temp = temp.replace(link, f"@@LINKPLACEHOLDER_{i}@@")
                tg_sum = temp.replace('_', '\\_').replace('*', '\\*')
                for i, link in enumerate(links): tg_sum = tg_sum.replace(f"@@LINKPLACEHOLDER_{i}@@", link)
                res = requests.post(f"https://api.telegram.org/bot{val}/sendMessage", json={"chat_id": chat_id, "text": tg_sum, "parse_mode": "Markdown"}, timeout=10)
            else:
                res = requests.post(val, json=({"msg_type": "text", "content": {"text": summary}} if platform == '飞书' else {"msgtype": "text", "text": {"content": summary}}), timeout=10)
            status_report.append(f"✅ {platform}: 成功" if res.status_code == 200 else f"❌ {platform}: 异常 ({res.text})")
        except Exception as e: status_report.append(f"❌ {platform}: 失败 ({e})")

    logging.info("\n" + "="*30 + "\n 📢 推送汇总\n" + "="*30)
    for s in status_report: logging.info(s)
    logging.info("="*30 + "\n")

def main(dry_run=False, input_file=None):
    logging.info("="*40 + " 🚀 GrowthHunter V10.0 (全生命周期调仓版) " + "="*40)
    if dry_run: logging.info("🏃 【DRY-RUN 空跑模式启动】")
    
    init_db()
    
    # 每天扫描新股前，先对现有持仓进行审查（自动卖出、释放资金）
    sell_report = "" if dry_run else review_portfolio()
    
    regime, macro_reason = check_macro_regime()
    if regime == 'RED':
        logging.warning("🛑 宏观红灯，触发系统熔断！")
        if not dry_run:
            bt_report = run_auto_backtest()
            send_notifications(pd.DataFrame(), sell_report, f"🚨 **大盘红灯熔断警告**\n{macro_reason}\n\n🛑 **防守指令**：今日暂停买入操作，注意破位止损！\n\n{bt_report}")
        return
    elif regime == 'YELLOW':
        logging.warning(f"⚠️ 宏观黄灯警告：{macro_reason}")

    tickers = get_small_cap_tickers(input_file=input_file)
    if not tickers: return

    passed_tech_tickers, tech_data_dict = batch_technical_screen(tickers)
    
    # 【Bug 修复】：提前初始化 final_selected 以防 df.empty 导致未被定义
    final_selected = []

    if not passed_tech_tickers:
        logging.info("📉 今日无新标的通过技术面初筛。")
        df = pd.DataFrame()
    else:
        results = []
        logging.info(f"⏳ 第二阶段：深挖 {len(passed_tech_tickers)} 只新标的财务、期权、散户情绪与风投叙事...")
        with ThreadPoolExecutor(max_workers=Config.THREAD_WORKERS) as executor:
            futures = {executor.submit(analyze_fundamentals, sym, tech_data_dict.get(sym, {'close': 0.0, 'atr': 0.0, 'tech_score': 0}), regime): sym for sym in passed_tech_tickers}
            for f in as_completed(futures):
                res = f.result()
                if res: results.append(res)
        df = pd.DataFrame(results)

    if not df.empty:
        df = df.sort_values(by=['综合得分', '市值(亿美元)'], ascending=[False, True])
        
        remaining_cash = get_remaining_cash()
        logging.info(f"💵 当前组合剩余可用资金: ${remaining_cash:.2f} / 初始净值 ${Config.PORTFOLIO_VALUE:.2f}")
        
        for _, row in df.iterrows():
            shares = row['建议股数']
            price = row['最新收盘价']
            slippage_price = price * 1.015
            cost = shares * slippage_price
            
            if shares > 0 and remaining_cash >= cost:
                remaining_cash -= cost
                shares_str = f"{int(shares)} 股"
            elif shares > 0 and remaining_cash >= slippage_price:
                affordable_shares = int(remaining_cash / slippage_price)
                cost = affordable_shares * slippage_price
                remaining_cash -= cost
                shares_str = f"{affordable_shares} 股 (受总资金限制调减)"
                row['建议股数'] = affordable_shares 
            else:
                shares_str = "资金耗尽，暂不买入"
                cost = 0
                row['建议股数'] = 0 
                
            reasons = (
                f"F-Score: {row['F_Score_raw']}/9 | 增速: {row['revenue_growth_raw']:.1%} | 毛利: {row['gross_margin']:.1%}\n"
                f"  └ 🛡️ 风控: 建议买入 {shares_str} | 破位止损 ${row['止损价']:.2f} | 预估动用 ${cost:.2f}\n"
                f"  └ 🎲 另类: {row['期权异动']} | {row['散户情绪']}\n"
                f"  └ 📰 消息: {row['催化剂']}\n"
                f"  └ 🩸 筹码: {row['内幕交易']} | {row['轧空雷达']}"
            )
            row['筛选理由'] = reasons
            row['链接'] = f"https://finance.yahoo.com/quote/{row['股票代码']}"
            final_selected.append(row)
            
        df = pd.DataFrame(final_selected)
        df = df.drop(columns=['止损价', '建议股数', 'F_Score_raw', 'gross_margin', 'revenue_growth_raw'], errors='ignore')

        md_df = df.copy()
        md_df['股票代码'] = md_df['股票代码'].apply(lambda x: f"[{x}](https://finance.yahoo.com/quote/{x})")
        df.to_csv('growth_hunter_results.csv', index=False)
        with open('growth_hunter_results.md', 'w', encoding='utf-8') as f: f.write(f"# 🚀 GrowthHunter 严选报告\n\n**生成时间**：{datetime.now()}\n\n{md_df.to_markdown(index=False)}")
        logging.info(f"🎉 大功告成！捕获 {len(df)} 只硬核标的。")
    
    if not dry_run:
        save_signals_to_db(df, len(tickers), len(passed_tech_tickers))
        update_portfolio(final_selected)
        backtest_report = run_auto_backtest()
        # 将每日的卖出报告、新增买入、历史复盘合并推送给用户
        send_notifications(df, sell_report, backtest_report)

def test_notifications():
    logging.info("🔧 推送测试...")
    init_db()
    save_signals_to_db(pd.DataFrame([{'股票代码': 'TEST', '公司名称': '配置测试股', '市值(亿美元)': 8.8, '营收增速': '50%', 'F-Score': '9/9', '综合得分': 7, '期权异动': '🔥 爆单', '催化剂': '🚀 情绪95 [财报超预期] 利润翻倍指引上调', '散户情绪': '🦍 极度狂热', '内幕交易': '🚨 买入', '轧空雷达': '🩸 轧空', '最新收盘价': 100.5, '筛选理由': 'V10.0 Reddit 引擎就绪！'}]), 1, 1)
    send_notifications(pd.DataFrame(), "♻️ **【组合自动调仓】**\n  └ 卖出 [TEST] (📈 翻倍止盈): 成本 $50 -> 卖出价 $100\n\n", "\n📊 【战报】\n • T+1 (推 3只): 胜率 67% | 均收益 +4.2%\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--input', type=str, default=None)
    args, _ = parser.parse_known_args()
    
    test_notifications() if args.test else main(dry_run=args.dry_run, input_file=args.input)
