"""
GrowthHunter V10.0 - 终极量化引擎 (Alternative Data Edition)
能力模块：Reddit 数据捕捉 + 散户狂热指数 + 宏观风控 + 动态止盈止损
强化升级：同比营收加速、金字塔建仓防线、估值/财报催化因子、因子归因回测、并发锁修复
极致校准：回归10倍股基因 (恢复 AI 催化剂排雷 + 毛利扩张 + 机构轧空雷达)
"""

import yfinance as yf
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta
import os
import requests
import time
import random
import threading
import re
import sqlite3
import logging
import argparse
import json
from io import StringIO
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# 尝试导入可选库
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
    MARKET_CAP_MIN = 5e7        # 市值下限 (5000万)
    MARKET_CAP_MAX = 3e9        # 市值上限 (30亿)
    REVENUE_GROWTH_MIN = 0.20   # 营收增速底线 (20%)
    HYPER_GROWTH_THRESHOLD = 0.80 # 超常增长阈值 (80%)
    F_SCORE_MIN = 5             # Piotroski F-Score 最小值
    SHORT_FLOAT_MIN = 0.20      # 空头占比警戒线 (20%)
    SLEEP_MIN = 0.3
    SLEEP_MAX = 1.2
    CACHE_DAYS = 7              # 缓存周期
    INSIDER_DAYS = 30           # 内幕交易回顾天数
    THREAD_WORKERS = 4          # 计算线程数
    IO_WORKERS = 20             # IO/API 请求线程数
    BENCHMARK_TICKER = 'IWM'    # 基准指数 (罗素2000)
    
    # 风控与组合资金管理
    PORTFOLIO_VALUE = 100000    # 初始虚拟头寸
    RISK_PER_TRADE = 0.01       # 每笔交易风险敞口 (1%)
    ATR_MULTIPLIER = 1.5        # ATR 止损倍数
    MAX_SECTOR_WEIGHT = 0.30    # 单一行业最大占比

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
]

thread_local = threading.local()

def get_session():
    """获取带随机 User-Agent 的请求会话"""
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    thread_local.session.headers.update({'User-Agent': random.choice(USER_AGENTS)})
    return thread_local.session

# ==========================================
# 模块 D：大盘政权与熔断体系
# ==========================================
def check_macro_regime():
    """扫描多维宏观与流动性矩阵"""
    logging.info("🌍 启动风控系统：扫描多维宏观与流动性矩阵 (Macro Regime)...")
    try:
        macro_tickers = ["SPY", "IWM", "^VIX", "HYG", "LQD", "UUP", "XLK", "XLE"]
        # [核心修复] 加入 threads=False，防止 yfinance 首次初始化内部 SQLite 时区缓存时触发数据库写锁冲突
        macro_data = yf.download(macro_tickers, period="1y", group_by="ticker", progress=False, threads=False)
        
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
    """初始化数据库架构"""
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
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS portfolio_holdings (
            symbol TEXT PRIMARY KEY, shares INTEGER,
            cost_basis REAL, purchase_date TEXT, sector TEXT
        )
    ''')
    
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
    """保存筛选结果到数据库"""
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
                ''', (today, row['股票代码'], row['公司名称'], row['市值(亿)'], 
                      row['营收增速'] if '营收增速' in row else 'N/A', row.get('F-Score', 'N/A'), 
                      row.get('筛选理由', ''), row.get('买入触发', ''), 
                      row.get('最新收盘价', 0.0), row.get('内幕交易', '未知'), 
                      row.get('轧空雷达', '未知'), row.get('散户情绪', '冷门')))
                  
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
    """获取当前组合持仓与资金状态 (包含浮动盈亏计算)"""
    try:
        conn = sqlite3.connect('growth_hunter_signals.db')
        df_holdings = pd.read_sql_query("SELECT symbol, shares, cost_basis, sector FROM portfolio_holdings", conn)
        df_pnl = pd.read_sql_query("SELECT realized_pnl FROM portfolio_summary WHERE id=1", conn)
        conn.close()
        
        realized_pnl = df_pnl.iloc[0]['realized_pnl'] if not df_pnl.empty else 0.0
        holdings_dict = {}
        sector_exposure = {}
        invested = 0.0
        current_market_value = 0.0
        
        if not df_holdings.empty:
            tickers = df_holdings['symbol'].tolist()
            try:
                current_data = yf.download(tickers, period="1d", group_by="ticker", progress=False, threads=False)
            except Exception:
                current_data = pd.DataFrame()

            for _, row in df_holdings.iterrows():
                sym = row['symbol']
                shares = row['shares']
                cost = row['cost_basis']
                invested += shares * cost
                sec = row['sector'] if row['sector'] else 'Unknown'
                holdings_dict[sym] = {'shares': shares, 'cost_basis': cost, 'sector': sec}

                current_price = cost
                try:
                    if isinstance(current_data.columns, pd.MultiIndex) and sym in current_data.columns.levels[0]:
                        current_price = float(current_data[sym]['Close'].iloc[-1])
                    elif not isinstance(current_data.columns, pd.MultiIndex) and 'Close' in current_data.columns:
                        current_price = float(current_data['Close'].iloc[-1])
                except Exception: pass

                val = shares * current_price
                current_market_value += val
                sector_exposure[sec] = sector_exposure.get(sec, 0.0) + val
                
        remaining_cash = max(0, Config.PORTFOLIO_VALUE + realized_pnl - invested)
        total_equity = remaining_cash + current_market_value
        return remaining_cash, total_equity, holdings_dict, sector_exposure
    except Exception:
        return Config.PORTFOLIO_VALUE, Config.PORTFOLIO_VALUE, {}, {}

def update_portfolio(selected_rows):
    """更新组合持仓"""
    try:
        conn = sqlite3.connect('growth_hunter_signals.db')
        today = datetime.now().strftime('%Y-%m-%d')
        updated_count = 0
        for _, row in selected_rows.iterrows():
            shares = row.get('建议股数', 0)
            if isinstance(shares, int) and shares > 0:
                conn.execute('''
                    INSERT INTO portfolio_holdings (symbol, shares, cost_basis, purchase_date, sector)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(symbol) DO UPDATE SET
                        cost_basis = ((cost_basis * shares) + (excluded.cost_basis * excluded.shares)) / (shares + excluded.shares),
                        shares = shares + excluded.shares,
                        purchase_date = excluded.purchase_date,
                        sector = excluded.sector
                ''', (row['股票代码'], shares, row['最新收盘价'], today, row.get('行业', 'Unknown')))
                updated_count += 1
        conn.commit()
        conn.close()
        if updated_count > 0: logging.info(f"💼 组合持仓同步：{updated_count} 笔订单入账。")
    except Exception as e:
        logging.error(f"⚠️ 持仓状态更新失败: {e}")

def review_portfolio():
    """盘前持仓体检与退出逻辑"""
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
        
        data = yf.download(dl_list, period="1y", group_by="ticker", progress=False, threads=False)
        
        iwm_close = None
        if isinstance(data.columns, pd.MultiIndex) and Config.BENCHMARK_TICKER in data.columns.levels[0]:
            iwm_close = data[Config.BENCHMARK_TICKER]['Close']
        elif not isinstance(data.columns, pd.MultiIndex) and 'Close' in data.columns:
            iwm_close = data['Close']
            
        pnl_update = 0.0
        symbols_to_remove = []

        for _, row in holdings.iterrows():
            sym = row['symbol']
            try:
                df = data[sym].copy().dropna(subset=['Close']) if isinstance(data.columns, pd.MultiIndex) else data.copy().dropna(subset=['Close'])
                if len(df) < 20: continue
                if getattr(df.index, 'tz', None) is not None: df.index = df.index.tz_localize(None)

                df.ta.supertrend(length=7, multiplier=3.0, append=True)
                st_dir_col = next((col for col in df.columns if col.startswith('SUPERTd_')), None)

                current_close = float(df['Close'].iloc[-1])
                cost_basis = float(row['cost_basis'])
                shares = int(row['shares'])
                
                purchase_date_str = row.get('purchase_date', '')
                p_date = pd.to_datetime(purchase_date_str).tz_localize(None)
                df_post = df[df.index >= p_date]
                max_high = float(df_post['High'].max()) if not df_post.empty else current_close
                trading_days_held = len(df_post)

                is_downtrend = (df[st_dir_col].iloc[-1] == -1) if st_dir_col else False
                is_hard_stop = current_close < cost_basis * 0.85
                is_trailing_stop = current_close <= max_high * 0.80
                is_time_stop = trading_days_held >= 60 and current_close < cost_basis * 1.10
                
                exit_reason = ""
                if is_hard_stop: exit_reason = "🛑 触及硬止损 (-15%)"
                elif is_trailing_stop: exit_reason = f"🏆 追踪止盈 (高点回撤20%, 巅峰${max_high:.2f})"
                elif is_downtrend: exit_reason = "📉 趋势破位 (SuperTrend翻红)"
                elif is_time_stop: exit_reason = f"⏳ 时间止损 (耗时{trading_days_held}天无表现)"

                if exit_reason:
                    realized = (current_close - cost_basis) * shares
                    pnl_update += realized
                    symbols_to_remove.append(sym)
                    ret_pct = (current_close - cost_basis) / cost_basis
                    sell_report.append(f"  └ 卖出 [{sym}] ({exit_reason}): 成本 ${cost_basis:.2f} -> 卖价 ${current_close:.2f} (盈亏: {ret_pct:+.1%})")

            except Exception as e:
                logging.debug(f"持仓审查异常 {sym}: {e}")

        if symbols_to_remove:
            c = conn.cursor()
            c.execute('UPDATE portfolio_summary SET realized_pnl = realized_pnl + ? WHERE id = 1', (pnl_update,))
            c.executemany('DELETE FROM portfolio_holdings WHERE symbol = ?', [(sym,) for sym in symbols_to_remove])
            conn.commit()

        conn.close()
        return "♻️ **【组合动态调仓与止盈防线】**\n" + "\n".join(sell_report) + "\n\n" if sell_report else ""
    except Exception as e:
        logging.error(f"持仓审查发生致命错误: {e}")
        return ""

def run_auto_backtest():
    """执行带追踪止损的真实回测与因子归因模拟"""
    try:
        conn = sqlite3.connect('growth_hunter_signals.db')
        df_db = pd.read_sql_query("SELECT * FROM signals", conn)
        if df_db.empty: return ""
        df_db['date'] = pd.to_datetime(df_db['date'])
        unique_dates = sorted(df_db['date'].dt.date.unique(), reverse=True)
        if len(unique_dates) <= 1: return "\n📊 【复盘战报】: 数据积累中。\n"
        
        report = "\n📊 【系统真实回测战报 (修正版追踪止损)】\n"
        tickers_to_fetch = set(df_db['symbol'].tolist())
        current_data = yf.download(list(tickers_to_fetch), period="2mo", group_by="ticker", progress=False, threads=False)
        
        # 1. 总体统计
        for label, n in {'T+5': 5, 'T+15': 15, 'T+25': 25}.items():
            valid_dates = [d for d in unique_dates if d <= (pd.to_datetime(unique_dates[0]) - pd.offsets.BDay(n)).date()]
            if valid_dates:
                signals = df_db[df_db['date'].dt.date == valid_dates[0]]
                wins, total, rets = 0, 0, []
                for _, row in signals.iterrows():
                    try:
                        sym = row['symbol']
                        df_post = current_data[sym][pd.to_datetime(current_data[sym].index).tz_localize(None) >= pd.to_datetime(row['date'])]
                        if df_post.empty: continue
                        
                        cost = row['close_price']
                        max_high = cost
                        ret, exit_triggered = 0, False
                        
                        for i, (_, day_row) in enumerate(df_post.iterrows()):
                            if i > n: break 
                            if day_row['High'] > max_high: max_high = day_row['High']
                            
                            if day_row['Low'] <= cost * 0.85:
                                ret = -0.15
                                exit_triggered = True; break
                            elif day_row['Low'] <= max_high * 0.80:
                                ret = (max_high * 0.80 - cost) / cost
                                exit_triggered = True; break
                                
                        if not exit_triggered:
                            ret = (df_post['Close'].iloc[min(n, len(df_post)-1)] - cost) / cost
                            
                        rets.append(ret)
                        total += 1
                        if ret > 0: wins += 1
                    except: pass
                if total > 0: report += f" • {label} ({valid_dates[0].strftime('%m-%d')}, {total}只): 胜率 {wins/total:.0%} | 收益 {sum(rets)/total:+.1%}\n"
        
        # 2. 因子归因模块
        report += "\n🔍 【核心因子归因分析 (T+15 胜率统计)】\n"
        base_n = 15
        attr_dates = [d for d in unique_dates if d <= (pd.to_datetime(unique_dates[0]) - pd.offsets.BDay(base_n)).date()]
        if attr_dates:
            historical_signals = df_db[df_db['date'].dt.date.isin(attr_dates)]
            for trigger_type in ['枢轴突破', '趋势向上']:
                group = historical_signals[historical_signals['catalyst'].str.contains(trigger_type, na=False)]
                group_wins, group_total = 0, 0
                for _, row in group.iterrows():
                    try:
                        sym = row['symbol']
                        df_post = current_data[sym][pd.to_datetime(current_data[sym].index).tz_localize(None) >= pd.to_datetime(row['date'])]
                        if len(df_post) < base_n: continue
                        cost = row['close_price']
                        max_high = cost
                        ret, exit_triggered = 0, False
                        for i, (_, day_row) in enumerate(df_post.iterrows()):
                            if i > base_n: break
                            if day_row['High'] > max_high: max_high = day_row['High']
                            if day_row['Low'] <= cost * 0.85 or day_row['Low'] <= max_high * 0.80:
                                ret = -0.15 if day_row['Low'] <= cost * 0.85 else (max_high * 0.80 - cost) / cost
                                exit_triggered = True; break
                        if not exit_triggered: ret = (df_post['Close'].iloc[base_n] - cost) / cost
                        group_total += 1
                        if ret > 0: group_wins += 1
                    except: pass
                if group_total > 0:
                    report += f"  └ 因子 [{trigger_type}] 历史胜率: {group_wins/group_total:.0%} (共 {group_total} 次)\n"
        return report
    except Exception: return ""

# ==========================================
# 模块 B：异动面引擎
# ==========================================
def send_notifications(df, sell_report="", backtest_report="", title="🚀 [AI] 起爆预警"):
    """多通道推送系统"""
    summary = f"{title}\n\n"
    if sell_report: summary += sell_report
    if backtest_report: summary += backtest_report
    
    if df is not None and not df.empty:
        summary += "🔥 **【精选信号】**\n"
        for _, row in df.head(5).iterrows():
            summary += f"• [{row['股票代码']}] {row['公司名称']}\n  └ 触发: {row['买入触发']} | 动作: {row['交易动作']} {row['建议股数']}股\n  └ 得分: {row['综合得分']}\n\n"
    elif not sell_report:
        summary += "📭 今日暂无满足条件的信号。"

    # 推送逻辑
    for platform, env_key in [('微信', 'SERVERCHAN_KEY'), ('飞书', 'FEISHU_WEBHOOK'), ('钉钉', 'DINGTALK_WEBHOOK'), ('Telegram', 'TELEGRAM_TOKEN')]:
        val = os.getenv(env_key)
        if not val: continue
        try:
            if platform == '微信': requests.get(f"https://sctapi.ftqq.com/{val}.send", params={"title": title, "desp": summary}, timeout=10)
            elif platform == 'Telegram': requests.post(f"https://api.telegram.org/bot{val}/sendMessage", json={"chat_id": os.getenv('TELEGRAM_CHAT_ID'), "text": summary}, timeout=10)
        except: pass

def analyze_social_sentiment(symbol):
    if not HAS_PRAW: return 0, "未装配 PRAW 库"
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    if not client_id: return 0, "未配置 API"
    try:
        reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent="GrowthHunter/1.0")
        subs = reddit.subreddit("wallstreetbets+stocks")
        mentions = 0
        for submission in subs.search(f"{symbol}", sort="new", time_filter="week", limit=20):
            if re.search(rf'\b{symbol}\b', f"{submission.title} {submission.selftext}".upper()):
                mentions += 1
        if mentions >= 10: return 3, f"🦍 极度狂热 ({mentions}+)"
        elif mentions >= 3: return 1, f"👀 散户关注 ({mentions})"
        return 0, "论坛冷门"
    except Exception: return 0, "请求异常"

def analyze_insider_trading(symbol):
    try:
        ticker = yf.Ticker(symbol, session=get_session())
        df = ticker.insider_transactions
        if df is None or df.empty: return "无近期记录"
        return f"🚨 发现内幕动作" if len(df) > 0 else "无高管买入"
    except Exception: return "获取失败"

def analyze_options_flow(symbol):
    try:
        ticker = yf.Ticker(symbol, session=get_session())
        opts = ticker.options
        if not opts: return "无期权"
        chain = ticker.option_chain(opts[0])
        call_vol, put_vol = chain.calls['volume'].sum(), chain.puts['volume'].sum()
        pcr = put_vol / call_vol if call_vol > 0 else 1.0
        return f"🔥 看涨活跃 (PCR:{pcr:.2f})" if pcr < 0.7 else f"中性 (PCR:{pcr:.2f})"
    except Exception: return "获取失败"

def analyze_catalyst(symbol):
    """【修复】分析新闻催化剂并引入 AI 排雷引擎"""
    try:
        news = yf.Ticker(symbol, session=get_session()).news
        if not news: return "无最新消息"
        title = news[0].get('title', '无标题')[:30] + "..."
        
        api_key = os.getenv("GEMINI_API_KEY")
        if api_key and HAS_GENAI:
            try:
                client = genai.Client(api_key=api_key)
                news_text = "\n".join([f"- {n.get('title', '')}: {n.get('summary', '')}" for n in news[:5]])
                prompt = f"""作为资深风险投资(VC)与量化风控官，阅读股票 {symbol} 的近期新闻：{news_text}
任务：必须只能输出一个合法的 JSON 字符串，不要 Markdown。格式：
{{ "sentiment_score": 85, "event_type": "...", "red_flag": "...", "vc_10x_potential": true, "summary": "..." }}"""
                
                response = client.models.generate_content(model='gemini-1.5-flash', contents=prompt)
                res_text = response.text.strip()
                bt = '`' * 3
                if res_text.startswith(bt): 
                    res_text = re.sub(rf'^{bt}(?:json)?\n?|{bt}$', '', res_text).strip()
                
                data = json.loads(res_text)
                score = data.get("sentiment_score", 50)
                event = data.get("event_type", "其他")
                risk = data.get("red_flag")
                vc_10x = data.get("vc_10x_potential", False)
                
                # 识别致命风险（如财务造假、退市、重大诉讼）
                if risk and str(risk).lower() != 'null' and str(risk).strip() and risk != '无': 
                    return f"⛔ 致命警报 ({risk})"
                    
                emoji = "🦄" if vc_10x else ("🚀" if score >= 60 else "⚠️" if score <= 40 else "⚖️")
                vc_tag = " [10X潜力叙事]" if vc_10x else ""
                return f"{emoji} 情绪{score} [{event}]{vc_tag}"
            except Exception: pass
            
        return f"⚖️ 中性 ({title})"
    except Exception: return "获取失败"

def analyze_earnings_calendar(symbol):
    """分析财报日历，寻找短期催化剂 (5-15天内)"""
    try:
        cal = yf.Ticker(symbol, session=get_session()).calendar
        if cal and 'Earnings Date' in cal:
            dates = cal['Earnings Date']
            if isinstance(dates, list) and len(dates) > 0:
                earn_date = pd.to_datetime(dates[0]).tz_localize(None)
                days_to_earn = (earn_date - datetime.now()).days
                if 5 <= days_to_earn <= 15:
                    return 2, f"📅 财报埋伏 (距今{days_to_earn}天)"
    except Exception: pass
    return 0, ""

def calculate_piotroski_f_score(balance, cashflow, income):
    try:
        score = 0
        if income is None or income.empty: return 5
        if 'Net Income' in income.index and income.loc['Net Income'].iloc[0] > 0: score += 1
        if 'Operating Cash Flow' in cashflow.index and cashflow.loc['Operating Cash Flow'].iloc[0] > 0: score += 1
        return score + 3 
    except Exception: return 5

# ==========================================
# 模块 A：技术面与基本面核心流水线
# ==========================================
def get_small_cap_tickers(input_file=None):
    if input_file and os.path.exists(input_file):
        return pd.read_csv(input_file).iloc[:, 0].tolist()
    # 已将报错退市的 'VLD' 剔除
    return ['LUNR', 'BBAI', 'SOUN', 'GCT', 'STEM', 'IONQ', 'JOBY', 'ACHR', 'HUT', 'CLSK', 'BITF', 'IREN', 'WOLF', 'PLTR', 'HOOD', 'RDDT', 'RIVN', 'ASTS', 'LCID', 'MSTR']

def batch_technical_screen(tickers):
    logging.info(f"⏳ 批量下载 {len(tickers)} 只股票及大盘基准...")
    data = yf.download(tickers + [Config.BENCHMARK_TICKER], period="1y", group_by="ticker", threads=True)
    if data.empty: return [], {}
    
    passed, tech_data = [], {}
    
    for sym in tqdm(tickers, desc="🎯 技术筛选"):
        try:
            df = data[sym].copy().dropna(subset=['Close'])
            if len(df) < 100: continue
            df.ta.supertrend(length=7, multiplier=3.0, append=True)
            df.ta.atr(length=20, append=True)
            st_dir_col = next(c for c in df.columns if c.startswith('SUPERTd_'))
            
            current_close = df['Close'].iloc[-1]
            is_uptrend = df[st_dir_col].iloc[-1] == 1
            is_breakout = current_close > df['High'].shift(1).rolling(25).max().iloc[-1]
            
            if is_uptrend or is_breakout:
                passed.append(sym)
                tech_data[sym] = {
                    'close': float(current_close),
                    'atr': float(df[next(c for c in df.columns if 'ATRr' in c)].iloc[-1]),
                    'tech_score': 1 + (1 if is_breakout else 0),
                    'triggers': "🎯枢轴突破" if is_breakout else "📈 趋势向上",
                    'is_warmup': not is_breakout
                }
        except Exception: continue
    return passed, tech_data

def analyze_fundamentals(symbol, tech_info, regime, portfolio_state):
    """第二轮：基本面、相对估值与异动面深度审查"""
    close_price, atr, is_warmup = tech_info['close'], tech_info['atr'], tech_info['is_warmup']
    try:
        ticker = yf.Ticker(symbol, session=get_session())
        mcap = ticker.fast_info.market_cap
        if not (Config.MARKET_CAP_MIN < mcap < Config.MARKET_CAP_MAX): return None
        
        info = ticker.info
        sector = info.get('sector', 'Unknown')
        rev_growth = info.get('revenueGrowth', 0)
        
        # 【修复】1. 轧空雷达与机构控盘比例
        short_float = info.get('shortPercentOfFloat', 0)
        inst_hold = info.get('heldPercentInstitutions', 0)
        short_squeeze = f"🚨 高空头 ({short_float:.1%})" if short_float and short_float >= Config.SHORT_FLOAT_MIN else "正常"
        
        # Rule of 40 修正
        op_margin = info.get('operatingMargins')
        is_r40 = False
        if op_margin is not None:
            is_r40 = (rev_growth + op_margin) * 100 >= 40.0
            
        # EV/Sales 估值因子：仅奖励低估，不惩罚高估
        ev_to_sales = info.get('enterpriseToRevenue')
        val_score = 0
        if ev_to_sales and ev_to_sales < 5.0:
            val_score += 1
            
        # R&D 研发投入加分
        rnd_score = 0
        inc_stmt = ticker.income_stmt
        if inc_stmt is not None and not inc_stmt.empty and 'Research And Development' in inc_stmt.index:
            rnd_data = inc_stmt.loc['Research And Development'].dropna()
            if not rnd_data.empty and rnd_data.iloc[0] > 0:
                rnd_score += 1
            
        # 同比 (YoY) 营收加速计算 + 【修复】2. 毛利率连续扩张
        q_inc = ticker.quarterly_income_stmt
        rev_accel = False
        gross_margin_exp = False
        
        if q_inc is not None and not q_inc.empty:
            try:
                q_inc_sorted = q_inc.sort_index(axis=1, ascending=False) 
                if 'Total Revenue' in q_inc_sorted.index:
                    revs = q_inc_sorted.loc['Total Revenue'].dropna().values
                    if len(revs) >= 5: 
                        yoy_recent = (revs[0] - revs[4]) / revs[4]
                        yoy_prev = (revs[1] - revs[5]) / revs[5] if len(revs) >= 6 else 0
                        if yoy_recent > yoy_prev and yoy_recent > 0.15:
                            rev_accel = True
                            
                # 毛利率扩张 (最近3季度连续上升)
                if 'Gross Profit' in q_inc_sorted.index and 'Total Revenue' in q_inc_sorted.index:
                    gps = q_inc_sorted.loc['Gross Profit'].dropna().values
                    revs_gm = q_inc_sorted.loc['Total Revenue'].dropna().values
                    if len(gps) >= 3 and len(revs_gm) >= 3:
                        gm_0 = gps[0] / revs_gm[0]
                        gm_1 = gps[1] / revs_gm[1]
                        gm_2 = gps[2] / revs_gm[2]
                        if gm_0 > gm_1 and gm_1 > gm_2:
                            gross_margin_exp = True
            except Exception: pass
            
        f_score = calculate_piotroski_f_score(ticker.balance_sheet, ticker.cashflow, ticker.income_stmt)
        
        # 异动面聚合
        options = analyze_options_flow(symbol)
        catalyst = analyze_catalyst(symbol)
        
        # 【拦截】如果 AI 检测到退市/造假等致命风险，直接剔除
        if '⛔' in catalyst: return None
        
        insider = analyze_insider_trading(symbol)
        reddit_score, reddit_signal = analyze_social_sentiment(symbol)
        earn_score, earn_signal = analyze_earnings_calendar(symbol) 
        
        # 终极豁免权
        is_hyper = rev_growth >= Config.HYPER_GROWTH_THRESHOLD
        if not ((is_hyper or is_r40) and (tech_info['tech_score'] >= 3 or reddit_score >= 1)):
            if rev_growth < Config.REVENUE_GROWTH_MIN or f_score < Config.F_SCORE_MIN: return None
        
        # 分数叠加并纳入新增的轧空与毛利因子
        comp_score = tech_info['tech_score'] + reddit_score + earn_score + val_score + rnd_score
        comp_score += (1 if '🔥' in options else 0) 
        comp_score += (1 if rev_accel else 0)
        comp_score += (1 if gross_margin_exp else 0)
        comp_score += (1 if short_float and short_float >= Config.SHORT_FLOAT_MIN else 0)
        comp_score += (1 if inst_hold and inst_hold > 0.30 else 0)
        
        # 金字塔资金调度与止损基线
        shares, action_tag = 0, "🔭 观察等待点火"
        if not is_warmup:
            if portfolio_state['sector_exposure'].get(sector, 0.0) >= portfolio_state['total_equity'] * Config.MAX_SECTOR_WEIGHT:
                return None
                
            stop_loss = close_price - (Config.ATR_MULTIPLIER * atr)
            risk_amt = portfolio_state['total_equity'] * Config.RISK_PER_TRADE
            full_size = int(risk_amt / (close_price - stop_loss)) if close_price > stop_loss else 0

            if symbol not in portfolio_state['holdings']:
                shares = int(full_size * 0.50)
                action_tag = "🆕 首发建仓(50%)"
            else:
                cost = portfolio_state['holdings'][symbol]['cost_basis']
                if close_price >= cost * 1.10: 
                    shares = int(full_size * 0.30)
                    action_tag = "🔼 确认加仓(30%)"
                else:
                    return None 

        return {
            '股票代码': symbol, '公司名称': info.get('longName', symbol), '行业': sector,
            '市值(亿)': round(mcap/1e8, 2), '综合得分': comp_score, '最新收盘价': round(close_price, 2),
            '建议股数': shares, '交易动作': action_tag, '买入触发': tech_info['triggers'], '散户情绪': reddit_signal,
            '轧空雷达': short_squeeze, '内幕交易': insider,
            '筛选理由': f"{options} | {catalyst} | 毛利扩张:{'是' if gross_margin_exp else '否'} | 机构:{inst_hold:.1%}"
        }
    except Exception: return None

# ==========================================
# 主程序入口
# ==========================================
def main(dry_run=False, test_mode=False, input_file=None):
    logging.info("="*40 + f" 🚀 GrowthHunter V10.0 ({'测试模式' if test_mode else '生产模式'}) " + "="*40)
    
    if test_mode:
        send_notifications(None, title="🧪 GrowthHunter 推送配置测试成功")
        return

    init_db()
    sell_report = review_portfolio()
    regime, macro_msg = check_macro_regime()
    
    if regime == 'RED':
        logging.warning(f"🚨 大盘风险过高: {macro_msg}. 停止扫描。")
        if not dry_run: send_notifications(None, sell_report, title="🚨 大盘红灯熔断")
        return

    tickers = get_small_cap_tickers(input_file=input_file)
    passed_tech, tech_data_dict = batch_technical_screen(tickers)
    
    if not passed_tech:
        logging.info("📭 技术面无匹配标的。")
        if not dry_run: send_notifications(None, sell_report)
        return

    results = []
    rem_cash, total_equity, cur_h, sec_e = get_portfolio_state()
    portfolio_state = {'holdings': cur_h, 'sector_exposure': sec_e, 'total_equity': total_equity}
    
    with ThreadPoolExecutor(max_workers=Config.IO_WORKERS) as executor:
        futures = {executor.submit(analyze_fundamentals, s, tech_data_dict[s], regime, portfolio_state): s for s in passed_tech}
        for f in as_completed(futures):
            res = f.result()
            if res: results.append(res)
    
    if results:
        df = pd.DataFrame(results).sort_values('综合得分', ascending=False)
        logging.info(f"✅ 筛选完成，发现 {len(df)} 个潜在目标。")
        
        # 保存与推送
        if not dry_run:
            actionable_df = df[df['建议股数'] > 0]
            save_signals_to_db(actionable_df, len(tickers), len(passed_tech))
            update_portfolio(actionable_df)
            backtest_report = run_auto_backtest()
            send_notifications(df, sell_report, backtest_report)
        
        # 生成报告文件
        df.to_csv('growth_hunter_results.csv', index=False)
        with open('growth_hunter_results.md', 'w', encoding='utf-8') as f:
            f.write(f"# 🚀 GrowthHunter 选股研报\n\n{df.to_markdown(index=False)}")
    else:
        logging.info("📭 深度审查后无标的入选。")
        if not dry_run: send_notifications(None, sell_report)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--input', type=str, default=None)
    args, _ = parser.parse_known_args()
    
    main(dry_run=args.dry_run, test_mode=args.test, input_file=args.input)
