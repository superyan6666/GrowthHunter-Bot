"""
🚀 GrowthHunter V7.0-ABC - 10倍股猎手 (全量终极版)
策略进化：期权异动 + NLP新闻 + 高管抄底 + 自动复盘 + 🩸 世纪轧空雷达 (Short Squeeze)
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
import logging
import atexit
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import warnings

warnings.filterwarnings('ignore')

# 【架构优化】：引入标准的日志记录，抛弃 print，便于云端排错
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 【架构优化】：集中管理超参数，便于后期调参
class Config:
    MARKET_CAP_MIN = 5e7
    MARKET_CAP_MAX = 2e9
    REVENUE_GROWTH_MIN = 0.20
    F_SCORE_MIN = 5
    SHORT_FLOAT_MIN = 0.20  # 做空流通股比例警戒线 (20%以上极易发生轧空)
    SLEEP_MIN = 0.2
    SLEEP_MAX = 1.0
    CACHE_DAYS = 30         # 小盘股名单缓存天数
    INSIDER_DAYS = 30       # 内幕交易探测回溯天数
    THREAD_WORKERS = 4      # 主流程分析并发线程数
    IO_WORKERS = 20         # 独立 I/O 超时控制专用线程数

# 随机 User-Agent 池
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
]

# 配置线程本地存储 (Thread Local)
thread_local = threading.local()

# 【修复致命错误】：定义全局复用的超时专用线程池，避免 NameError 和高频创建/销毁的性能损耗
# max_workers 确保大于外层主线程池*最大并发请求数，防止线程饥饿死锁
shared_io_executor = ThreadPoolExecutor(max_workers=Config.IO_WORKERS)

# 【稳健性优化】：注册 atexit 清理函数，确保即使遇到未捕获异常崩溃也能正确释放系统线程池
atexit.register(lambda: shared_io_executor.shutdown(wait=False))

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
    
    # 向后兼容：动态增加内幕交易与轧空雷达字段，防止表结构不一致报错
    for col in ['insider_trading', 'short_squeeze']:
        try:
            c.execute(f'ALTER TABLE signals ADD COLUMN {col} TEXT')
        except sqlite3.OperationalError:
            pass # 列已存在
        
    # 【细节优化】：为查询高频列建立独立索引，提升后期自动复盘的检索速度
    c.execute('CREATE INDEX IF NOT EXISTS idx_date ON signals (date)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON signals (symbol)')
    conn.commit()
    conn.close()

def save_signals_to_db(df):
    """将当天的有效信号保存进数据库，用于后期自动复盘"""
    try:
        conn = sqlite3.connect('growth_hunter_signals.db')
        today = datetime.now().strftime('%Y-%m-%d')
        for _, row in df.iterrows():
            # 使用 ON CONFLICT DO UPDATE 实现 Upsert，完美解决同一天重跑时旧记录无法更新的问题
            conn.execute('''
                INSERT INTO signals 
                (date, symbol, name, market_cap, revenue_growth, f_score, options_flow, catalyst, close_price, insider_trading, short_squeeze)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, symbol) DO UPDATE SET
                    market_cap=excluded.market_cap,
                    options_flow=excluded.options_flow,
                    catalyst=excluded.catalyst,
                    close_price=excluded.close_price,
                    insider_trading=excluded.insider_trading,
                    short_squeeze=excluded.short_squeeze
            ''', (today, row['股票代码'], row['公司名称'], row['市值(亿美元)'], 
                  row['营收增速'], row['F-Score'], row['期权异动'], row['催化剂'], 
                  row.get('最新收盘价', 0.0), row.get('内幕交易', '未知'), row.get('轧空雷达', '未知')))
        conn.commit()
        conn.close()
        logging.info("💾 信号已成功归档至本地 SQLite 数据库 (growth_hunter_signals.db)")
    except Exception as e:
        logging.error(f"⚠️ 数据库保存失败: {e}")

# ==========================================
# 终极模块 B-1：SEC 内幕交易与高管抄底嗅探
# ==========================================
def analyze_insider_trading(symbol):
    """扫描近期是否有高管或 10% 股东的真金白银净买入 (带超时防护)"""
    def _fetch_insider():
        local_ticker = yf.Ticker(symbol, session=get_session())
        return local_ticker.insider_transactions

    try:
        future = shared_io_executor.submit(_fetch_insider)
        try:
            df = future.result(timeout=5)
        except TimeoutError:
            return "请求超时"
        except Exception:
            return "获取失败"
                
        if df is None or df.empty: return "无近期记录"
        
        # 【时间过滤】：只看近 30 天的内幕交易，避免被半年前的历史老数据干扰
        try:
            cutoff = datetime.now() - pd.Timedelta(days=Config.INSIDER_DAYS)
            if isinstance(df.index, pd.DatetimeIndex):
                df = df[df.index.tz_localize(None) >= cutoff]
            elif 'Start Date' in df.columns:
                df = df[pd.to_datetime(df['Start Date']).dt.tz_localize(None) >= cutoff]
        except Exception:
            pass
            
        if df.empty: return "无近期记录"
        
        buy_keywords = ['Buy', 'Purchase', 'P - Purchase']
        sell_keywords = ['Sale', 'Sell', 'S - Sale', 'Disposition']
        
        text_col = None
        for col in df.columns:
            if 'transaction' in str(col).lower() or 'text' in str(col).lower() or 'action' in str(col).lower():
                text_col = col
                break
                
        if not text_col: return "格式不支持"
            
        text_series = df[text_col].astype(str)
        is_buy = text_series.str.contains('|'.join(buy_keywords), case=False, na=False)
        is_sell = text_series.str.contains('|'.join(sell_keywords), case=False, na=False)
        
        buys = df[is_buy & ~is_sell]
        
        if len(buys) > 0:
            return f"🚨 高管净买入 ({len(buys)}笔)"
        else:
            return "无高管买入"
    except Exception:
        return "获取失败"

# ==========================================
# 终极模块 B：期权异动与 Gamma 挤压扫描
# ==========================================
def analyze_options_flow(symbol):
    """拉取最近一期期权链，扫描看跌看涨比(PCR)与异常爆单（带超时防护）"""
    def _fetch_options():
        local_ticker = yf.Ticker(symbol, session=get_session())
        opts = local_ticker.options
        if not opts: return None, "无期权"
        return local_ticker.option_chain(opts[0]), None

    try:
        future = shared_io_executor.submit(_fetch_options)
        try:
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
        
        if call_vol == 0: return "⚠️ 无看涨成交 (极空)"
        
        pcr = put_vol / call_vol
        
        # 加入绝对门槛 (volume > 100)，剔除垃圾合约
        unusual_calls = calls[(calls['volume'] > 100) & (calls['openInterest'] > 0) & (calls['volume'] > calls['openInterest'] * 1.5)]
        
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
def analyze_catalyst(symbol):
    """基于内置词典的极速 NLP 新闻标题情绪打分"""
    def _fetch_news():
        return yf.Ticker(symbol, session=get_session()).news

    try:
        future = shared_io_executor.submit(_fetch_news)
        try:
            news = future.result(timeout=5)
        except TimeoutError:
            return "请求超时"
        except Exception:
            return "获取失败"
                
        if not news: return "无最新消息"
        
        pos_words = ['beat', 'surge', 'upgrade', 'fda', 'acquire', 'buy', 'profit', 'record', 'breakout', 'partner', 'approval']
        neg_words = ['miss', 'downgrade', 'lawsuit', 'investigate', 'offering', 'dilution', 'decline', 'warning', 'reject', 'fail', 'penalty']
        negation_context = ['not', 'fail', 'miss']
        
        score = 0
        latest_title = news[0].get('title', '无标题')
        
        for n in news[:3]:
            title = n.get('title', '').lower()
            
            has_pos = any(w in title for w in pos_words)
            has_negation = any(re.search(rf'\b{neg}\b', title) for neg in negation_context)
            
            if has_pos and not has_negation: 
                score += 1
            if any(w in title for w in neg_words): 
                score -= 1
        
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
            if cache_age < Config.CACHE_DAYS:
                df = pd.read_csv(cache_path)
                logging.info(f"✅ 使用本地缓存小盘股池（{len(df)} 只，缓存时效 {Config.CACHE_DAYS} 天）")
                return df['Symbol'].tolist()
        except Exception as e:
            logging.warning(f"⚠️ 缓存文件损坏或读取失败 ({e})，将重新拉取...")

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
            logging.info(f"✅ 成功加载 Russell 2000 共 {len(tickers)} 只")
            return tickers
        except Exception as e:
            logging.warning(f"⚠️ 从 {url} 获取源数据失败: {e}")
            continue

    logging.warning("⚠️ Russell 2000 所有源失效，自动回退抓取 S&P 600 小盘股...")
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_600_companies'
        headers = {'User-Agent': random.choice(USER_AGENTS)}
        response = requests.get(url, headers=headers, timeout=15)
        
        table = pd.read_html(response.text)[0]
        tickers = table['Symbol'].tolist()
        pd.DataFrame(tickers, columns=['Symbol']).to_csv(cache_path, index=False)
        logging.info(f"✅ 成功加载 S&P 600 共 {len(tickers)} 只")
        return tickers
    except Exception as e:
        logging.error(f"❌ 所有股票池获取失败: {e}")
        # 【终极兜底】：网页结构突变时的最后防线，使用符合市值要求(<20亿)的真实小盘股替换 mega-caps
        fallback_tickers = ['LUNR', 'BBAI', 'SOUN', 'GCT', 'VLD', 'STEM', 'IONQ', 'JOBY', 'ACHR', 'HUT', 'CLSK', 'BITF', 'IREN', 'WOLF', 'ENVX']
        logging.info(f"🛡️ 启动终极兜底方案，使用内置高弹性小盘股票池 ({len(fallback_tickers)} 只)")
        return fallback_tickers

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
    logging.info(f"⏳ 开始第一阶段：批量下载 {len(tickers)} 只股票及大盘基准(IWM)数据...")
    
    download_list = tickers + ['IWM']
    
    data = None
    for attempt in range(3):
        data = yf.download(download_list, period="1y", group_by="ticker", threads=True, show_errors=False)
        if data is not None and not data.empty:
            break
        logging.warning(f"⚠️ Yahoo Finance 接口返回空数据，等待 3 秒后重试 ({attempt+1}/3)...")
        time.sleep(3)
    
    if data is None or data.empty:
        logging.error("❌ Yahoo Finance 接口请求失败或重试耗尽，本次筛选终止。")
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
        logging.error("❌ 核心基准(IWM)获取失败，无法计算相对强度，终止流程。")
        return [], {}

    passed_tickers = []
    close_prices = {}
    
    # 【体验优化】：引入 tqdm 进度条，并在 GitHub Actions 云端运行时自动静音以防日志污染
    is_github_actions = os.getenv('GITHUB_ACTIONS') == 'true'
    for sym in tqdm(tickers, desc="🎯 技术面筛选", disable=is_github_actions):
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
            
    logging.info(f"🎯 第一阶段完成：技术与资金面保留 {len(passed_tickers)} 只标的")
    return passed_tickers, close_prices

def calculate_piotroski_f_score(bs, cf, inc):
    """Piotroski F-Score 复合财务健康评分 (0-9分)"""
    f_score = 0
    try:
        if bs is None or cf is None or inc is None: return "N/A"
        if bs.empty or cf.empty or inc.empty or len(bs.columns) < 2 or len(inc.columns) < 2: return "N/A"
            
        def get_val(df, keys, idx=0):
            """【终极修复】：彻底抛弃 loc 和类型判断，使用纯矢量化的布尔掩码安全提取，免疫重复索引与结构突变"""
            if df is None or df.empty or len(df.columns) <= idx: return 0
            try:
                # 将索引安全转为小写字符串
                str_idx = df.index.astype(str).str.lower()
                for key in keys:
                    # 使用布尔掩码过滤出所有包含关键字的行
                    mask = str_idx.str.contains(key.lower(), regex=False, na=False)
                    if mask.any():
                        # 安全获取满足条件的子 DataFrame，保证数据结构规整
                        matches = df[mask]
                        # 强制取第一个匹配行的指定列索引
                        val = matches.iloc[0, idx]
                        if pd.notna(val): 
                            return float(val)
            except Exception:
                pass
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
    第二阶段：财务过滤 + 期权异动 + 新闻情绪 + 内幕交易 + 【轧空雷达】
    """
    time.sleep(random.uniform(Config.SLEEP_MIN, Config.SLEEP_MAX))
    
    for attempt in range(2):
        try:
            local_session = get_session()
            ticker = yf.Ticker(symbol, session=local_session)
            
            info = ticker.info
            if not info or 'longName' not in info: return None

            market_cap = info.get('marketCap', 0)
            if not (Config.MARKET_CAP_MIN < market_cap < Config.MARKET_CAP_MAX): return None

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
                    
            if revenue_growth is None or revenue_growth < Config.REVENUE_GROWTH_MIN: return None

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
            if not isinstance(f_score, int) or f_score < Config.F_SCORE_MIN: return None

            total_revenue = info.get('totalRevenue', 0)
            rd_expense = info.get('researchAndDevelopment')
            rd_ratio = (rd_expense / total_revenue) if (rd_expense and total_revenue > 0) else 0
            
            sector = info.get('sector', '')
            if sector in {'Healthcare', 'Technology'} and rd_ratio < 0.08: return None

            # ==============================
            # 调用 V7.0-ABC 终极外挂
            # ==============================
            options_flow = analyze_options_flow(symbol)
            catalyst = analyze_catalyst(symbol)
            insider_trading = analyze_insider_trading(symbol)
            
            # 【终极方向 C：世纪轧空雷达】直接从 info 中极速提取，0 额外耗时
            short_float = info.get('shortPercentOfFloat')
            short_ratio = info.get('shortRatio', 'N/A')
            squeeze_signal = "无"
            if short_float is not None:
                if short_float >= Config.SHORT_FLOAT_MIN:
                    squeeze_signal = f"🩸 世纪轧空 (空头占比: {short_float:.1%}, 回补天数: {short_ratio})"
                elif short_float >= 0.10:
                    squeeze_signal = f"⚠️ 高度做空 (空头占比: {short_float:.1%})"

            rg_str = f"{revenue_growth:.1%}" if revenue_growth is not None else "N/A"
            gm_str = f"{gross_margin:.1%}" if gross_margin is not None else "N/A"
            f_score_str = f"{f_score}/9" if isinstance(f_score, int) else "N/A"

            # 【综合得分权重逻辑说明】：
            # 基础分：F-Score 财务健康度 (0-9分) 决定企业底盘
            # 核心爆发加分项：
            # +2分：期权看涨爆单 (聪明资金正在埋伏)
            # +2分：高管净买入 (内部人士真金白银背书)
            # +2分：世纪轧空 (做空比例超 20%，踩踏暴涨潜力极大)
            # 辅助催化加分项：
            # +1分：新闻利好 (短期情绪催化)
            # +1分：高度做空 (做空比例超 10%，具备轧空雏形)
            
            # 【得分重构】：将 F-Score 减去最低门槛(5)，使基础分为 0-4 分，让催化剂权重更凸显
            comp_score = (f_score - Config.F_SCORE_MIN) if isinstance(f_score, int) else 0
            if '🔥' in options_flow: comp_score += 2
            if '🚀' in catalyst: comp_score += 1
            if '🚨' in insider_trading: comp_score += 2
            if '🩸' in squeeze_signal: comp_score += 2
            elif '⚠️' in squeeze_signal: comp_score += 1

            reasons = (
                f"F-Score: {f_score_str} | 增速: {rg_str} | 毛利: {gm_str}\n"
                f"  └ 🎲 期权: {options_flow}\n"
                f"  └ 📰 消息: {catalyst}\n"
                f"  └ 🕵️‍♂️ 内幕: {insider_trading}\n"
                f"  └ 🩸 轧空: {squeeze_signal}"
            )

            return {
                '股票代码': symbol,
                '公司名称': info.get('longName', symbol),
                '所属行业': sector,
                '市值(亿美元)': round(market_cap / 1e8, 2),
                '营收增速': rg_str,
                'F-Score': f_score_str,
                '综合得分': comp_score,
                '期权异动': options_flow,
                '催化剂': catalyst,
                '内幕交易': insider_trading,
                '轧空雷达': squeeze_signal,
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

# ==========================================
# 终极模块 A：T+N 胜率自动复盘与自我考核引擎
# ==========================================
def run_auto_backtest():
    """从 SQLite 数据库读取历史推荐，自动计算 T+1, T+5, T+20 真实胜率"""
    try:
        if not os.path.exists('growth_hunter_signals.db'):
            return ""
        
        conn = sqlite3.connect('growth_hunter_signals.db')
        df_db = pd.read_sql_query("SELECT * FROM signals", conn)
        conn.close()
        
        if df_db.empty: return ""
        
        df_db['date'] = pd.to_datetime(df_db['date'])
        unique_dates = sorted(df_db['date'].dt.date.unique(), reverse=True)
        
        if len(unique_dates) <= 1: 
            return "\n📊 【复盘战报】: 数据积累中，明天将产生首份 T+1 战报。\n"
        
        report = "\n📊 【系统自动复盘战报】\n"
        
        target_intervals = {'T+1': 1, 'T+5': 5, 'T+20': 20}
        tickers_to_fetch = set()
        backtest_tasks = {}
        
        anchor_date = pd.to_datetime(unique_dates[0])
        
        for label, n in target_intervals.items():
            ideal_target_ts = anchor_date - pd.offsets.BDay(n)
            ideal_target_date = ideal_target_ts.date()
            
            valid_past_dates = [d for d in unique_dates if d <= ideal_target_date]
            
            if valid_past_dates:
                actual_target_date = valid_past_dates[0]
                signals = df_db[df_db['date'].dt.date == actual_target_date]
                if not signals.empty:
                    backtest_tasks[label] = signals
                    tickers_to_fetch.update(signals['symbol'].tolist())
                    
        if not tickers_to_fetch: return ""
        
        logging.info(f"⏳ 正在执行 T+N 自动复盘，拉取 {len(tickers_to_fetch)} 只历史标的最新价...")
        current_data = yf.download(list(tickers_to_fetch), period="1d", group_by="ticker", show_errors=False, threads=True)
        current_prices = {}
        
        if isinstance(current_data.columns, pd.MultiIndex):
            for t in tickers_to_fetch:
                if t in current_data.columns.get_level_values(0):
                    s = current_data[t]['Close'].dropna()
                    if not s.empty: current_prices[t] = float(s.iloc[-1])
        else:
            if len(tickers_to_fetch) == 1:
                t = list(tickers_to_fetch)[0]
                s = current_data['Close'].dropna()
                if not s.empty: current_prices[t] = float(s.iloc[-1])
        
        for label, signals in backtest_tasks.items():
            wins, total = 0, 0
            returns = []
            for _, row in signals.iterrows():
                sym = row['symbol']
                base_price = row['close_price']
                if sym in current_prices and base_price and base_price > 0:
                    curr_p = current_prices[sym]
                    ret = (curr_p - base_price) / base_price
                    returns.append(ret)
                    if ret > 0: wins += 1
                    total += 1
            
            if total > 0:
                win_rate = wins / total
                avg_ret = sum(returns) / total
                target_date_str = signals['date'].iloc[0].strftime('%m-%d')
                
                # 【透明度优化】：计算实际相距的自然日天数，避免节假日或漏运行带来的周期感知偏差
                actual_days_diff = (datetime.now().date() - pd.to_datetime(signals['date'].iloc[0]).date()).days
                report += f" • {label} ({target_date_str}推, 距今{actual_days_diff}天, {total}只): 胜率 {win_rate:.0%} | 平均收益 {avg_ret:+.1%}\n"
        
        return report
    except Exception as e:
        return f"\n⚠️ 复盘模块异常: {e}\n"

def send_notifications(df, backtest_report=""):
    """多平台推送模块 (安全截断与 Markdown 适配)"""
    if df.empty and not backtest_report:
        logging.info("📭 今日无符合严苛条件的标的，且无历史复盘数据。")
        return
        
    summary = f"🚀 GrowthHunter V7.0-ABC 异动播报\n\n"
    
    if not df.empty:
        summary += f"捕获 {len(df)} 只底盘扎实且量价齐升的起爆股！\n\n"
    else:
        summary += f"今日无新增达标起爆股。\n\n"
        
    max_len = 3500
    for _, row in df.head(10).iterrows():
        item_text = f"• [{row['股票代码']}] {row['公司名称']} ({row['市值(亿美元)']}亿)\n  └ {row['筛选理由']}\n\n"
        if len(summary) + len(item_text) > max_len:
            summary += "...（新标的内容过长已自动截断）\n\n"
            break
        summary += item_text
        
    summary += backtest_report
        
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
                res = requests.get(f"https://sctapi.ftqq.com/{val}.send", params={"title": "🚀 V7 终极起爆及战报", "desp": summary}, timeout=10)
            elif platform == 'Telegram':
                chat_id = os.getenv('TELEGRAM_CHAT_ID')
                if not chat_id:
                    status_report.append(f"⚪ Telegram: 未配置 CHAT_ID")
                    continue
                    
                # 【稳健性优化】：利用正则暂存 URL，防止 Markdown 转义符破坏链接
                links = re.findall(r'\[.*?\]\(.*?\)', summary)
                temp_summary = summary
                for i, link in enumerate(links):
                    temp_summary = temp_summary.replace(link, f"@@LINK_{i}@@")
                    
                tg_summary = temp_summary.replace('_', '\\_').replace('*', '\\*')
                
                # 恢复 URL
                for i, link in enumerate(links):
                    tg_summary = tg_summary.replace(f"@@LINK_{i}@@", link)
                    
                res = requests.post(f"https://api.telegram.org/bot{val}/sendMessage", json={"chat_id": chat_id, "text": tg_summary, "parse_mode": "Markdown"}, timeout=10)
            else:
                payload = {"msg_type": "text", "content": {"text": summary}} if platform == '飞书' else {"msgtype": "text", "text": {"content": summary}}
                res = requests.post(val, json=payload, timeout=10)
                
            if res.status_code == 200: status_report.append(f"✅ {platform}: 成功")
            else: status_report.append(f"❌ {platform}: 异常 ({res.text})")
        except Exception as e: 
            status_report.append(f"❌ {platform}: 失败 ({str(e)})")

    logging.info("\n" + "="*30 + "\n 📢 推送汇总\n" + "="*30)
    for s in status_report: logging.info(s)
    logging.info("="*30 + "\n")

def test_notifications():
    logging.info("🔧 启动推送测试模式...")
    init_db()
    mock_data = [{'股票代码': 'TEST', '公司名称': '配置测试股', '市值(亿美元)': 8.8, '营收增速': '50%', 'F-Score': '9/9', '综合得分': 7, '期权异动': '🔥 看涨爆单', '催化剂': '🚀 利好 (TEST Q3 Beat...)', '内幕交易': '🚨 高管净买入 (2笔)', '轧空雷达': '🩸 世纪轧空 (空头占比: 25.5%, 回补天数: 6.2)', '最新收盘价': 100.5, '筛选理由': 'V7.0-ABC 系统全栈就绪！\n  └ 🩸 轧空: 🩸 世纪轧空 (空头占比: 25.5%, 回补天数: 6.2)'}]
    df = pd.DataFrame(mock_data)
    save_signals_to_db(df)
    
    mock_backtest = "\n📊 【系统自动复盘战报】\n • T+1 (10-24推 3只): 胜率 67% | 平均收益 +4.2%\n • T+5 (10-18推 5只): 胜率 80% | 平均收益 +12.5%\n"
    send_notifications(df, mock_backtest)

def main(dry_run=False):
    logging.info("="*40 + " 🚀 GrowthHunter V7.0-ABC (全量终极版) " + "="*40)
    if dry_run:
        logging.info("🏃 【DRY-RUN 空跑模式启动】：仅选股，跳过数据库写入与推送。")
    
    init_db()
    
    tickers = get_small_cap_tickers()
    if not tickers: return

    passed_tech_tickers, close_prices_dict = batch_technical_screen(tickers)
    if not passed_tech_tickers: return

    results = []
    logging.info(f"⏳ 第二阶段：深挖 {len(passed_tech_tickers)} 只股票财务、期权、消息与做空面...")
    
    with ThreadPoolExecutor(max_workers=Config.THREAD_WORKERS) as executor:
        futures = {executor.submit(analyze_fundamentals, sym, close_prices_dict.get(sym, 0.0)): sym for sym in passed_tech_tickers}
        for f in as_completed(futures):
            res = f.result()
            if res: results.append(res)

    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values(by=['综合得分', '市值(亿美元)'], ascending=[False, True])
        
        md_df = df.copy()
        md_df['股票代码'] = md_df['股票代码'].apply(lambda x: f"[{x}](https://finance.yahoo.com/quote/{x})")
        
        df.to_csv('growth_hunter_results.csv', index=False)
        try: md_table = md_df.to_markdown(index=False)
        except: md_table = md_df.to_string(index=False)
        with open('growth_hunter_results.md', 'w', encoding='utf-8') as f:
            f.write(f"# 🚀 GrowthHunter 严选报告\n\n**生成时间**：{datetime.now()}\n\n{md_table}")
        
        logging.info(f"🎉 大功告成！捕获 {len(results)} 只硬核标的。")
        
        if not dry_run:
            save_signals_to_db(df)
    else: 
        logging.info("📉 今日无新增达标标的。")
    
    if not dry_run:
        backtest_report = run_auto_backtest()
        send_notifications(df, backtest_report)

if __name__ == "__main__":
    try:
        if len(sys.argv) > 1 and sys.argv[1] == '--test': 
            test_notifications()
        elif len(sys.argv) > 1 and sys.argv[1] == '--dry-run': 
            main(dry_run=True)
        else: 
            main()
    finally:
        pass # atexit 已接管线程池释放，保持代码结构规整
