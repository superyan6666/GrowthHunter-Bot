import yfinance as yf
import pandas as pd
from datetime import datetime
import time
import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_safe_metric(info, key, default=None):
    """安全获取指标，区分 None 与 0，避免误杀"""
    val = info.get(key)
    if val is not None:
        return val
    return default

def get_tickers():
    """三重保障：缓存 + 实时 + 备用源"""
    cache_path = 'russell2000_cache.csv'
    # 优先使用7天内缓存
    if os.path.exists(cache_path):
        cache_age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_path))).days
        if cache_age < 7:
            df = pd.read_csv(cache_path)
            print(f"✅ 使用本地缓存 Russell 2000（{len(df)} 只）")
            return df['Symbol'].tolist()

    # 实时获取（多源备用）
    sources = [
        'https://stockanalysis.com/list/russell-2000/',
        'https://www.marketbeat.com/russell-2000/'
    ]
    for url in sources:
        try:
            tables = pd.read_html(url)
            df = tables[0]
            tickers = df['Symbol'].tolist()
            # 保存缓存
            pd.DataFrame(tickers, columns=['Symbol']).to_csv(cache_path, index=False)
            print(f"✅ 成功加载 Russell 2000 共 {len(tickers)} 只（已缓存）")
            return tickers
        except Exception as e:
            print(f"⚠️ {url} 加载失败: {str(e)[:80]}，尝试下一个源...")
            continue

    # 最终回退 S&P 500
    print("⚠️ 所有 Russell 源失败，回退 S&P 500")
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    table = pd.read_html(url)[0]
    return table['Symbol'].tolist()

def analyze_ticker(symbol):
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        if not info or 'longName' not in info:
            return None

        # 1. 市值过滤（10倍基因核心）
        market_cap = info.get('marketCap', 0)
        if not (1e8 < market_cap < 5e10):  # 1亿 ~ 500亿美元
            return None

        name = info.get('longName', symbol)
        sector = info.get('sector', '')
        industry = info.get('industry', '')
        summary = info.get('longBusinessSummary', '').lower()

        # 2. 高潜力扩展期
        promising_sectors = {'Technology', 'Healthcare', 'Communication Services'}
        promising_industries = {'Biotechnology', 'Semiconductors', 'Software', 'Application Software'}
        expansion_keywords = ['ai', 'artificial intelligence', 'biotech', 'electric vehicle', 'renewable', 'cloud', 'saas']
        is_promising = (sector in promising_sectors or 
                       any(ind in industry for ind in promising_industries) or
                       any(kw in summary for kw in expansion_keywords))
        if not is_promising:
            return None

        # 3. 增长 + 财务健康（安全处理缺失值）
        revenue_growth = get_safe_metric(info, 'revenueGrowth', 0)
        if revenue_growth < 0.20:
            return None

        gross_margin = get_safe_metric(info, 'grossMargins')
        operating_margin = get_safe_metric(info, 'operatingMargins')
        profit_margin = get_safe_metric(info, 'profitMargins')
        if (gross_margin is not None and gross_margin < 0.25) or \
           (operating_margin is not None and operating_margin <= 0) or \
           (profit_margin is not None and profit_margin <= 0):
            return None

        ps_ratio = get_safe_metric(info, 'priceToSalesTrailing12Months', 999)
        if ps_ratio > 15:
            return None

        # 风险控制
        debt_to_equity = get_safe_metric(info, 'debtToEquity', 0)
        free_cash_flow = get_safe_metric(info, 'freeCashflow', -999)
        current_ratio = get_safe_metric(info, 'currentRatio', 0)
        if debt_to_equity > 2.0 or free_cash_flow < 0 or current_ratio < 0.8:
            return None

        # 4. 研发费用率（10倍基因新因子）
        rd_expense = get_safe_metric(info, 'researchAndDevelopment', 0)
        total_revenue = get_safe_metric(info, 'totalRevenue', 0)
        rd_ratio = rd_expense / total_revenue if total_revenue > 0 else 0
        if sector in {'Healthcare', 'Technology'} and rd_ratio < 0.08:
            return None

        # 5. 股价趋势（52周高点 + 52周涨幅）
        current_price = info.get('currentPrice') or info.get('regularMarketPrice', 0)
        high_52w = info.get('fiftyTwoWeekHigh', 0)
        near_high = (high_52w > 0 and current_price / high_52w > 0.80)
        week52_change = get_safe_metric(info, '52WeekChange', get_safe_metric(info, 'fiftyTwoWeekChange', 0))
        if not near_high:
            return None

        # 6. 季度业绩改善（更严谨）
        q_income = ticker.quarterly_income_stmt
        net_income_row = next((row for row in q_income.index if 'Net Income' in str(row)), None)
        if net_income_row:
            net_series = q_income.loc[net_income_row].dropna().sort_index(ascending=True).tail(4)
            if len(net_series) >= 2:
                recent = net_series.iloc[-1]
                prev = net_series.iloc[-2]
                was_loss = any(x < 0 for x in net_series[:-1])
                improving = recent > prev * 0.9
                turnaround = (recent > 0) and was_loss
                if not (improving or turnaround):
                    return None

        # 7. 筛选理由
        reasons = (
            f"市值 {market_cap/1e9:.1f}B | 营收增长 {revenue_growth:.1%} | 毛利率 {gross_margin:.1% if gross_margin is not None else 'N/A'} | "
            f"研发率 {rd_ratio:.1%} | FCF 正 | 债务/权益 {debt_to_equity:.1f} | 52周涨幅 {week52_change:.1%} | 扩展期: 是"
        )

        return {
            '股票代码': symbol,
            '公司名称': name,
            '行业': f"{sector}/{industry}",
            '市值(亿USD)': round(market_cap / 1e9, 2),
            '52周涨幅': f"{week52_change:.1%}",
            '筛选理由': reasons,
            'Yahoo链接': f"https://finance.yahoo.com/quote/{symbol}"
        }
    except Exception as e:
        print(f"⚠️ 处理 {symbol} 时出错: {str(e)[:80]}")
        return None

def send_notifications(df):
    """四平台推送"""
    if df.empty:
        return
    repo = os.getenv('GITHUB_REPOSITORY', '你的用户名/仓库名')
    summary = f"🚀 GrowthHunter Bot v2.1 筛选完成！\n\n共找到 {len(df)} 只潜力10倍股（Russell 2000 小盘）\n\n"
    for _, row in df.head(8).iterrows():  # 最多推8条避免超长
        summary += f"• [{row['股票代码']}] {row['公司名称']} ({row['市值(亿USD)']}亿) - {row['52周涨幅']} ↑\n   {row['筛选理由'][:80]}...\n"
    summary += f"\n📊 完整表格请查看：https://github.com/{repo}/blob/main/screened_stocks.md"

    # Telegram
    token = os.getenv('TELEGRAM_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    if token and chat_id:
        try:
            requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          json={"chat_id": chat_id, "text": summary, "parse_mode": "Markdown"})
        except:
            pass

    # 飞书
    feishu_webhook = os.getenv('FEISHU_WEBHOOK')
    if feishu_webhook:
        try:
            requests.post(feishu_webhook, json={"msg_type": "text", "content": {"text": summary}})
        except:
            pass

    # 钉钉
    dingtalk_webhook = os.getenv('DINGTALK_WEBHOOK')
    if dingtalk_webhook:
        try:
            requests.post(dingtalk_webhook, json={"msgtype": "text", "text": {"content": summary}})
        except:
            pass

    # 微信 (Server酱)
    serverchan_key = os.getenv('SERVERCHAN_KEY')
    if serverchan_key:
        try:
            requests.get(f"https://sctapi.ftqq.com/{serverchan_key}.send",
                         params={"title": "🚀 GrowthHunter 10倍股更新", "desp": summary})
        except:
            pass

    print("✅ 推送已发送（已配置的平台）")

def main():
    tickers = get_tickers()
    results = []
    print(f"🚀 开始多线程筛选 {len(tickers)} 只股票...")

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_symbol = {executor.submit(analyze_ticker, sym): sym for sym in tickers}
        for future in as_completed(future_to_symbol):
            result = future.result()
            if result:
                results.append(result)
            # 每100只打印进度
            if len(results) % 100 == 0 and len(results) > 0:
                print(f"已找到 {len(results)} 只候选...")

    df = pd.DataFrame(results)
    df.to_csv('screened_stocks.csv', index=False, encoding='utf-8')

    with open('screened_stocks.md', 'w', encoding='utf-8') as f:
        f.write("# 🚀 GrowthHunter Bot v2.1 - 10倍股猎手\n\n")
        f.write(f"**更新时间**：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (太平洋时间)\n")
        f.write("**股票池**：Russell 2000（缓存防失效）+ 多线程 + 专家全部可实现升级\n\n")
        f.write("**核心筛选**：高增长 + 经营改善 + 财务健康 + R&D + 趋势确认 + 扩展期\n")
        f.write("**管理层背景**：暂手动复核（后续 LLM）\n\n")
        if len(df) > 0:
            f.write(df.to_markdown(index=False))
        else:
            f.write("暂无符合条件标的")
        f.write("\n\n*每日自动运行 · 四平台推送 · 完全开源免费*")

    print(f"✅ 筛选完成！共找到 {len(results)} 只潜力标的")
    send_notifications(df)

if __name__ == "__main__":
    main()
