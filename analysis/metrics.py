# -*- coding: utf-8 -*-
"""趋势指标（不含预测）。

同比/环比、N 月移动均值、套均面积、区域份额/排名、面积段·价格段占比。
全部基于官方自爬数据，确定性、可复现。
"""
import sys

import chinese_calendar as cc
import pandas as pd
import numpy as np

import quality


def add_yoy_mom(monthly):
    """给全市月度表加同比/环比列（住宅签约套数）。

    防护（设计 v1.1 §2.3）：
    - 环比断档：上一行不是上个整月（年×12+月 序数差≠1）→ NaN，禁止跨缺失月硬算；
    - 同比门控：仅当本点处于 ≥13 个月的连续段（去年同月与之间 12 个月都在）才出值。
    """
    df = monthly.copy().sort_values('period').reset_index(drop=True)
    idx = df['year'] * 12 + df['month']
    step = idx.diff()
    df['环比'] = np.where(step == 1, df['住宅签约套数'].pct_change() * 100, np.nan)

    run_start = idx.groupby((step != 1).cumsum()).transform('first')
    span = idx - run_start
    prev = df[['year', 'month', '住宅签约套数']].rename(
        columns={'year': 'prev_year', '住宅签约套数': '去年同月'}
    )
    prev['prev_year'] = prev['prev_year'] + 1  # 对齐：当前年(year) ← 去年(year-1)
    df = df.merge(
        prev, left_on=['year', 'month'], right_on=['prev_year', 'month'], how='left'
    )
    yoy = np.where(
        df['去年同月'].notna(),
        (df['住宅签约套数'] - df['去年同月']) / df['去年同月'] * 100,
        np.nan,
    )
    df['同比'] = np.where(span >= 12, yoy, np.nan)
    return df


def add_moving_avg(monthly, windows=(3, 12)):
    """加 N 月移动均值列（住宅签约套数）。序列短于窗口时自然全 NaN（降级）。"""
    df = monthly.copy().sort_values('period').reset_index(drop=True)
    s = df['住宅签约套数']
    for w in windows:
        df[f'MA{w}'] = s.rolling(window=w, min_periods=w).mean()
    return df


def sanitize_bad_values(monthly):
    """把 quality.KNOWN_ISSUES 登记的坏值置 NaN（当前：2025-01 网上签约面积）。

    供 TTM/YTD 等后续指标使用：含坏值的窗口必须整窗 NaN，而不是 skipna 剔月求和。
    """
    df = monthly.copy()
    for (ym, label), _ in quality.KNOWN_ISSUES.items():
        if label == '住宅签约面积>网上签约面积':
            df.loc[df['period'].astype(str) == ym, '网上签约面积(m2)'] = np.nan
    return df


def expand_estimated_days(mapping=None):
    """ESTIMATED_DAYS 键（'2026-07-25/26' 简写或 '2026-09-16'）→ 具体 'YYYY-MM-DD' 列表。"""
    if mapping is None:
        mapping = quality.ESTIMATED_DAYS
    days = []
    for k in mapping:
        parts = [p.strip() for p in str(k).split('/')]
        days.append(parts[0])
        for p in parts[1:]:
            days.append(p if len(p) > 2 else parts[0][:8] + p)
    return sorted(set(days))


def avg_unit_area(monthly):
    """套均面积 = 住宅签约面积 / 住宅签约套数（反映刚需/改善结构）。"""
    df = monthly.copy()
    df = df[df['住宅签约套数'] > 0].copy()
    df['套均面积'] = df['住宅签约面积(m2)'] / df['住宅签约套数']
    return df


def district_share(district):
    """各区签约套数 + 占全市份额（按月）。返回长表 [period, 区县_clean, 签约套数, 份额]。"""
    if district.empty:
        return district
    df = district[district['区县_clean'] != '全市'].copy()
    totals = df.groupby('period')['签约套数'].transform('sum')
    df['份额'] = df['签约套数'] / totals * 100
    return df


def district_rank_change(district):
    """各区最新月份 vs 最早月份的排名变化。返回 DataFrame。"""
    detail = district[district['区县_clean'] != '全市'].copy()
    if detail.empty:
        return detail
    periods = sorted(detail['period'].unique())
    if len(periods) < 2:
        first = last = periods[-1]
    else:
        first, last = periods[0], periods[-1]
    f = detail[detail['period'] == first].groupby('区县_clean')['签约套数'].sum()
    l = detail[detail['period'] == last].groupby('区县_clean')['签约套数'].sum()
    out = pd.DataFrame({'首月套数': f, '末月套数': l})
    out['首月排名'] = out['首月套数'].rank(ascending=False, method='min').astype(int)
    out['末月排名'] = out['末月套数'].rank(ascending=False, method='min').astype(int)
    out['排名变化'] = out['首月排名'] - out['末月排名']  # 正=上升
    out['份额变化_pp'] = (out['末月套数'] / out['末月套数'].sum() * 100
                        - out['首月套数'] / out['首月套数'].sum() * 100)
    return out.sort_values('末月套数', ascending=False)


def segment_share(series_df, period_col, seg_col, val_col):
    """通用：分段表按月的占比长表 [period, seg, val, share]。"""
    if series_df.empty:
        return series_df
    df = series_df.copy()
    totals = df.groupby(period_col)[val_col].transform('sum')
    df['share'] = df[val_col] / totals * 100
    return df


def _week_bucket(df, aggs):
    """按自然周（周一起）分桶的共用实现。aggs: {输出列: (源列, 聚合)}。

    周口径（周一起、跨月归月首所在周）只此一处定义，周度类聚合共用。
    会修改传入的 df（新增 week_start 列）——调用方须传自己的副本。
    返回按周升序且重排索引（末尾 reset 已含，调用方无需再 reset）。
    """
    df['week_start'] = df['日期'] - pd.to_timedelta(df['日期'].dt.weekday, unit='d')
    return (df.groupby('week_start').agg(**aggs)
            .reset_index().sort_values('week_start').reset_index(drop=True))


def weekly_aggregate(daily, estimated_dates=None):
    """按自然周（周一起）聚合日数据：套数、面积、套均面积、天数 + 4周移动均值。

    估算标记随聚合传播（设计 v1.1 §2.3 / 需求 §5.4）：
    - est_days：本周含估算日天数（estimated_dates 缺省取 quality.ESTIMATED_DAYS 展开）；
    - MA4含估算：当前 MA4 窗口（近 4 周）是否含估算周。
    部分周因节假日/抓取缺口，实际天数 <7，数值偏低（已在 days 列体现）。
    """
    if daily.empty:
        return daily
    if estimated_dates is None:
        estimated_dates = expand_estimated_days()
    est = {str(d)[:10] for d in estimated_dates}
    df = daily.copy()
    df['_is_est'] = df['日期'].dt.strftime('%Y-%m-%d').isin(est)
    g = _week_bucket(df, {'套数': ('住宅签约套数', 'sum'),
                          '面积': ('住宅签约面积', 'sum'),
                          'days': ('日期', 'nunique'),
                          'est_days': ('_is_est', 'sum')})
    g['est_days'] = g['est_days'].astype(int)
    g['套均面积'] = np.where(g['套数'] > 0, g['面积'] / g['套数'], np.nan)
    g['MA4'] = g['套数'].rolling(4, min_periods=1).mean()
    g['MA4含估算'] = g['est_days'].rolling(4, min_periods=1).sum() > 0
    return g


# ---------------------------------------------------------------------------
# 周度叙述事实（需求 v0.3 / 设计 v1.2 §2.1）：只出数字与枚举，文案在
# analysis/weekly_narrative.py。verify 侧另有一套 CSV 独立重算（防同源盲区），
# 两边规则必须逐字对齐——改任一侧先同步设计文档。

_CC_WARNED = {'flag': False}


def _net_workdays(week_start):
    """周内（周一起 7 天）工作日净额。chinese_calendar 含调休；未覆盖日期回退
    纯 weekday 并一次性 stderr 告警（回退模式同 script/analyze.py:classify_day_type）。"""
    n = 0
    for i in range(7):
        d = week_start + pd.Timedelta(days=i)
        try:
            if cc.is_workday(d):
                n += 1
        except NotImplementedError:
            if not _CC_WARNED['flag']:
                _CC_WARNED['flag'] = True
                print(f'警告: chinese_calendar 未覆盖日期 {d.date()} 及之后，'
                      '节假日周判定回退纯 weekday（无假期识别）', file=sys.stderr)
            if d.weekday() < 5:
                n += 1
    return n


def _is_holiday_week(week_start):
    """节假日周：工作日净额 ≤3（需求 §2.3——净额口径覆盖调休补班与 1 天桥假）。"""
    return _net_workdays(week_start) <= 3


def _pair_comparable(a, b, hol):
    """相邻完整周是否可比 → (可比, 方向 1/-1 或中断原因)。

    中断：周序缺口 >7 天 / 任一侧节假日周 / 持平（设计 §2.1 判定一）。
    """
    if (b['week_start'] - a['week_start']).days > 7:
        return False, 'gap'
    if hol(a['week_start']) or hol(b['week_start']):
        return False, 'holiday'
    if a['套数'] == b['套数']:
        return False, 'tie'
    return True, (1 if b['套数'] > a['套数'] else -1)


def _pick_extreme(w13, hol, want_max):
    """13 周窗内极值周；同值 tie-break 优先非节假日周（防把次周同值归因假期）。"""
    best_ws, best_u = None, None
    for _, r in w13.iterrows():
        ws, u = r['week_start'], int(r['套数'])
        if best_u is None:
            best_ws, best_u = ws, u
        elif u == best_u:
            if not hol(ws) and hol(best_ws):    # tie-break：候选非节假日、现任是 → 换
                best_ws, best_u = ws, u
        elif (u > best_u) == want_max:
            best_ws, best_u = ws, u
    return best_ws, best_u


def weekly_trend_facts(weekly, daily=None):
    """周度叙述事实（设计 v1.2 §2.1/§3 schema）——纯数据 dict，不含任何文案。

    weekly：weekly_aggregate 输出（周口径与 est_days 单一来源）；
    daily：末周三态与同期对齐（ip_* 单一来源=末周子集；管线内必传，缺省仅测试用）。
    覆盖：方向链（N=链长−1）、null 四成因、峰谷（13 周窗）、水位（基准剔近 4 周的
    ≤26 周、±5% 死区）、末周三态（complete/in_progress/lagged）、同期对齐、
    narrative 门（n_full<8 字段最小化）。
    """
    f = {'narrative': False, 'n_full_weeks': 0, 'last_week_state': None,
         'last_full_week': None, 'last_full_units': None,
         'direction': None, 'null_reason': None, 'streak_weeks': 0,
         'last_pair_reversal': False, 'streak_contains_est': False,
         'post_holiday': False,
         'chain_first_week': None, 'chain_first_units': None,
         'anchor_kind': 'none', 'anchor_week': None,
         'avg4': None, 'avg26': None, 'avg26_weeks': 0,
         'level': None, 'level_gap_pct': None,
         'peak13_week': None, 'peak13_units': None, 'peak13_est': None,
         'peak13_holiday': None, 'trough13_week': None, 'trough13_units': None,
         'trough13_est': None, 'trough13_holiday': None,
         'ip_week': None, 'ip_units': None, 'ip_days': None,
         'ip_contiguous': None, 'ip_daily_avg': None,
         'ip_prev_same_avg': None, 'ip_prev_same_ok': None}
    if weekly is None or weekly.empty:
        return f
    weekly = weekly.sort_values('week_start').reset_index(drop=True)
    fw = weekly[weekly['days'] >= 7].reset_index(drop=True)
    hol_cache = {}

    def hol(ws):
        if ws not in hol_cache:
            hol_cache[ws] = _is_holiday_week(ws)
        return hol_cache[ws]

    f['n_full_weeks'] = int(len(fw))

    # ---- 周侧：方向链 + 峰谷（设计 §2.1 判定一/三）----
    if not fw.empty:
        last = fw.iloc[-1]
        f['last_full_week'] = str(last['week_start'].date())
        f['last_full_units'] = int(last['套数'])
        chain = [len(fw) - 1]
        chain_dir = None
        first_break = None
        i = len(fw) - 1
        while i >= 1:
            ok, v = _pair_comparable(fw.iloc[i - 1], fw.iloc[i], hol)
            if not ok:
                first_break = v
                break
            if chain_dir is None:
                chain_dir = v
            elif v != chain_dir:
                break
            chain.insert(0, i - 1)
            i -= 1
        if chain_dir is not None:
            f['direction'] = 'up' if chain_dir == 1 else 'down'
            f['streak_weeks'] = len(chain) - 1  # N = 上涨/下跌次数（链 [100,110] → N=1）
            if len(chain) >= 2:                 # S3"从 X 回到 Y"需要链首值（实施增补）
                cf = fw.iloc[chain[0]]
                f['chain_first_week'] = str(cf['week_start'].date())
                f['chain_first_units'] = int(cf['套数'])
        else:
            f['null_reason'] = {
                'gap': 'gap',
                'holiday': 'holiday_last' if hol(last['week_start']) else 'holiday_prev',
                'tie': 'tie',
            }.get(first_break)
        if len(chain) == 2 and chain[0] >= 1:
            ok, v = _pair_comparable(fw.iloc[chain[0] - 1], fw.iloc[chain[0]], hol)
            f['last_pair_reversal'] = bool(ok and isinstance(v, int) and v != chain_dir)
        f['streak_contains_est'] = bool(any(fw.iloc[j]['est_days'] > 0 for j in chain))
        j0 = chain[0]
        f['post_holiday'] = bool(j0 >= 1 and hol(fw.iloc[j0 - 1]['week_start']))

        # 峰谷：13 周窗（不足 13 全 None）；谷值保留节假日周候选
        if len(fw) >= 13:
            w13 = fw.tail(13)
            for key, want_max in (('peak13', True), ('trough13', False)):
                ws, u = _pick_extreme(w13, hol, want_max)
                row = w13[w13['week_start'] == ws].iloc[0]
                f[f'{key}_week'] = str(ws.date())
                f[f'{key}_units'] = u
                f[f'{key}_est'] = bool(row['est_days'] > 0)
                f[f'{key}_holiday'] = bool(hol(ws))

        # 水位：avg4 vs 基准（剔近 4 周的最近 ≤26 完整周；基准 <13 → level=null）
        if len(fw) >= 4:
            raw4 = float(fw['套数'].tail(4).mean())
            base = fw.iloc[:-4].tail(26)
            f['avg4'] = round(raw4, 1)
            f['avg26_weeks'] = int(len(base))
            if not base.empty:
                raw26 = float(base['套数'].mean())
                f['avg26'] = round(raw26, 1)
                if len(base) >= 13:
                    gap = (raw4 - raw26) / raw26 * 100
                    f['level_gap_pct'] = round(gap, 1)
                    f['level'] = ('parity' if abs(gap) <= 5
                                  else 'above' if gap > 0 else 'below')

            # 锚句事实（实施增补）：候选=13 周窗内排除链上/节假日/近 4 周各周，
            # 与 avg4 差 ≤10% 的最近周；无则 avg4 为窗口极值时高/低位，再否则 none
            if len(fw) >= 13:
                w13 = fw.tail(13)
                chain_set = {fw.iloc[j]['week_start'] for j in chain}
                last4_start = fw['week_start'].iloc[-4]
                cand = [r for _, r in w13.iterrows()
                        if r['week_start'] not in chain_set
                        and not hol(r['week_start'])
                        and r['week_start'] < last4_start]
                hit = None
                if cand:
                    close = min(cand, key=lambda r: abs(int(r['套数']) - raw4))
                    if abs(int(close['套数']) - raw4) <= 0.10 * raw4:
                        hit = close
                if hit is not None:
                    f['anchor_kind'] = 'week'
                    f['anchor_week'] = str(hit['week_start'].date())
                elif all(raw4 > int(u) for u in w13['套数']):
                    f['anchor_kind'] = 'high'
                elif all(raw4 < int(u) for u in w13['套数']):
                    f['anchor_kind'] = 'low'

    # ---- 末周三态 + 同期对齐（设计 §2.1 判定四；ip_* 源自 daily 末周子集）----
    lw = weekly.iloc[-1]
    if int(lw['days']) == 7:
        f['last_week_state'] = 'complete'
    else:
        prev = weekly.iloc[-2] if len(weekly) >= 2 else None
        if prev is not None and int(prev['days']) < 7 and not hol(prev['week_start']):
            f['last_week_state'] = 'lagged'
        else:
            f['last_week_state'] = 'in_progress'
        if daily is not None and not daily.empty:
            ws = lw['week_start']
            sub = daily[(daily['日期'] >= ws)
                        & (daily['日期'] <= ws + pd.Timedelta(days=6))]
            dates = sorted(sub['日期'])
            f['ip_week'] = str(ws.date())
            f['ip_days'] = int(len(dates))
            f['ip_units'] = int(sub['住宅签约套数'].sum())
            if dates:
                offs = [(d - ws).days for d in dates]
                f['ip_contiguous'] = bool(max(offs) - min(offs) + 1 == len(offs))
                f['ip_daily_avg'] = int(round(f['ip_units'] / f['ip_days']))
                prev_dates = [d - pd.Timedelta(days=7) for d in dates]
                have = set(daily['日期'])
                if all(d in have for d in prev_dates):
                    s = daily.set_index('日期')['住宅签约套数']
                    psum = int(sum(s.loc[d] for d in prev_dates))
                    f['ip_prev_same_avg'] = int(round(psum / len(prev_dates)))
                    f['ip_prev_same_ok'] = True
                else:
                    f['ip_prev_same_ok'] = False

    # ---- narrative 门：n_full<8 字段最小化（防 verify 直比歧义）----
    f['narrative'] = bool(len(fw) >= 8)
    if not f['narrative']:
        keep = {'narrative', 'n_full_weeks', 'last_week_state'}
        for k in f:
            if k not in keep:
                f[k] = None
    return f


def add_ttm(monthly, col='住宅签约套数', name='TTM套数'):
    """滚动 12 月总和（TTM，设计 v1.1 §2.3 / R5）。

    窗口须 12 个月**连续**（run_id 判定，断档整窗 NaN）且值全有效
    （min_periods=12：窗口内任何 NaN → 整窗 NaN，禁 skipna 剔月洗白）。
    """
    df = monthly.copy().sort_values('period').reset_index(drop=True)
    idx = df['year'] * 12 + df['month']
    run_id = (idx.diff() != 1).cumsum()
    ttm = df[col].rolling(12, min_periods=12).sum()
    same_run = run_id.eq(run_id.shift(11))
    df[name] = np.where(same_run, ttm, np.nan)
    return df


def add_ytd(monthly, col='住宅签约套数', name='YTD同比'):
    """累计同比（等窗，R5）：当年 1..n 月累计 vs 去年 1..n 月累计。

    年内缺月或含 NaN → 该年其后累计整段无效（groupby-year cummax 前缀污染），
    禁不等窗/剔月比较。仅住宅口径出值（总面积恒 NaN 明示，见需求 §4 R5）。
    """
    df = monthly.copy().sort_values('period').reset_index(drop=True)
    df['cum'] = df.groupby('year')[col].cumsum()
    seq = df.groupby('year').cumcount() + 1
    bad = df[col].isna() | (df['month'] != seq)
    bad_prefix = bad.groupby(df['year']).cummax()      # 年内一旦坏，其后全无效
    df['cum_ok'] = df['cum'].where(~bad_prefix & df[col].notna())
    prev = (df.assign(py=df['year'] + 1)[['py', 'month', 'cum_ok']]
            .rename(columns={'py': 'year', 'cum_ok': 'prev_cum'}))
    df = df.merge(prev, on=['year', 'month'], how='left')
    df[name] = np.where(
        df['cum_ok'].notna() & df['prev_cum'].notna() & (df['prev_cum'] > 0),
        (df['cum_ok'] - df['prev_cum']) / df['prev_cum'] * 100, np.nan)
    return df.drop(columns=['cum', 'cum_ok', 'prev_cum'])


def nonresidential_share(monthly):
    """非住宅占比（(总−住宅)/总，%）：套数与面积两列（R5）。

    面积侧守卫 总>住宅：2025-01 坏值（总 12.5 万 < 住宅 117 万）直接 NaN，
    不会算出负占比。
    """
    df = monthly.copy()
    tot_u, sub_u = df['网上签约套数'], df['住宅签约套数']
    tot_a, sub_a = df['网上签约面积(m2)'], df['住宅签约面积(m2)']
    df['非住宅套数占比'] = np.where(tot_u > sub_u, (tot_u - sub_u) / tot_u * 100, np.nan)
    df['非住宅面积占比'] = np.where(tot_a > sub_a, (tot_a - sub_a) / tot_a * 100, np.nan)
    return df


def new_home_weekly(new_daily):
    """新房周度（住宅口径，R1）：认购/签约 = 期房+现房合计，按自然周（周一起）。

    认购/签约字段 2026-08-23 起全量（此前 NaN→0，prep 层裁前导空周）；
    days<7 为不完整周（含 9-16 缺失影响的周）。附 MA4。
    """
    if new_daily is None or new_daily.empty:
        return new_daily
    df = new_daily.copy()
    df['认购住宅'] = df['期房认购住宅套数'].fillna(0) + df['现房认购住宅套数'].fillna(0)
    df['签约住宅'] = df['期房签约住宅套数'].fillna(0) + df['现房签约住宅套数'].fillna(0)
    g = _week_bucket(df, {'认购住宅': ('认购住宅', 'sum'),
                          '签约住宅': ('签约住宅', 'sum'),
                          'days': ('日期', 'nunique')})
    g['MA4'] = g['签约住宅'].rolling(4, min_periods=1).mean()
    return g


def new_inventory(new_daily):
    """新房住宅库存快照（日频）：可售期房住宅 + 未签约现房住宅（套）。

    库存列 2026-04-29 起全程有效（列级清洗保留）。去化周期需 3 个完整
    签约月（字段全量且无缺日）才启用——当前 0 个（9 月缺 16 日），
    预计 2027-01 起出值，先只列库存（设计决策表 #4）。
    """
    if new_daily is None or new_daily.empty:
        return new_daily
    df = new_daily.copy()
    df['库存住宅'] = df['可售期房住宅套数'] + df['未签约现房住宅套数']
    return df[['日期', '库存住宅']].dropna(subset=['库存住宅'])


def district_mom(district):
    """各区签约套数月度环比%（宽表：行=区县按总量排序，列=月）。

    首月无环比；断档防护（当前区县序列连续，防御性：跨档整列 NaN）。
    """
    d = district[district['区县_clean'] != '全市'].copy()
    wide = d.pivot_table(index='period', columns='区县_clean',
                         values='签约套数', aggfunc='sum')
    wide = wide[wide.sum().sort_values(ascending=False).index]
    mom = wide.pct_change() * 100   # 首行天然 NaN（无上月可比）
    for i in range(1, len(wide)):
        if (wide.index[i] - wide.index[i - 1]).n != 1:
            mom.iloc[i] = np.nan
    return mom.T  # 行=区县、列=月（热力图布局）


def agency_refund(agency):
    """机构退房率（月度，上榜口径，R3）。

    当月 = Σ上榜退房/Σ上榜签约；滚动3月 = 窗口内 Σ/Σ（合并比，禁比率均值）。
    2026-05/06 断档月补全为 NaN 行：当月值缺失，且任何含断档月的滚动窗口
    整窗 NaN（rolling min_periods=3 天然满足）。
    """
    if agency is None or agency.empty:
        return agency
    g = agency.groupby('period')[['签约套数', '退房套数']].sum()
    full = g.reindex(pd.period_range(g.index.min(), g.index.max(), freq='M'))
    full['退房率'] = np.where(full['签约套数'] > 0,
                             full['退房套数'] / full['签约套数'] * 100, np.nan)
    s3 = full[['签约套数', '退房套数']].rolling(3).sum()
    full['滚动3月'] = np.where(s3['签约套数'] > 0,
                               s3['退房套数'] / s3['签约套数'] * 100, np.nan)
    full.index.name = 'period'
    return full.reset_index()


def agency_top(agency, n=10):
    """最新月 Top N 机构 + 上榜内份额%（分母=当月上榜机构签约合计，非全市）。"""
    if agency is None or agency.empty:
        return agency
    latest = agency['period'].max()
    m = agency[agency['period'] == latest]
    top = m.nlargest(n, '签约套数').copy()
    top['上榜份额'] = top['签约套数'] / m['签约套数'].sum() * 100
    return top


def annual_anchor(monthly, annual):
    """五年长周期锚点（住宅·万套，R4）：官方年度 2020–2024（圆整值）+
    月度覆盖满 12 个月的年份求和（精确值）。口径切换缝由图注明示。
    """
    rows = {}
    for _, r in annual.iterrows():
        rows[int(r['年份'])] = {'万套': float(r['住宅套数万']), '口径': '官方年度'}
    for y, grp in monthly.groupby(monthly['period'].dt.year):
        if len(grp) == 12 and int(y) not in rows:
            rows[int(y)] = {'万套': grp['住宅签约套数'].sum() / 1e4, '口径': '月度求和'}
    return [{'年份': y, **rows[y]} for y in sorted(rows)]


def coverage_counts(monthly, district, area):
    """数据覆盖月数 (月度, 区县, 面积段)——report 与 ai_digest 共用的唯一计数实现。"""
    return (len(monthly),
            district['period'].nunique() if not district.empty else 0,
            area['period'].nunique() if not area.empty else 0)
