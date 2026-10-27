# -*- coding: utf-8 -*-
"""趋势指标（不含预测）。

同比/环比、N 月移动均值、套均面积、区域份额/排名、面积段·价格段占比。
全部基于官方自爬数据，确定性、可复现。
"""
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
    df['week_start'] = df['日期'] - pd.to_timedelta(df['日期'].dt.weekday, unit='d')
    df['_is_est'] = df['日期'].dt.strftime('%Y-%m-%d').isin(est)
    g = df.groupby('week_start').agg(
        套数=('住宅签约套数', 'sum'),
        面积=('住宅签约面积', 'sum'),
        days=('日期', 'nunique'),
        est_days=('_is_est', 'sum'),
    ).reset_index().sort_values('week_start')
    g['est_days'] = g['est_days'].astype(int)
    g['套均面积'] = np.where(g['套数'] > 0, g['面积'] / g['套数'], np.nan)
    g['MA4'] = g['套数'].rolling(4, min_periods=1).mean()
    g['MA4含估算'] = g['est_days'].rolling(4, min_periods=1).sum() > 0
    return g.reset_index(drop=True)


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
    df['week_start'] = df['日期'] - pd.to_timedelta(df['日期'].dt.weekday, unit='d')
    df['认购住宅'] = df['期房认购住宅套数'].fillna(0) + df['现房认购住宅套数'].fillna(0)
    df['签约住宅'] = df['期房签约住宅套数'].fillna(0) + df['现房签约住宅套数'].fillna(0)
    g = (df.groupby('week_start')
         .agg(认购住宅=('认购住宅', 'sum'), 签约住宅=('签约住宅', 'sum'),
              days=('日期', 'nunique'))
         .reset_index().sort_values('week_start'))
    g['MA4'] = g['签约住宅'].rolling(4, min_periods=1).mean()
    return g.reset_index(drop=True)


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
    mom = wide.pct_change() * 100
    mom.iloc[0] = np.nan
    for i in range(1, len(wide)):
        if (wide.index[i] - wide.index[i - 1]).n != 1:
            mom.iloc[i] = np.nan
    return mom.T  # 行=区县、列=月（热力图布局）


def coverage_counts(monthly, district, area):
    """数据覆盖月数 (月度, 区县, 面积段)——report 与 ai_digest 共用的唯一计数实现。"""
    return (len(monthly),
            district['period'].nunique() if not district.empty else 0,
            area['period'].nunique() if not area.empty else 0)
