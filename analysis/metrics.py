# -*- coding: utf-8 -*-
"""趋势指标（不含预测）。

同比/环比、N 月移动均值、套均面积、区域份额/排名、面积段·价格段占比。
全部基于官方自爬数据，确定性、可复现。
"""
import pandas as pd
import numpy as np


def add_yoy_mom(monthly):
    """给全市月度表加同比/环比列（住宅签约套数）。"""
    df = monthly.copy()
    df = df.sort_values('period').reset_index(drop=True)
    df['环比'] = df['住宅签约套数'].pct_change() * 100
    # 同比：与去年同月对比
    prev = df[['year', 'month', '住宅签约套数']].rename(
        columns={'year': 'prev_year', '住宅签约套数': '去年同月'}
    )
    prev['prev_year'] = prev['prev_year'] + 1  # 对齐：当前年(year) ← 去年(year-1)
    df = df.merge(
        prev, left_on=['year', 'month'], right_on=['prev_year', 'month'], how='left'
    )
    df['同比'] = np.where(
        df['去年同月'].notna(),
        (df['住宅签约套数'] - df['去年同月']) / df['去年同月'] * 100,
        np.nan,
    )
    return df


def add_moving_avg(monthly, windows=(3, 12)):
    """加 N 月移动均值列（住宅签约套数）。"""
    df = monthly.copy().sort_values('period').reset_index(drop=True)
    s = df['住宅签约套数']
    for w in windows:
        df[f'MA{w}'] = s.rolling(window=w, min_periods=w).mean()
    return df


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


def weekly_aggregate(daily):
    """按自然周（周一起）聚合日数据：套数、面积、套均面积、天数 + 4周移动均值。

    部分周因节假日/抓取缺口，实际天数 <7，数值偏低（已在 days 列体现）。
    """
    if daily.empty:
        return daily
    df = daily.copy()
    df['week_start'] = df['日期'] - pd.to_timedelta(df['日期'].dt.weekday, unit='d')
    g = df.groupby('week_start').agg(
        套数=('住宅签约套数', 'sum'),
        面积=('住宅签约面积', 'sum'),
        days=('日期', 'nunique'),
    ).reset_index().sort_values('week_start')
    g['套均面积'] = np.where(g['套数'] > 0, g['面积'] / g['套数'], np.nan)
    g['MA4'] = g['套数'].rolling(4, min_periods=1).mean()
    return g.reset_index(drop=True)
