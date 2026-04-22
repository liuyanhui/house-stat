# -*- coding: utf-8 -*-
"""北京二手住宅成交量分析脚本

用法:
  python script/analyze.py                    # 使用默认 data/ 目录
  python script/analyze.py --data-dir /path   # 指定数据目录
"""

import argparse
import os
import sys
import pandas as pd
import numpy as np

# 确保中文输出不乱码
try:
    sys.stdout.reconfigure(encoding='utf-8')
except (AttributeError, OSError):
    pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DATA_DIR = os.path.join(BASE_DIR, 'data')
ENC = 'utf-8-sig'


def load_data(data_dir):
    daily_path = os.path.join(data_dir, 'daily.csv')
    month_path = os.path.join(data_dir, 'month.csv')
    district_path = os.path.join(data_dir, 'month_district.csv')
    area_path = os.path.join(data_dir, 'month_area.csv')

    # 检查核心文件
    if not os.path.exists(daily_path):
        print(f'错误: 未找到核心数据文件 {daily_path}', file=sys.stderr)
        print('  daily.csv 包含每日签约数据，为必填文件。', file=sys.stderr)
        sys.exit(1)
    if not os.path.exists(month_path):
        print(f'错误: 未找到核心数据文件 {month_path}', file=sys.stderr)
        print('  month.csv 包含月度汇总数据，为必填文件。', file=sys.stderr)
        sys.exit(1)

    # 检查可选文件
    warnings = []
    if not os.path.exists(district_path):
        warnings.append(f'  未找到 {district_path}，区县分析将跳过')
    if not os.path.exists(area_path):
        warnings.append(f'  未找到 {area_path}，面积段分析将跳过')
    if warnings:
        print('注意:', file=sys.stderr)
        for w in warnings:
            print(w, file=sys.stderr)
        print(file=sys.stderr)

    daily = pd.read_csv(daily_path, encoding=ENC)
    month = pd.read_csv(month_path, encoding=ENC)

    # 区县和面积段数据为可选，缺失时返回空 DataFrame
    district = pd.read_csv(district_path, encoding=ENC) if os.path.exists(district_path) else pd.DataFrame()
    area = pd.read_csv(area_path, encoding=ENC) if os.path.exists(area_path) else pd.DataFrame()

    daily['日期'] = pd.to_datetime(daily['日期'], format='mixed')
    daily['year'] = daily['日期'].dt.year
    daily['month'] = daily['日期'].dt.month
    daily['weekday'] = daily['日期'].dt.weekday
    daily['year_month'] = daily['日期'].dt.to_period('M')

    month['日期'] = pd.to_datetime(month['月份'].astype(str).str.strip(), format='%Y-%m')
    month['year'] = month['日期'].dt.year
    month['month'] = month['日期'].dt.month

    if not district.empty:
        district['日期'] = pd.to_datetime(district['年月'].astype(str).str.strip(), format='%Y-%m')
    if not area.empty:
        area['日期'] = pd.to_datetime(area['年月'].astype(str).str.strip(), format='%Y-%m')

    return daily, month, district, area


def classify_day_type(df):
    """从数据本身推断假期：工作日(Mon-Fri)成交量低于当月工作日均值15%的视为假期"""
    df = df.copy()
    df['day_type'] = np.where(df['weekday'] >= 5, '周末', '工作日')

    for ym, grp in df.groupby('year_month'):
        workdays = grp[grp['day_type'] == '工作日']
        if len(workdays) == 0:
            continue
        threshold = workdays['住宅签约套数'].mean() * 0.15
        if threshold < 5:
            threshold = 5
        holiday_mask = (grp['day_type'] == '工作日') & (grp['住宅签约套数'] <= threshold)
        df.loc[holiday_mask[holiday_mask].index, 'day_type'] = '假期'

    return df


def pct_change(cur, prev):
    if prev == 0:
        return float('inf')
    return (cur - prev) / prev * 100


def fmt_pct(val):
    if val == float('inf') or val == float('-inf') or abs(val) > 9999:
        return 'N/A'
    return f'{val:+.1f}%'


def normalize_district(name):
    """统一区县名称：去除空格（全角/半角）"""
    return name.strip().replace('　', '').replace(' ', '')


# ============================================================
# Section 1: 月度住宅签约概览
# ============================================================
def section_monthly_overview(month, cur_year, prev_year):
    print('=' * 80)
    print('一、月度住宅签约量概览')
    print('=' * 80)

    years = sorted(month['year'].unique())
    m_cur = month[month['year'] == cur_year].set_index('month')
    m_prev = month[month['year'] == prev_year].set_index('month') if prev_year else None
    area_col = [c for c in month.columns if '住宅签约面积' in c][0]

    print()
    for y in years:
        label = f'← 当前年' if y == cur_year else ''
        print(f'--- {y}年 {label} ---')
        sub = month[month['year'] == y].sort_values('month')
        for _, row in sub.iterrows():
            print(f"  {y}-{row['month']:02d}: {int(row['住宅签约套数']):>6,} 套  |  {row[area_col]:>12,.2f} m2")
        print()

    # 同比 & 环比表
    print(f'--- 同比({cur_year} vs {prev_year}) & 环比 ---')
    print(f"{'月份':>6s} | {f'{prev_year}套数':>8s} | {f'{cur_year}套数':>8s} | {'同比':>8s} | {'环比':>8s}")
    print('-' * 55)
    prev_val = None
    for m in sorted(m_cur.index):
        cur = int(m_cur.loc[m, '住宅签约套数'])
        yoy_str = 'N/A'
        if m_prev is not None and m in m_prev.index:
            yoy_str = fmt_pct(pct_change(cur, int(m_prev.loc[m, '住宅签约套数'])))
        mom_str = 'N/A' if prev_val is None else fmt_pct(pct_change(cur, prev_val))
        val_prev = int(m_prev.loc[m, '住宅签约套数']) if (m_prev is not None and m in m_prev.index) else 0
        print(f'{m:>4d}月 | {val_prev:>8d} | {cur:>8d} | {yoy_str:>8s} | {mom_str:>8s}')
        prev_val = cur


# ============================================================
# Section 2: 按周住宅签约量
# ============================================================
def section_weekly(daily, cur_year):
    print()
    print('=' * 80)
    print('二、按周住宅签约量')
    print('=' * 80)

    df = daily[daily['year'] == cur_year].copy()
    df['yw'] = df['日期'].dt.strftime('%G-W%V')

    weekly = df.groupby('yw').agg(
        start=('日期', 'min'),
        end=('日期', 'max'),
        units=('住宅签约套数', 'sum'),
        days=('日期', 'count'),
    ).sort_values('start')

    print()
    print(f"{'周':>12s} | {'日期范围':>22s} | {'住宅套数':>8s} | {'日均':>6s} | {'周环比':>8s}")
    print('-' * 75)
    prev = None
    for idx, row in weekly.iterrows():
        avg = row['units'] / row['days'] if row['days'] > 0 else 0
        mom = fmt_pct(pct_change(row['units'], prev)) if prev is not None else 'N/A'
        dr = f"{row['start'].strftime('%m/%d')} - {row['end'].strftime('%m/%d')}"
        print(f'{idx:>12s} | {dr:>22s} | {int(row["units"]):>8d} | {avg:>6.0f} | {mom:>8s}')
        prev = row['units']


# ============================================================
# Section 3: 工作日 vs 周末/假期
# ============================================================
def section_day_type(df_classified):
    print()
    print('=' * 80)
    print('三、工作日 vs 周末/假期 住宅签约量')
    print('=' * 80)

    for ym, grp in df_classified.groupby('year_month'):
        print(f'\n  {ym}:')
        for dt in ['工作日', '周末', '假期']:
            sub = grp[grp['day_type'] == dt]
            if len(sub) == 0:
                continue
            total = int(sub['住宅签约套数'].sum())
            avg = sub['住宅签约套数'].mean()
            print(f'    {dt}: {total:>5d} 套 (共{len(sub)}天, 日均 {avg:.0f} 套)')


# ============================================================
# Section 4: 工作日日均趋势
# ============================================================
def section_workday_trend(df_classified):
    print()
    print('=' * 80)
    print('四、工作日日均住宅签约量趋势')
    print('=' * 80)

    wd = df_classified[df_classified['day_type'] == '工作日']

    wd_m = wd.groupby('year_month').agg(
        days=('日期', 'count'),
        total=('住宅签约套数', 'sum'),
        avg=('住宅签约套数', 'mean'),
        avg_area=('住宅签约面积', 'mean'),
        hi=('住宅签约套数', 'max'),
        lo=('住宅签约套数', 'min'),
    )

    print()
    print(f"{'月份':>8s} | {'工作日':>4s} | {'总签约':>6s} | {'日均':>6s} | {'日均面积':>8s} | {'最高':>5s} | {'最低':>5s} | {'日均环比':>8s}")
    print('-' * 80)
    prev_avg = None
    for idx, row in wd_m.iterrows():
        mom = fmt_pct(pct_change(row['avg'], prev_avg)) if prev_avg is not None else 'N/A'
        print(f'{str(idx):>8s} | {int(row["days"]):>4d} | {int(row["total"]):>6d} | {row["avg"]:>6.0f} | {row["avg_area"]:>8.0f} | {int(row["hi"]):>5d} | {int(row["lo"]):>5d} | {mom:>8s}')
        prev_avg = row['avg']


# ============================================================
# Section 5: 同比对比 (有日数据的月份)
# ============================================================
def section_yoy_daily(daily, cur_year, prev_year):
    print()
    print('=' * 80)
    print('五、工作日同比对比 (有同期日数据的月份)')
    print('=' * 80)

    years = sorted(daily['year'].unique())
    if len(years) < 2:
        print('  日数据不足两年，无法进行同比')
        return

    df_cur = classify_day_type(daily[daily['year'] == cur_year])
    df_prev = classify_day_type(daily[daily['year'] == prev_year])

    # 找有重叠的月份
    common_months = sorted(set(df_cur['month'].unique()) & set(df_prev['month'].unique()))

    for m in common_months:
        cur_m = df_cur[df_cur['month'] == m]
        prev_m = df_prev[df_prev['month'] == m]
        # 取相同日期范围
        max_day = min(cur_m['日期'].dt.day.max(), prev_m['日期'].dt.day.max())
        cur_r = cur_m[cur_m['日期'].dt.day <= max_day]
        prev_r = prev_m[prev_m['日期'].dt.day <= max_day]

        # 如果某年该月数据不完整（天数太少），跳过
        if len(cur_r) < 3 or len(prev_r) < 3:
            continue

        cur_total = int(cur_r['住宅签约套数'].sum())
        prev_total = int(prev_r['住宅签约套数'].sum())

        print(f'\n  {m}月 (截至{max_day}日):')
        print(f'    {cur_year}年: {cur_total:,} 套 | {prev_year}年: {prev_total:,} 套')
        print(f'    总量同比: {fmt_pct(pct_change(cur_total, prev_total))}')

        cur_wd = cur_r[cur_r['day_type'] == '工作日']
        prev_wd = prev_r[prev_r['day_type'] == '工作日']
        if len(cur_wd) > 0 and len(prev_wd) > 0:
            cur_avg = cur_wd['住宅签约套数'].mean()
            prev_avg = prev_wd['住宅签约套数'].mean()
            print(f'    工作日日均: {cur_year}={cur_avg:.0f} vs {prev_year}={prev_avg:.0f}, 同比 {fmt_pct(pct_change(cur_avg, prev_avg))}')


# ============================================================
# Section 6: 区县分析
# ============================================================
def section_district(district):
    print()
    print('=' * 80)
    print('六、区县分析')
    print('=' * 80)

    if district.empty:
        print('  无区县数据')
        return

    months = sorted(district['日期'].unique())
    if len(months) < 1:
        print('  无区县数据')
        return

    latest = months[-1]
    prev = months[-2] if len(months) >= 2 else None

    latest_data = district[district['日期'] == latest].copy()
    latest_data['区县_clean'] = latest_data['区县'].apply(normalize_district)
    latest_data = latest_data[latest_data['区县_clean'] != '全市']
    latest_data = latest_data.sort_values('签约套数', ascending=False)

    print(f'\n  最新月份: {pd.Timestamp(latest).strftime("%Y-%m")}')
    print(f"  {'区县':>6s} | {'签约套数':>8s} | {'成交面积':>10s} | {'环比变化':>8s}")
    print('  ' + '-' * 50)

    prev_data = district[district['日期'] == prev].copy() if prev else None
    if prev_data is not None and not prev_data.empty:
        prev_data['区县_clean'] = prev_data['区县'].apply(normalize_district)

    for _, row in latest_data.iterrows():
        name = row['区县_clean']
        units = int(row['签约套数'])
        area_val = row['成交面积']
        mom = 'N/A'
        if prev_data is not None and not prev_data.empty:
            prev_row = prev_data[prev_data['区县_clean'] == name]
            if len(prev_row) > 0:
                mom = fmt_pct(pct_change(units, int(prev_row['签约套数'].values[0])))
        print(f'  {name:>6s} | {units:>8d} | {area_val:>10.0f} | {mom:>8s}')

    # Top 3
    print(f'\n  Top 3 区县: {", ".join(latest_data.head(3)["区县_clean"].tolist())}')


# ============================================================
# Section 7: 面积段分析
# ============================================================
def section_area(area):
    print()
    print('=' * 80)
    print('七、面积段分析')
    print('=' * 80)

    if area.empty:
        print('  无面积段数据')
        return

    months = sorted(area['日期'].unique())
    if len(months) < 1:
        print('  无面积段数据')
        return

    latest = months[-1]
    prev = months[-2] if len(months) >= 2 else None

    latest_data = area[area['日期'] == latest].sort_values('成交套数', ascending=False)
    prev_data = area[area['日期'] == prev] if prev else None

    print(f'\n  最新月份: {pd.Timestamp(latest).strftime("%Y-%m")}')
    print(f"  {'面积段':>12s} | {'成交套数':>8s} | {'占比':>6s} | {'环比变化':>8s}")
    print('  ' + '-' * 50)

    total = latest_data['成交套数'].sum()
    for _, row in latest_data.iterrows():
        seg = row['面积区间']
        units = int(row['成交套数'])
        share = units / total * 100 if total > 0 else 0
        mom = 'N/A'
        if prev_data is not None:
            prev_row = prev_data[prev_data['面积区间'] == seg]
            if len(prev_row) > 0:
                mom = fmt_pct(pct_change(units, int(prev_row['成交套数'].values[0])))
        print(f'  {seg:>12s} | {units:>8d} | {share:>5.1f}% | {mom:>8s}')


# ============================================================
# Section 8: 关键发现
# ============================================================
def section_summary(daily, month, cur_year, prev_year, df_classified):
    print()
    print('=' * 80)
    print('八、关键发现与趋势总结')
    print('=' * 80)

    m_cur = month[month['year'] == cur_year].set_index('month')
    cur_months = sorted(m_cur.index)

    # Q1 comparison — 只比较两年都有的月份
    if prev_year:
        m_prev = month[month['year'] == prev_year].set_index('month')
        q1_months = [m for m in range(1, 4) if m in m_cur.index and m in m_prev.index]
        if q1_months:
            q1_cur = sum(int(m_cur.loc[m, '住宅签约套数']) for m in q1_months)
            q1_prev = sum(int(m_prev.loc[m, '住宅签约套数']) for m in q1_months)
            print(f'\n  1. Q1整体: {cur_year}年 {q1_cur:,} 套, {prev_year}年 {q1_prev:,} 套, 同比 {fmt_pct(pct_change(q1_cur, q1_prev))}')

        for m in cur_months:
            if m in m_prev.index:
                cur_v = int(m_cur.loc[m, '住宅签约套数'])
                prev_v = int(m_prev.loc[m, '住宅签约套数'])
                print(f'     {m}月: {cur_v:,} vs {prev_v:,} → {fmt_pct(pct_change(cur_v, prev_v))}')

    # Best/worst month
    if len(cur_months) > 1:
        best_m = max(cur_months, key=lambda m: int(m_cur.loc[m, '住宅签约套数']))
        worst_m = min(cur_months, key=lambda m: int(m_cur.loc[m, '住宅签约套数']))
        print(f'\n  2. 年内最高月: {best_m}月 ({int(m_cur.loc[best_m, "住宅签约套数"]):,} 套)')
        print(f'     年内最低月: {worst_m}月 ({int(m_cur.loc[worst_m, "住宅签约套数"]):,} 套)')

    # Workday trend
    wd = df_classified[df_classified['day_type'] == '工作日']
    wd_trend = wd.groupby('year_month')['住宅签约套数'].mean()
    if len(wd_trend) >= 2:
        direction = '上升' if wd_trend.iloc[-1] > wd_trend.iloc[-2] else '下降'
        print(f'\n  3. 工作日日均趋势: {direction} ({wd_trend.iloc[-2]:.0f} → {wd_trend.iloc[-1]:.0f})')

    # Peak day
    if len(df_classified) > 0:
        peak = df_classified.loc[df_classified['住宅签约套数'].idxmax()]
        print(f'\n  4. 单日最高: {peak["日期"].strftime("%Y-%m-%d")} ({int(peak["住宅签约套数"])} 套)')

    # Latest week vs previous week
    df2 = daily[daily['year'] == cur_year].copy()
    df2['yw'] = df2['日期'].dt.strftime('%G-W%V')
    weekly = df2.groupby('yw')['住宅签约套数'].sum().sort_index()
    if len(weekly) >= 2:
        print(f'\n  5. 最近两周: {weekly.iloc[-2]:,} → {weekly.iloc[-1]:,} ({fmt_pct(pct_change(weekly.iloc[-1], weekly.iloc[-2]))})')


def parse_args():
    parser = argparse.ArgumentParser(
        description='北京二手住宅成交量分析报告',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='示例:\n'
               '  python script/analyze.py\n'
               '  python script/analyze.py --data-dir /path/to/data',
    )
    parser.add_argument(
        '--data-dir', default=DEFAULT_DATA_DIR,
        help=f'数据目录路径 (默认: {DEFAULT_DATA_DIR})',
    )
    return parser.parse_args()


def main():
    args = parse_args()
    data_dir = args.data_dir

    if not os.path.isdir(data_dir):
        print(f'错误: 数据目录不存在: {data_dir}', file=sys.stderr)
        sys.exit(1)

    print(f'数据目录: {os.path.abspath(data_dir)}')
    print()

    daily, month, district, area = load_data(data_dir)

    cur_year = daily['year'].max()
    m_years = sorted(month['year'].unique())
    prev_year = m_years[-2] if len(m_years) > 1 and m_years[-2] < cur_year else None
    date_range = f'{daily["日期"].min().strftime("%Y-%m-%d")} ~ {daily["日期"].max().strftime("%Y-%m-%d")}'

    # 统一做一次 classify_day_type，各 section 复用
    df_classified = classify_day_type(daily[daily['year'] == cur_year])

    print('=' * 80)
    print(f'北京二手住宅成交量分析报告')
    print(f'数据范围: {date_range}')
    print('=' * 80)

    section_monthly_overview(month, cur_year, prev_year)
    section_weekly(daily, cur_year)
    section_day_type(df_classified)
    section_workday_trend(df_classified)
    section_yoy_daily(daily, cur_year, prev_year)
    section_district(district)
    section_area(area)
    section_summary(daily, month, cur_year, prev_year, df_classified)

    print()
    print('=' * 80)
    print('报告结束')
    print('=' * 80)


if __name__ == '__main__':
    main()
