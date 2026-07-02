# -*- coding: utf-8 -*-
"""趋势图表（matplotlib）。

8 张图：全市月度趋势、季节性热力、套均面积、各区堆叠/份额/small-multiples、
面积段与价格段结构。中文字体配置、Agg 后端（无显示环境可用）。
对短序列（<5 个月）自动改用柱状图而非面积图。
"""
import os
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

import config

# 中文字体 + 负号
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['figure.dpi'] = 100
plt.rcParams['savefig.dpi'] = 110
plt.rcParams['axes.grid'] = True
plt.rcParams['grid.alpha'] = 0.3

# 区县配色（暖→冷渐变，足够区分 17 个区）
DISTRICT_CMAP = plt.cm.tab20.colors


def _save(fig, name):
    os.makedirs(config.REPORT_DIR, exist_ok=True)
    path = os.path.join(config.REPORT_DIR, name)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return name


def _str_index(wide):
    """把宽表 PeriodIndex 转成 '25-12' 字符串 index。

    必须在 plot.area/plot.bar 之前调用：否则 pandas 用 Period 序数（如 671）
    做 x 坐标，与 set_xticks(range(n)) 错位，月份标签全乱。
    """
    out = wide.copy()
    out.index = _period_labels(out.index.tolist())
    return out


def _period_labels(periods):
    """Period[M] 列表 → '25-01' 风格标签。"""
    return [f"{str(p)[2:5]}-{str(p)[5:7]}" for p in periods]


# ---------------------------------------------------------------- chart 1
def city_trend(monthly_ma, out='city_trend.png'):
    """全市月度住宅网签：折线 + 12月移动均值 + 同比柱（双轴）。"""
    df = monthly_ma.dropna(subset=['住宅签约套数']).sort_values('period')
    labels = _period_labels(df['period'].tolist())
    x = np.arange(len(df))
    units = df['住宅签约套数'].values
    yoy = df['同比'].values

    fig, ax1 = plt.subplots(figsize=(11, 5))
    ax1.bar(x, units, color='#9ecae1', alpha=0.55, label='月度网签套数')
    ax1.plot(x, units, color='#08519c', marker='o', lw=1.5, label='套数')
    if 'MA12' in df.columns:
        ma = df['MA12'].values
        m = ~np.isnan(ma)
        if m.any():
            ax1.plot(x[m], ma[m], color='#de2d26', lw=2.4, label='12月移动均值')
    ax1.set_ylabel('住宅网签套数', color='#08519c')
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, rotation=45, ha='right')
    ax1.set_title('北京二手住宅 月度网签量趋势', fontsize=14, fontweight='bold')

    ax2 = ax1.twinx()
    valid_yoy = ~np.isnan(yoy)
    colors = ['#31a354' if v >= 0 else '#e6550d' for v in np.where(valid_yoy, yoy, 0)]
    ax2.bar(x[valid_yoy], yoy[valid_yoy], color=[c for c, v in zip(colors, valid_yoy) if v],
            alpha=0.35, width=0.5, label='同比')
    ax2.set_ylabel('同比 %', color='#555')
    ax2.axhline(0, color='#888', lw=0.8)

    h1, l1 = ax1.get_legend_handles_labels()
    ax1.legend(h1, l1, loc='upper left', fontsize=9)
    return _save(fig, out)


# ---------------------------------------------------------------- chart 2
def seasonality_heatmap(monthly, out='seasonality.png'):
    """年×月 热力图（色=住宅网签套数），看季节性与同比。"""
    df = monthly.copy()
    pivot = df.pivot_table(index='year', columns='month', values='住宅签约套数', aggfunc='sum')
    pivot = pivot.reindex(columns=range(1, 13))
    n_years = len(pivot.index)
    fig, ax = plt.subplots(figsize=(11, max(2.4, 0.9 * n_years + 1.0)))
    cmap = LinearSegmentedColormap.from_list('blues', ['#f7fbff', '#08306b'])
    im = ax.imshow(pivot.values, aspect='auto', cmap=cmap)
    ax.set_xticks(range(12))
    ax.set_xticklabels([f'{m}月' for m in range(1, 13)])
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    # 标注数值
    for i in range(pivot.shape[0]):
        for j in range(pivot.shape[1]):
            v = pivot.values[i, j]
            if not np.isnan(v):
                ax.text(j, i, f'{int(v):,}', ha='center', va='center',
                        fontsize=8, color='white' if v > np.nanmax(pivot.values) * 0.6 else '#222')
    ax.set_title('月度网签量季节性热力图（年×月）', fontsize=13, fontweight='bold')
    fig.colorbar(im, ax=ax, label='套数', shrink=0.7)
    return _save(fig, out)


# ---------------------------------------------------------------- chart 3
def avg_area_trend(aua, out='avg_unit_area.png'):
    """套均面积趋势（改善化/刚需化）。"""
    df = aua.sort_values('period')
    labels = _period_labels(df['period'].tolist())
    x = np.arange(len(df))
    fig, ax = plt.subplots(figsize=(11, 4))
    ax.plot(x, df['套均面积'].values, color='#6a51a3', marker='o', lw=2)
    z = np.polyfit(x, df['套均面积'].values, 1)
    ax.plot(x, np.poly1d(z)(x), '--', color='#e7298a', lw=1.5, label='线性趋势')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.set_ylabel('套均面积 (m²)')
    ax.set_title('套均成交面积趋势（数值↑=改善化，↓=刚需化）', fontsize=13, fontweight='bold')
    ax.legend()
    return _save(fig, out)


# ---------------------------------------------------------------- charts 4/5/6
def _district_wide(district):
    """区县 → 宽表 [period × 区县_clean]，剔除全市。"""
    d = district[district['区县_clean'] != '全市'].copy()
    wide = d.pivot_table(index='period', columns='区县_clean', values='签约套数', aggfunc='sum').fillna(0)
    # 按总量排序区县
    order = wide.sum().sort_values(ascending=False).index
    return wide[order]


def _district_topn(district, n=8):
    """Top-N 区县宽表，其余区合并为"其他"列。改善 17 区堆叠不可读的问题。"""
    wide = _district_wide(district)
    if wide.empty or wide.shape[1] <= n:
        return wide
    top = wide.iloc[:, :n].copy()
    top['其他'] = wide.iloc[:, n:].sum(axis=1)
    return top


def district_stacked(district, out='district_stacked.png'):
    """各区成交套数堆叠面积图（Top 8 + 其他）。"""
    wide = _str_index(_district_topn(district, 8))
    if wide.empty:
        return None
    fig, ax = plt.subplots(figsize=(11, 5.5))
    wide.plot.area(ax=ax, color=DISTRICT_CMAP, alpha=0.85, legend=False)
    ax.set_xticks(range(len(wide.index)))
    ax.set_xticklabels(wide.index.tolist(), rotation=0)
    ax.set_ylabel('网签套数')
    ax.set_title('各区二手住宅网签量（Top 8 + 其他）', fontsize=13, fontweight='bold')
    ax.legend(loc='center left', bbox_to_anchor=(1.01, 0.5), fontsize=8, ncol=1)
    return _save(fig, out)


def district_share(district, out='district_share.png'):
    """各区份额 100% 堆叠面积图（Top 8 + 其他）。"""
    wide = _str_index(_district_topn(district, 8))
    if wide.empty:
        return None
    pct = wide.div(wide.sum(axis=1), axis=0) * 100
    fig, ax = plt.subplots(figsize=(11, 5.5))
    pct.plot.area(ax=ax, color=DISTRICT_CMAP, alpha=0.85, legend=False)
    ax.set_ylim(0, 100)
    ax.set_xticks(range(len(pct.index)))
    ax.set_xticklabels(pct.index.tolist(), rotation=0)
    ax.set_ylabel('份额 %')
    ax.set_title('各区网签量份额（Top 8 + 其他，100% 堆叠）', fontsize=13, fontweight='bold')
    ax.legend(loc='center left', bbox_to_anchor=(1.01, 0.5), fontsize=8, ncol=1)
    return _save(fig, out)


def district_small_multiples(district, out='district_smallmul.png'):
    """各区各自趋势（small multiples），可比。"""
    wide = _district_wide(district)
    if wide.empty:
        return None
    n = len(wide.columns)
    ncols = 5
    nrows = int(np.ceil(n / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(13, 2.2 * nrows), sharex=True)
    axes = np.array(axes).reshape(-1)
    labels = _period_labels(wide.index.tolist())
    for i, col in enumerate(wide.columns):
        ax = axes[i]
        ax.plot(range(len(wide)), wide[col].values, color='#08519c', marker='o', lw=1.5)
        ax.set_title(col, fontsize=9)
        ax.tick_params(labelsize=7)
        ax.grid(alpha=0.3)
    for j in range(i + 1, len(axes)):
        axes[j].axis('off')
    # 最下一行加 x 标签
    for k in range(ncols):
        idx = (nrows - 1) * ncols + k
        if idx < len(axes) and axes[idx].axis:
            axes[idx].set_xticks(range(len(labels)))
            axes[idx].set_xticklabels(labels, rotation=45, ha='right', fontsize=7)
    fig.suptitle('各区网签量走势（各自坐标）', fontsize=13, fontweight='bold', y=1.0)
    return _save(fig, out)


# ---------------------------------------------------------------- charts 7/8
def _area_pivots(area):
    """返回 (pivot_cnt, pivot_sh, col_labels)：面积段×月份 的成交套数与占比。"""
    order = ['60m2以下', '60～80m2', '80～100m2', '100～120m2', '120～140m2', '140m2以上']
    df = area.copy()
    df['share'] = df['成交套数'] / df.groupby('period')['成交套数'].transform('sum') * 100
    pivot_cnt = df.pivot_table(index='面积区间', columns='period', values='成交套数', aggfunc='sum')
    pivot_cnt = pivot_cnt.reindex([s for s in order if s in pivot_cnt.index]).reindex(sorted(pivot_cnt.columns), axis=1)
    pivot_sh = df.pivot_table(index='面积区间', columns='period', values='share', aggfunc='sum')
    pivot_sh = pivot_sh.reindex(pivot_cnt.index).reindex(pivot_cnt.columns, axis=1)
    return pivot_cnt, pivot_sh, _period_labels(pivot_cnt.columns.tolist())


def area_segment_count(area, out='area_segment_count.png'):
    """各面积段 × 月份 成交「套数」热力图（单一维度：只看绝对量）。"""
    if area is None or area.empty:
        return None
    pivot_cnt, _, col_labels = _area_pivots(area)
    nrow, ncol = pivot_cnt.shape
    fig, ax = plt.subplots(figsize=(max(5.5, 1.9 * ncol + 2.2), max(3.2, 0.8 * nrow + 1.6)))
    cmap = LinearSegmentedColormap.from_list('blues', ['#f7fbff', '#08519c'])
    im = ax.imshow(pivot_cnt.values, aspect='auto', cmap=cmap, vmin=0)
    ax.set_xticks(range(ncol)); ax.set_xticklabels(col_labels, rotation=0)
    ax.set_yticks(range(nrow)); ax.set_yticklabels(pivot_cnt.index)
    vmax = np.nanmax(pivot_cnt.values)
    for i in range(nrow):
        for j in range(ncol):
            v = pivot_cnt.values[i, j]
            if not np.isnan(v):
                ax.text(j, i, f'{int(v):,}', ha='center', va='center', fontsize=10,
                        color='white' if v > vmax * 0.6 else '#222')
    ax.set_title('各面积段成交套数（月度）', fontsize=13, fontweight='bold')
    fig.colorbar(im, ax=ax, label='套数', shrink=0.8)
    return _save(fig, out)


def area_segment_share(area, out='area_segment_share.png'):
    """各面积段 × 月份 成交「占比%」热力图（单一维度：只看结构迁移）。"""
    if area is None or area.empty:
        return None
    _, pivot_sh, col_labels = _area_pivots(area)
    nrow, ncol = pivot_sh.shape
    fig, ax = plt.subplots(figsize=(max(5.5, 1.9 * ncol + 2.2), max(3.2, 0.8 * nrow + 1.6)))
    cmap = LinearSegmentedColormap.from_list('blues', ['#f7fbff', '#08519c'])
    im = ax.imshow(pivot_sh.values, aspect='auto', cmap=cmap, vmin=0)
    ax.set_xticks(range(ncol)); ax.set_xticklabels(col_labels, rotation=0)
    ax.set_yticks(range(nrow)); ax.set_yticklabels(pivot_sh.index)
    vmax = np.nanmax(pivot_sh.values)
    for i in range(nrow):
        for j in range(ncol):
            v = pivot_sh.values[i, j]
            if not np.isnan(v):
                ax.text(j, i, f'{v:.1f}%', ha='center', va='center', fontsize=10,
                        color='white' if v > vmax * 0.55 else '#222')
    ax.set_title('各面积段成交占比（月度，颜色=占比%）', fontsize=13, fontweight='bold')
    fig.colorbar(im, ax=ax, label='占比 %', shrink=0.8)
    return _save(fig, out)


# ---------------------------------------------------------------- chart 9
def weekly_trend(weekly, out='weekly_trend.png'):
    """周度成交趋势：套数柱 + 4周移动均值线 + 面积线（双轴）。"""
    if weekly is None or weekly.empty:
        return None
    wk = weekly.sort_values('week_start').reset_index(drop=True)
    x = np.arange(len(wk))
    fig, ax1 = plt.subplots(figsize=(12, 5))
    # 标注天数不足 7 的周（柱用浅色）
    partial = wk['days'] < 7
    colors = ['#fdd0a2' if p else '#9ecae1' for p in partial]
    ax1.bar(x, wk['套数'], color=colors, alpha=0.8, label='周度套数（橙=该周天数<7）')
    ax1.plot(x, wk['MA4'], color='#de2d26', lw=2.2, label='4周移动均值')
    ax1.set_ylabel('住宅网签套数', color='#08519c')
    ax1.set_title('北京二手住宅 周度成交趋势', fontsize=14, fontweight='bold')

    ax2 = ax1.twinx()
    ax2.plot(x, wk['面积'] / 1e4, color='#31a354', lw=1.3, alpha=0.85, label='周度面积(万m²)')
    ax2.set_ylabel('住宅网签面积 (万m²)', color='#31a354')

    step = max(1, len(wk) // 12)
    ax1.set_xticks(x[::step])
    ax1.set_xticklabels([d.strftime('%m/%d') for d in wk['week_start'][::step]],
                        rotation=45, ha='right', fontsize=8)

    h1, l1 = ax1.get_legend_handles_labels()
    ax1.legend(h1, l1, loc='upper left', fontsize=9)
    return _save(fig, out)


# ---------------------------------------------------------------- chart 10
def weekly_avg_area(weekly, out='weekly_avg_area.png'):
    """周度套均面积连续趋势（住宅面积/住宅套数）。

    面积段分月仅少数点，套均面积可从日数据算出连续周序列，是当下最可靠的
    "成交面积结构走向"代理：数值↑=改善化、↓=刚需化。
    """
    if weekly is None or weekly.empty:
        return None
    wk = weekly.sort_values('week_start').reset_index(drop=True)
    # 只画满 6 天以上的周（避免不完整周噪声）
    full = wk[wk['days'] >= 6].copy()
    if full.empty:
        return None
    x = np.arange(len(full))
    fig, ax = plt.subplots(figsize=(12, 4.2))
    ax.plot(x, full['套均面积'].values, color='#6a51a3', marker='o', markersize=3, lw=1.4, alpha=0.8)
    full = full.copy()
    full['MA8'] = full['套均面积'].rolling(8, min_periods=3).mean()
    ax.plot(x, full['MA8'].values, color='#e7298a', lw=2.4, label='8周移动均值')
    z = np.polyfit(x, full['套均面积'].values, 1)
    ax.plot(x, np.poly1d(z)(x), '--', color='#999', lw=1.3, label='线性趋势')
    step = max(1, len(full) // 12)
    ax.set_xticks(x[::step])
    ax.set_xticklabels([d.strftime('%m/%d') for d in full['week_start'][::step]],
                       rotation=45, ha='right', fontsize=8)
    ax.set_ylabel('套均面积 (m²)')
    ax.set_title('周度套均成交面积趋势（↑改善化 / ↓刚需化）', fontsize=13, fontweight='bold')
    ax.legend(loc='upper left', fontsize=9)
    return _save(fig, out)
