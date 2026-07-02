# -*- coding: utf-8 -*-
"""趋势报告组装：生成 report/trend_report.md + PNG 图表。

全部基于官方自爬数据。第三方媒体锚点仅作"参考"附录，明确标注非自爬。
无预测——只呈现历史趋势。
"""
import os
import pandas as pd
import numpy as np

import config
from . import load, metrics, plots


# 经 2025 口径验证的媒体锚点（非自爬，仅参考）
# 2025 H1 = 88575 与本项目住宅签约套数加总精确吻合，口径一致
MEDIA_H1_ANCHORS = [
    # (年份, 上半年二手住宅网签套数, 来源类型)
    (2022, 69754),
    (2023, 84332),
    (2024, 74780),
    (2025, 88575),
]


def _fmt(v, spec=',.0f'):
    if pd.isna(v):
        return '—'
    return format(v, spec)


def _pct(v):
    if pd.isna(v):
        return '—'
    return f'{v:+.1f}%'


def _table(headers, rows):
    line1 = '| ' + ' | '.join(headers) + ' |'
    line2 = '| ' + ' | '.join(['---'] * len(headers)) + ' |'
    body = '\n'.join('| ' + ' | '.join(str(c) for c in r) + ' |' for r in rows)
    return '\n'.join([line1, line2, body])


def generate():
    # 加载数据
    monthly = load.load_monthly()
    annual = load.load_annual()
    district = load.load_district()
    area = load.load_area()
    price = load.load_price()
    daily = load.load_daily()

    if monthly.empty:
        raise RuntimeError('无月度数据，无法生成报告')

    os.makedirs(config.REPORT_DIR, exist_ok=True)

    # 指标
    monthly_yoy = metrics.add_yoy_mom(monthly)
    monthly_ma = metrics.add_moving_avg(monthly_yoy, windows=(12,))
    aua = metrics.avg_unit_area(monthly)
    rank = metrics.district_rank_change(district) if not district.empty else pd.DataFrame()
    weekly = metrics.weekly_aggregate(daily) if not daily.empty else pd.DataFrame()

    # 画图
    imgs = {}
    imgs['city'] = plots.city_trend(monthly_ma)
    imgs['season'] = plots.seasonality_heatmap(monthly)
    imgs['avgarea'] = plots.avg_area_trend(aua)
    imgs['weekly'] = plots.weekly_trend(weekly)
    imgs['weekly_aua'] = plots.weekly_avg_area(weekly)
    imgs['d_stack'] = plots.district_stacked(district)
    imgs['d_share'] = plots.district_share(district)
    imgs['d_sm'] = plots.district_small_multiples(district)
    imgs['area_cnt'] = plots.area_segment_count(area)
    imgs['area_sh'] = plots.area_segment_share(area)

    # 摘要数字
    cur = monthly.iloc[-1]
    prev = monthly.iloc[-2] if len(monthly) > 1 else None
    first = monthly.iloc[0]
    span = f"{first['period']} ~ {cur['period']}"
    cur_units = int(cur['住宅签约套数'])
    mom = monthly_yoy.iloc[-1]['环比']
    yoy = monthly_yoy.iloc[-1]['同比']
    peak_idx = monthly['住宅签约套数'].idxmax()
    peak = monthly.loc[peak_idx]
    trough_idx = monthly['住宅签约套数'].idxmin()
    trough = monthly.loc[trough_idx]
    avg_area_latest = aua.iloc[-1]['套均面积'] if not aua.empty else np.nan
    avg_area_first = aua.iloc[0]['套均面积'] if not aua.empty else np.nan

    md = []
    md.append('# 北京二手住宅成交趋势报告\n')
    md.append(f'> 数据范围：{span}（共 {len(monthly)} 个月）· 数据来源：北京市住建委（官方自爬）· 仅历史趋势，不含预测\n')
    md.append('---\n')

    # 摘要
    md.append('## 一、核心摘要\n')
    md.append(f'- **最新月份 {cur['period']}**：住宅网签 **{cur_units:,} 套**'
              f'，环比 {_pct(mom)}，同比 {_pct(yoy)}。\n')
    md.append(f'- **区间峰值**：{peak['period']}（{int(peak["住宅签约套数"]):,} 套）'
              f'　**区间谷值**：{trough['period']}（{int(trough["住宅签约套数"]):,} 套）。\n')
    if not np.isnan(avg_area_latest) and not np.isnan(avg_area_first):
        direction = '上升（趋于改善）' if avg_area_latest > avg_area_first else '下降（趋于刚需）'
        md.append(f'- **套均面积**：{avg_area_first:.1f} → {avg_area_latest:.1f} m²，{direction}。\n')
    md.append(f'- **年度长周期**（二手住宅，万套）：'
              + '、'.join(f"{int(r['年份'])}={r['住宅套数万']:.2f}" for _, r in annual.iterrows()) + '。\n')

    # 全市趋势
    md.append('\n## 二、全市成交趋势\n')
    md.append(f'![全市月度趋势]({imgs["city"]})\n')
    md.append('\n*月度网签套数 + 12 月移动均值 + 同比。移动均值剥离月度噪声看大方向。*\n')
    md.append(f'\n![季节性]({imgs["season"]})\n')
    md.append('\n*年×月热力图。注意 2 月（春节）通常塌量、3-4 月及年末冲量——属季节性，勿误读为趋势拐点。*\n')
    md.append(f'\n![套均面积]({imgs["avgarea"]})\n')

    md.append('\n### 月度明细\n')
    rows = []
    for _, r in monthly_yoy.iloc[::-1].iterrows():
        rows.append([str(r['period']), f"{int(r['住宅签约套数']):,}",
                     _pct(r['环比']), _pct(r['同比']),
                     f"{r['住宅签约面积(m2)']:,.0f}",
                     f"{r['住宅签约面积(m2)']/r['住宅签约套数']:.1f}" if r['住宅签约套数'] else '—'])
    md.append(_table(['年月', '住宅网签(套)', '环比', '同比', '住宅面积(m²)', '套均(m²)'], rows))
    md.append('')

    # 周度趋势
    md.append('\n## 三、周度成交趋势\n')
    if not weekly.empty and imgs.get('weekly'):
        n_wk = len(weekly)
        partial_n = int((weekly['days'] < 7).sum())
        wk_latest = weekly.iloc[-1]
        wk_prev = weekly.iloc[-2] if len(weekly) > 1 else None
        wk_mom = ((wk_latest['套数'] - wk_prev['套数']) / wk_prev['套数'] * 100) if wk_prev is not None else np.nan
        md.append(f'> 基于 {n_wk} 个自然周（{weekly["week_start"].iloc[0].date()} ~ '
                  f'{weekly["week_start"].iloc[-1].date()}）。周度粒度比月度更能反映短期动能。\n')
        md.append(f'\n![周度趋势]({imgs["weekly"]})\n')
        md.append('\n*柱=周度套数，红线=4 周移动均值，绿线=周度面积（右轴）。'
                  f'橙色柱表示该周天数 <7（节假日/抓取缺口），数值偏低（共 {partial_n} 周）。*\n')
        md.append('\n### 近 10 周\n')
        wrows = []
        for _, r in weekly.iloc[::-1].head(10).iterrows():
            mark = ' ⚠️' if r['days'] < 7 else ''
            wrows.append([r['week_start'].strftime('%Y-%m-%d'), f"{int(r['套数']):,}",
                          f"{r['面积']:,.0f}", f"{r['套均面积']:.1f}", f"{int(r['days'])}{mark}"])
        md.append(_table(['周起始(周一)', '住宅网签(套)', '住宅面积(m²)', '套均(m²)', '天数'], wrows))
        md.append(f'\n*最新周 {wk_latest["week_start"].date()}：{int(wk_latest["套数"]):,} 套 / '
                  f'{wk_latest["面积"]:,.0f} m²，环比 {_pct(wk_mom)}。*\n')

        # 周度套均面积连续趋势（面积结构走向的代理信号）
        if imgs.get('weekly_aua'):
            full_wk = weekly[weekly['days'] >= 6]
            aua_dir = '—'
            if len(full_wk) >= 2:
                first_aua, last_aua = full_wk['套均面积'].iloc[0], full_wk['套均面积'].iloc[-1]
                aua_dir = '上升（趋于改善）' if last_aua > first_aua else '下降（趋于刚需）'
            md.append(f'\n![周度套均面积]({imgs["weekly_aua"]})\n')
            md.append('\n*周度套均面积（仅画满 ≥6 天的周）。面积段分月只有少数点，'
                      '套均面积可连续观察"成交往大还是往小走"。'
                      f'区间走势：{aua_dir}。*\n')
    else:
        md.append('无可用日数据，跳过周度分析。\n')

    # 区域格局
    md.append('\n## 四、区域格局\n')
    n_dist_months = district['period'].nunique() if not district.empty else 0
    md.append(f'> 区县月度数据仅 {n_dist_months} 个月（{district["period"].min() if not district.empty else "—"} ~ '
              f'{district["period"].max() if not district.empty else "—"}），呈现短期格局，趋势需更长序列确认。\n')
    if imgs.get('d_stack'):
        md.append(f'\n![各区堆叠]({imgs["d_stack"]})\n')
    if imgs.get('d_share'):
        md.append(f'\n![各区份额]({imgs["d_share"]})\n')
    if imgs.get('d_sm'):
        md.append(f'\n![各区走势]({imgs["d_sm"]})\n')

    if not rank.empty:
        md.append('\n### 各区排名与变化\n')
        rrows = []
        for name, r in rank.iterrows():
            chg = f"{int(r['排名变化']):+d}" if r['排名变化'] != 0 else '0'
            rrows.append([name, f"{int(r['末月套数']):,}", int(r['末月排名']),
                          int(r['首月排名']), chg, f"{r['份额变化_pp']:+.2f}pp"])
        md.append(_table(['区县', '最新套数', '最新排名', '首月排名', '排名变化', '份额变化'], rrows))
        md.append('')

    # 市场结构
    md.append('\n## 五、市场结构\n')
    if not area.empty:
        area_months = area['period'].nunique()
        md.append('### 面积段成交量（月度）\n')
        if imgs.get('area_cnt'):
            md.append(f'![面积段成交量]({imgs["area_cnt"]})\n')
        md.append('\n### 面积段成交占比（月度）\n')
        if imgs.get('area_sh'):
            md.append(f'![面积段占比]({imgs["area_sh"]})\n')
        md.append(f'\n*占比视角剥离总量波动（如 2 月春节全线塌量），更能看出结构迁移。'
                  f'当前 {area_months} 个月（2026-04 不可补、缺失），序列不连续。*\n')

        # 各面积段 × 月份 占比表 + 占比变化(pp)
        order = ['60m2以下', '60～80m2', '80～100m2', '100～120m2', '120～140m2', '140m2以上']
        months = sorted(area['period'].unique())
        df = area.copy()
        df['share'] = df['成交套数'] / df.groupby('period')['成交套数'].transform('sum') * 100
        pv_cnt = df.pivot_table(index='面积区间', columns='period', values='成交套数', aggfunc='sum')
        pv_sh = df.pivot_table(index='面积区间', columns='period', values='share', aggfunc='sum')
        pv_cnt = pv_cnt.reindex([s for s in order if s in pv_cnt.index])
        pv_sh = pv_sh.reindex(pv_cnt.index)
        md.append('\n### 各面积段月度成交（套数 / 占比%）\n')
        header = ['面积区间'] + [str(m) for m in months] + ['占比变化(首→最新)']
        rows = []
        first_m, last_m = months[0], months[-1]
        for seg in pv_cnt.index:
            cells = []
            for m in months:
                c, s = pv_cnt.loc[seg, m], pv_sh.loc[seg, m]
                cells.append(f"{int(c):,} ({s:.1f}%)" if not pd.isna(c) else '—')
            pp = '—'
            sf, sl = pv_sh.loc[seg, first_m], pv_sh.loc[seg, last_m]
            if not pd.isna(sf) and not pd.isna(sl):
                pp = f"{sl - sf:+.2f}pp"
            rows.append([seg] + cells + [pp])
        md.append(_table(header, rows))
        md.append('\n*占比变化为百分点(pp)。正值=该面积段份额上升，负值=下降。*\n')

    # 数据说明 + 参考锚点
    md.append('\n## 六、数据说明\n')
    md.append('- **主数据**：全部来自北京市住建委（pageId=307749）官方自爬，经完整性校验（面积/价格段加总=全市）。\n')
    md.append(f'- **月度覆盖**：{span}（{len(monthly)} 个月）。区县 {n_dist_months} 个月。'
              f'面积段 {area["period"].nunique() if not area.empty else 0} 个月（序列不连续）。\n')
    md.append('- **面积段 2026-04 永久缺失**：2026-03 已从 git 历史快照恢复；但 2026-04 的各面积段明细'
              '因解析器时间窗口 bug 永久丢失（4 月数据 5 月才上线，恰在解析器改版失效之后），'
              '官方无回溯、日数据无面积段拆分，无法恢复。全市总量（19784 套）仍可在区县数据中查到。\n')
    md.append('- **价格段**：官方近月仅发布成交数据（发布数据全为占位）、且仅 3 个月，不足以呈现趋势，已从报告中移除。\n')
    md.append('- **不含预测**：本报告仅呈现历史趋势，不预测未来。\n')
    md.append('- **历史数据**：官方无历史月度回溯接口；第三方历史因口径/可靠性未纳入主序列。\n')
    md.append('\n### 参考：上半年网签量长周期（媒体引用·非自爬）\n')
    md.append('> 经 2025 年验证口径与官方一致（媒体 88575 = 本项目自爬加总）。'
              '2022-2024 为媒体引用，仅作长周期参考，不参与主趋势计算。\n\n')
    arows = [[y, f'{v:,}'] for y, v in MEDIA_H1_ANCHORS]
    md.append(_table(['年份', '上半年二手住宅网签(套)'], arows))
    md.append('')

    out = os.path.join(config.REPORT_DIR, 'trend_report.md')
    with open(out, 'w', encoding='utf-8') as f:
        f.write('\n'.join(md))
    return out
