# -*- coding: utf-8 -*-
"""趋势报告组装：生成 report/trend_report.md + PNG 图表。

全部基于官方自爬数据，不引用任何第三方/媒体数据。
无预测——只呈现历史趋势。
"""
import json
import os
import pandas as pd
import numpy as np

import config
import quality
from . import load, metrics, charts


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
    new_daily = load.load_new_daily()
    agency = load.load_agency()

    # 覆盖月数（metrics.coverage_counts 为 report 与 ai_digest 共用实现）
    n_months, n_dist_months, n_area_months = metrics.coverage_counts(monthly, district, area)

    if monthly.empty:
        raise RuntimeError('无月度数据，无法生成报告')

    os.makedirs(config.REPORT_DIR, exist_ok=True)

    # 干净房间（code-review 抓获）：先清本次将重生成的图表产物——若页面改版
    # 致某 prep 返回 None，旧 PNG/JSON 不得冒充新产物混过 verify 门（fail-closed）
    charts_dir = os.path.join(config.REPORT_DIR, 'charts')
    if os.path.isdir(charts_dir):
        for fn in os.listdir(charts_dir):
            if fn.endswith('.json'):
                os.remove(os.path.join(charts_dir, fn))
    for fn in os.listdir(config.REPORT_DIR):
        if fn.endswith('.png'):
            os.remove(os.path.join(config.REPORT_DIR, fn))

    # 指标
    monthly_yoy = metrics.add_yoy_mom(monthly)
    monthly_ma = metrics.add_moving_avg(monthly_yoy, windows=(12,))
    aua = metrics.avg_unit_area(monthly)
    rank = metrics.district_rank_change(district) if not district.empty else pd.DataFrame()
    weekly = metrics.weekly_aggregate(daily) if not daily.empty else pd.DataFrame()

    # 画图（charts 引擎：PNG 与 charts/<stem>.json 同 stem 成对产出，T1.4b）
    def _chart(spec):
        return charts.write_chart(spec)['png'] if spec else None

    imgs = {}
    imgs['city'] = _chart(charts.prep_city_trend(monthly_ma))
    imgs['season'] = _chart(charts.prep_seasonality(monthly))
    imgs['avgarea'] = _chart(charts.prep_avg_unit_area(aua))
    imgs['weekly'] = _chart(charts.prep_weekly_trend(weekly))
    imgs['weekly_aua'] = _chart(charts.prep_weekly_avg_area(weekly))
    imgs['d_stack'] = _chart(charts.prep_district_stacked(district))
    imgs['d_share'] = _chart(charts.prep_district_share(district))
    imgs['d_sm'] = _chart(charts.prep_district_smallmul(district))
    imgs['area_cnt'] = _chart(charts.prep_area_segment_count(area))
    imgs['area_sh'] = _chart(charts.prep_area_segment_share(area))
    # R5 累计与非住宅结构（T2.6）：TTM/YTD 基于消毒后的月度（2025-01 面积坏值→整窗 NaN）
    monthly_s = metrics.sanitize_bad_values(monthly_yoy)
    m_cum = metrics.add_ytd(metrics.add_ttm(monthly_s))
    imgs['cum'] = _chart(charts.prep_cum_view(m_cum))
    imgs['share'] = _chart(charts.prep_structure_share(metrics.nonresidential_share(monthly_s)))
    # M3 R1/R2（用户 2026-10-27 确认口径）：新房周度三件套 + 区县环比热力
    new_wk = metrics.new_home_weekly(new_daily)
    inv_df = metrics.new_inventory(new_daily)
    imgs['new_wk'] = _chart(charts.prep_new_weekly(new_wk))
    imgs['resale_new'] = _chart(charts.prep_resale_vs_new(weekly, new_wk))
    imgs['new_inv'] = _chart(charts.prep_new_inventory(inv_df))
    imgs['d_mom'] = _chart(charts.prep_district_mom(metrics.district_mom(district)))
    # M3 R4/R3：五年锚点 + 机构板块
    imgs['annual'] = _chart(charts.prep_annual_anchor(metrics.annual_anchor(monthly, annual)))
    agency_top10 = metrics.agency_top(agency)
    imgs['agency_top'] = _chart(charts.prep_agency_top(agency_top10))
    imgs['refund'] = _chart(charts.prep_refund_rate(metrics.agency_refund(agency)))

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

    # KPI 数据（T2.3）：html_render 静态卡片渲染 + verify 门直比；NaN→None（JSON 合法）
    mtd = None
    if not daily.empty:
        last_ym = daily['日期'].max().strftime('%Y-%m')
        dm = daily[daily['日期'].dt.strftime('%Y-%m') == last_ym]
        if not dm.empty:
            mtd = {'period': last_ym, 'units': int(dm['住宅签约套数'].sum()),
                   'days': int(len(dm)), 'note': '当月至今·非全月'}
    kpi = {
        'latest': {
            'period': str(cur['period']), 'units': int(cur_units),
            'mom': None if pd.isna(mom) else round(float(mom), 1),
            'yoy': None if pd.isna(yoy) else round(float(yoy), 1),
            'avg_area': None if np.isnan(avg_area_latest) else round(float(avg_area_latest), 1),
        },
        'mtd': mtd,
    }
    with open(os.path.join(config.REPORT_DIR, 'kpi.json'), 'w', encoding='utf-8') as f:
        json.dump(kpi, f, ensure_ascii=False, allow_nan=False)

    # 质量元数据（T2.2/T2.4）：六字段，coverage 从数据派生，图上标注/头部新鲜度同源
    gen_at = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')
    periods = {
        'resale_monthly': [str(p) for p in monthly['period']],
        'resale_daily': [d.strftime('%Y-%m-%d') for d in daily['日期']] if not daily.empty else [],
        'district_monthly': sorted(str(p) for p in district['period'].unique()) if not district.empty else [],
        'area_monthly': sorted(str(p) for p in area['period'].unique()) if not area.empty else [],
        'new_daily': [d.strftime('%Y-%m-%d') for d in new_daily['日期']] if not new_daily.empty else [],
        'agency_monthly': sorted(str(p) for p in agency['period'].unique()) if not agency.empty else [],
        'annual': [str(int(y)) for y in annual['年份']] if not annual.empty else [],
    }
    with open(os.path.join(config.REPORT_DIR, 'quality.json'), 'w', encoding='utf-8') as f:
        json.dump(quality.dataset_meta(periods, generated_at=gen_at), f,
                  ensure_ascii=False)

    md = []
    md.append('# 北京二手住宅成交趋势报告\n')
    md.append(f'> 数据范围：{span}（共 {len(monthly)} 个月）· 数据来源：北京市住建委（官方自爬）· 仅历史趋势，不含预测\n')
    # 新鲜度分数据集标注（O2/T2.4）：与 quality.json 同源，禁单一截止日误导
    fres = []
    if not daily.empty:
        fres.append(f"日度 {daily['日期'].max().strftime('%Y-%m-%d')}")
    if not district.empty:
        fres.append(f"区县/面积段 {district['period'].min()}~{district['period'].max()}")
    if not annual.empty:
        fres.append(f"年度 {int(annual['年份'].min())}~{int(annual['年份'].max())}")
    mtd_flag = (f" ｜ 本月 {daily['日期'].max().strftime('%Y-%m')} 进行中"
                if not daily.empty else '')
    md.append(f"> 数据截止：{' · '.join(fres)} ｜ 生成 {gen_at}{mtd_flag}\n")
    md.append('---\n')
    md.append('<!-- toc -->\n')

    # 摘要
    md.append('## 一、核心摘要\n')
    md.append('<!-- kpi -->\n')
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
    md.append('\n*蓝柱（左轴）=月度网签套数，红线=12 月移动均值；绿/橙柱（右轴）=同比上涨/下跌。'
              '移动均值剥离月度噪声看大方向。*\n')
    md.append(f'\n![季节性]({imgs["season"]})\n')
    md.append('\n*年×月热力图。注意 2 月（春节）通常塌量、3-4 月及年末冲量——属季节性，勿误读为趋势拐点。*\n')
    md.append(f'\n![套均面积]({imgs["avgarea"]})\n')
    md.append('\n*y 轴自 80 m² 起（放大区间差异），数值保留 2 位小数。*\n')

    md.append('\n### 月度明细\n')
    rows = []
    for _, r in monthly_yoy.iloc[::-1].iterrows():
        rows.append([str(r['period']), f"{int(r['住宅签约套数']):,}",
                     _pct(r['环比']), _pct(r['同比']),
                     f"{r['住宅签约面积(m2)']:,.0f}",
                     f"{r['住宅签约面积(m2)']/r['住宅签约套数']:.1f}" if r['住宅签约套数'] else '—'])
    md.append(_table(['年月', '住宅网签(套)', '环比', '同比', '住宅面积(m²)', '套均(m²)'], rows))
    md.append('')

    # R5 累计与非住宅结构（T2.6）
    if imgs.get('cum'):
        md.append('\n### 累计视角（滚动12月 / 累计同比）\n')
        md.append(f'![累计视角]({imgs["cum"]})\n')
        md.append('\n*蓝线=滚动 12 月住宅网签总和（左轴，剥离季节性看水位）；绿/橙柱=累计同比'
                  '（右轴，等窗口径：当年 1..n 月 vs 去年同期 1..n 月）。*\n')
    if imgs.get('share'):
        md.append('\n### 非住宅占比\n')
        md.append(f'![非住宅占比]({imgs["share"]})\n')
        md.append('\n*非住宅 = 总量 − 住宅。2025-01 面积口径坏值 → 该点缺失（不插值）。*\n')
    if imgs.get('annual'):
        md.append('\n### 五年长周期锚点\n')
        md.append(f'![五年锚点]({imgs["annual"]})\n')
        md.append('\n*年度住宅成交（万套）：2020–2024 为官方年度表（万套圆整值），'
                  '2025 为自爬月度求和（精确值）——两段存在口径切换缝，仅作周期定位锚点、'
                  '不做趋势外推；灰虚线为官方五年均值。*\n')

    # 周度趋势
    md.append('\n## 三、周度成交趋势\n')
    if not weekly.empty and imgs.get('weekly'):
        n_wk = len(weekly)
        partial_n = int((weekly['days'] < 7).sum())
        wk_latest = weekly.iloc[-1]
        wk_prev = weekly.iloc[-2] if len(weekly) > 1 else None
        est_note = f'，含估算 {int(wk_latest["est_days"])} 天' if wk_latest.get('est_days', 0) else ''
        if (int(wk_latest['days']) < 7 and wk_prev is not None
                and int(wk_prev['days']) >= 7):
            # 进行中周：与满周不可比，禁总量环比（O1），改日均口径
            avg_now = wk_latest['套数'] / wk_latest['days']
            avg_prev = wk_prev['套数'] / wk_prev['days']
            d_avg = (avg_now - avg_prev) / avg_prev * 100 if avg_prev else np.nan
            wk_note = (f'\n*最新周 {wk_latest["week_start"].date()}（进行中，仅 '
                       f'{int(wk_latest["days"])} 天{est_note}）：{int(wk_latest["套数"]):,} 套，'
                       f'日均 {avg_now:.0f} 套（上周日均 {avg_prev:.0f}，日均环比 {_pct(d_avg)}）。'
                       '不完整周不做总量环比。*\n')
        else:
            wk_mom = ((wk_latest['套数'] - wk_prev['套数']) / wk_prev['套数'] * 100) if wk_prev is not None else np.nan
            wk_note = (f'\n*最新周 {wk_latest["week_start"].date()}：{int(wk_latest["套数"]):,} 套 / '
                       f'{wk_latest["面积"]:,.0f} m²，环比 {_pct(wk_mom)}{est_note}。*\n')
        md.append(f'> 基于 {n_wk} 个自然周（{weekly["week_start"].iloc[0].date()} ~ '
                  f'{weekly["week_start"].iloc[-1].date()}）。周度粒度比月度更能反映短期动能。\n')
        md.append(f'\n![周度趋势]({imgs["weekly"]})\n')
        est_total = int((weekly['est_days'] > 0).sum()) if 'est_days' in weekly.columns else 0
        est_note_chart = f'红框柱=含估算日的周（共 {est_total} 周，明细见数据说明）。' if est_total else ''
        md.append('\n*柱=周度套数，红线=4 周移动均值，绿线=周度面积（右轴）。'
                  f'橙色柱表示该周天数 <7（节假日/抓取缺口），数值偏低（共 {partial_n} 周）。'
                  f'{est_note_chart}*\n')
        md.append('\n### 近 10 周\n')
        wrows = []
        for _, r in weekly.iloc[::-1].head(10).iterrows():
            mark = ' ⚠️' if r['days'] < 7 else ''
            wrows.append([r['week_start'].strftime('%Y-%m-%d'), f"{int(r['套数']):,}",
                          f"{r['面积']:,.0f}", f"{r['套均面积']:.1f}", f"{int(r['days'])}{mark}"])
        md.append(_table(['周起始(周一)', '住宅网签(套)', '住宅面积(m²)', '套均(m²)', '天数'], wrows))
        md.append(wk_note)

        # 周度套均面积连续趋势（面积结构走向的代理信号）
        if imgs.get('weekly_aua'):
            full_wk = weekly[weekly['days'] >= 6]
            aua_dir = '—'
            if len(full_wk) >= 2:
                first_aua, last_aua = full_wk['套均面积'].iloc[0], full_wk['套均面积'].iloc[-1]
                aua_dir = '上升（趋于改善）' if last_aua > first_aua else '下降（趋于刚需）'
            md.append(f'\n![周度套均面积]({imgs["weekly_aua"]})\n')
            md.append('\n*周度套均面积（仅画满 ≥6 天的周；y 轴自 80 起）。面积段分月只有少数点，'
                      '套均面积可连续观察"成交往大还是往小走"。'
                      f'区间走势：{aua_dir}。*\n')
    else:
        md.append('无可用日数据，跳过周度分析。\n')

    # 区域格局
    md.append('\n## 四、区域格局\n')
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

    if imgs.get('d_mom'):
        md.append('\n### 各区月度环比热力\n')
        md.append(f'![各区环比]({imgs["d_mom"]})\n')
        md.append('\n*各区签约套数月度环比%（发散色：绿=涨、橙=跌、白=0；首月无环比）。'
                  '环比受季节性影响显著（春节 2→3 月普遍反弹，勿读作结构性回暖）；'
                  '色阶截断于 ±100%（超限格以数值为准）。'
                  '区县数据为月度粒度——官方不发布区县×周数据，此为最细可用粒度。*\n')

    # 市场结构
    md.append('\n## 五、市场结构\n')
    if not area.empty:
        md.append('### 面积段成交量（月度）\n')
        if imgs.get('area_cnt'):
            md.append(f'![面积段成交量]({imgs["area_cnt"]})\n')
        md.append('\n### 面积段成交占比（月度）\n')
        if imgs.get('area_sh'):
            md.append(f'![面积段占比]({imgs["area_sh"]})\n')
        md.append(f'\n*占比视角剥离总量波动（如 2 月春节全线塌量），更能看出结构迁移。'
                  f'当前 {n_area_months} 个月（2026-04 不可补、缺失），序列不连续。*\n')

        # 各面积段 × 月份 占比表 + 占比变化(pp)；段序单一来源（charts.AREA_SEGMENTS）
        order = charts.AREA_SEGMENTS
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

    # 新房市场（M3 R1/R2，插在数据说明前；守卫与各节一致用 imgs.get，
    # 标题在守卫内——三 prep 全空时不留空节/空目录项）
    if any(imgs.get(k) for k in ('new_wk', 'resale_new', 'new_inv')):
        md.append('\n## 六、新房市场（认购/签约/库存）\n')
        md.append('> 新房认购/签约字段 2026-08-23 起全量（序列尚短，趋势仅作方向参考）；'
                  '2026-09-16 缺失（约定 11）。区县维度官方不发布，全市口径。\n')
        if imgs.get('new_wk'):
            md.append(f'\n![新房周度]({imgs["new_wk"]})\n')
            md.append('\n*柱=周签约住宅套数（期房+现房，橙=不完整周），绿线=周认购住宅，'
                      '红线=签约 4 周均线。认购与签约高度同周联动（周度互相关峰值在零滞后），'
                      '领先性尚待更长序列验证。*\n')
        if imgs.get('resale_new'):
            md.append(f'\n![二手vs新房]({imgs["resale_new"]})\n')
            md.append('\n*同窗口对照：蓝=二手住宅周网签，橙=新房周签约（住宅），绿虚线=新房周认购。'
                      '新房序列自 2026-08 下旬起，禁止跨窗口外推。*\n')
        if imgs.get('new_inv'):
            inv_latest = inv_df.iloc[-1]
            md.append(f'\n![新房库存]({imgs["new_inv"]})\n')
            md.append(f'\n*住宅库存快照=可售期房住宅+未签约现房住宅，最新 '
                      f'{inv_latest["库存住宅"] / 1e4:.2f} 万套（{inv_latest["日期"]:%Y-%m-%d}）。'
                      '去化周期待满 3 个完整签约月后启用（预计 2027 年初起出值），当前先列库存水位。*\n')

    # 机构格局（M3 R3，插在数据说明前）
    if imgs.get('agency_top') or imgs.get('refund'):
        md.append('\n## 七、机构格局（上榜口径）\n')
        md.append('> 经纪机构月度排行（2025-12 起，缺 2026-05/06——页面删列致解析断流，'
                  '约定 9）。份额为**上榜机构内份额**（分母=当月上榜机构签约合计，非全市）；'
                  '上榜名单逐月变动。\n')
        if not agency_top10.empty:
            latest_ym = str(agency['period'].max())
            md.append(f'\n### {latest_ym} Top{len(agency_top10)}（签约套数 / 上榜内份额）\n')
            arows = [[_r['经纪机构'], f"{int(_r['签约套数']):,}", f"{_r['上榜份额']:.1f}%"]
                     for _, _r in agency_top10.iterrows()]
            md.append(_table(['经纪机构', '签约套数', '上榜内份额'], arows))
            md.append('')
        if imgs.get('agency_top'):
            md.append(f'\n![机构Top]({imgs["agency_top"]})\n')
        if imgs.get('refund'):
            md.append(f'\n![退房率]({imgs["refund"]})\n')
            md.append('\n*柱=当月退房率（Σ上榜退房/Σ上榜签约），红线=3 月滚动（窗口内 Σ/Σ'
                      ' 合并比，禁比率均值）；2026-05/06 断档月缺失、跨断档滚动窗口为空'
                      '（不插值）。退房是历史签约的回流，解读为"回流强度"而非当月成交质量。*\n')

    # 数据说明 + 参考锚点
    md.append('\n## 八、数据说明\n')
    md.append('- **主数据**：全部来自北京市住建委（pageId=307749）官方自爬，经完整性校验（面积/价格段加总=全市）。\n')
    md.append(f'- **月度覆盖**：{span}（{n_months} 个月）。区县 {n_dist_months} 个月。'
              f'面积段 {n_area_months} 个月（序列不连续）。\n')
    md.append('- **面积段 2026-04 永久缺失**：2026-03 已从 git 历史快照恢复；但 2026-04 的各面积段明细'
              '因解析器时间窗口 bug 永久丢失（4 月数据 5 月才上线，恰在解析器改版失效之后），'
              '官方无回溯、日数据无面积段拆分，无法恢复。全市总量（19784 套）仍可在区县数据中查到。\n')
    md.append('- **日度估算日**（非真实观测，引用单日值须注明）：'
              + '；'.join(f'{d} {desc}' for d, desc in quality.ESTIMATED_DAYS.items())
              + '。估算仅存在于日/周粒度（如 9/14 周 4,257 套含估算 704），月度序列均为官方观测。\n')
    md.append('- **价格段**：官方近月仅发布成交数据（发布数据全为占位）、且仅 3 个月，不足以呈现趋势，已从报告中移除。\n')
    md.append('- **不含预测**：本报告仅呈现历史趋势，不预测未来。\n')
    md.append('- **历史数据**：官方无历史月度回溯接口；第三方历史因口径/可靠性未纳入主序列。\n')

    out = os.path.join(config.REPORT_DIR, 'trend_report.md')
    with open(out, 'w', encoding='utf-8') as f:
        f.write('\n'.join(md))
    return out
