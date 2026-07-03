# -*- coding: utf-8 -*-
"""AI 分析输入导出：客观事实 digest + 可粘贴 prompt。

设计原则（与项目可靠性一致）：
  - 代码拥有数字：本模块从 load/metrics 确定性计算全部事实，AI 只能引用此处数字。
  - 仅客观视角、不含价格（price_monthly 完全不读）、不含预测。
  - 输出单个 markdown 文件（report/ai_digest.md），整篇复制粘贴给 LLM 即可。

用法：python script/gen_ai_digest.py
"""
import os
import numpy as np
import pandas as pd

import config
from . import load, metrics

# 经 2025 口径验证的媒体锚点（非自爬，仅客观参考）
_H1_ANCHORS = [(2022, 69754), (2023, 84332), (2024, 74780), (2025, 88575)]


def _f(v, spec=',.0f'):
    return '—' if pd.isna(v) else format(v, spec)


def _pct(v):
    return '—' if pd.isna(v) else f'{v:+.1f}%'


def _period_label(p):
    s = str(p)
    return f"{s[:4]}-{s[5:7]}"


def _monthly_block(monthly_yoy, monthly_ma):
    lines = []
    last = monthly_yoy.iloc[-1]
    lines.append(f"- 最新 {_period_label(last['period'])}：住宅网签 {_f(last['住宅签约套数'])} 套，"
                 f"环比 {_pct(last['环比'])}，同比 {_pct(last['同比'])}")
    peak = monthly_yoy.loc[monthly_yoy['住宅签约套数'].idxmax()]
    trough = monthly_yoy.loc[monthly_yoy['住宅签约套数'].idxmin()]
    lines.append(f"- 区间峰值 {_period_label(peak['period'])}（{int(peak['住宅签约套数']):,} 套）；"
                 f"谷值 {_period_label(trough['period'])}（{int(trough['住宅签约套数']):,} 套，春节月）")
    # 12 月移动均值方向
    ma = monthly_ma['MA12'].dropna()
    if len(ma) >= 2:
        d = '上升' if ma.iloc[-1] > ma.iloc[-2] else '下降'
        lines.append(f"- 12 月移动均值：{ma.iloc[-2]:.0f} → {ma.iloc[-1]:.0f}（{d}）")
    # 逐月明细（套数 / 环比 / 同比）
    lines.append("- 逐月明细（年月: 套数 | 环比 | 同比）：")
    for _, r in monthly_yoy.iterrows():
        lines.append(f"  - {_period_label(r['period'])}: {int(r['住宅签约套数']):,} | "
                     f"{_pct(r['环比'])} | {_pct(r['同比'])}")
    return '\n'.join(lines)


def _weekly_block(weekly):
    if weekly is None or weekly.empty:
        return '- （无周度数据）'
    full = weekly[weekly['days'] >= 7]
    lines = []
    latest = weekly.iloc[-1]
    note = '天数<7，未满周' if latest['days'] < 7 else '满周'
    lines.append(f"- 最新周 {latest['week_start'].date()}（{int(latest['days'])} 天，{note}）："
                 f"{int(latest['套数']):,} 套")
    if len(full) >= 4:
        last4 = full.tail(4)
        series = '、'.join(f"{int(x):,}" for x in last4['套数'])
        d = '上升' if last4['套数'].iloc[-1] > last4['套数'].iloc[0] else '下降'
        lines.append(f"- 近 4 个满周套数：{series}（{d}）")
    lines.append(f"- 周度总计 {len(weekly)} 周，其中 {int((weekly['days'] < 7).sum())} 周天数<7（节假日/抓取缺口，数值偏低）")
    return '\n'.join(lines)


def _avg_area_block(aua):
    if aua is None or aua.empty:
        return '- （无数据）'
    first, last = aua.iloc[0], aua.iloc[-1]
    d = '下降（趋于刚需）' if last['套均面积'] < first['套均面积'] else '上升（趋于改善）'
    return (f"- {_period_label(first['period'])} {first['套均面积']:.1f} m² → "
            f"{_period_label(last['period'])} {last['套均面积']:.1f} m²（{d}）")


def _district_block(district):
    if district is None or district.empty:
        return '- （无区县数据）'
    rank = metrics.district_rank_change(district)
    if rank.empty:
        return '- （区县数据不足）'
    detail = district[district['区县_clean'] != '全市']
    latest_period = detail['period'].max()
    latest = detail[detail['period'] == latest_period].sort_values('签约套数', ascending=False)
    lines = [f"- 最新 {_period_label(latest_period)} Top 5 区："
             + '、'.join(f"{r['区县_clean']} {int(r['签约套数']):,}" for _, r in latest.head(5).iterrows())]
    movers = rank[(rank['排名变化'] != 0) & rank['排名变化'].notna()]
    if not movers.empty:
        chg = '、'.join(f"{name} {'↑' if int(r['排名变化']) > 0 else '↓'}{abs(int(r['排名变化']))}"
                        for name, r in movers.head(6).iterrows())
        lines.append(f"- 排名变化（首→末）：{chg}")
    share_movers = rank[rank['份额变化_pp'].abs() >= 0.3].sort_values('份额变化_pp')
    if not share_movers.empty:
        sh = '、'.join(f"{name} {r['份额变化_pp']:+.2f}pp" for name, r in share_movers.head(6).iterrows())
        lines.append(f"- 份额变化（首→末，|≥0.3pp|）：{sh}")
    return '\n'.join(lines)


def _area_block(area):
    if area is None or area.empty:
        return '- （无面积段数据）'
    months = sorted(area['period'].unique())
    df = area.copy()
    df['share'] = df['成交套数'] / df.groupby('period')['成交套数'].transform('sum') * 100
    pv = df.pivot_table(index='面积区间', columns='period', values='share', aggfunc='sum')
    order = ['60m2以下', '60～80m2', '80～100m2', '100～120m2', '120～140m2', '140m2以上']
    pv = pv.reindex([s for s in order if s in pv.index])
    latest_p, first_p = months[-1], months[0]
    lines = [f"- 最新 {_period_label(latest_p)} 各段占比："
             + '、'.join(f"{seg} {pv.loc[seg, latest_p]:.1f}%" for seg in pv.index)]
    lines.append(f"- 占比变化（{_period_label(first_p)}→{_period_label(latest_p)}）："
                 + '、'.join(f"{seg} {pv.loc[seg, latest_p] - pv.loc[seg, first_p]:+.2f}pp"
                             for seg in pv.index))
    lines.append(f"- 仅 {len(months)} 个月且缺 2026-04，序列不连续，结构趋势仅供参考")
    return '\n'.join(lines)


def build_digest_text():
    """计算并返回"客观事实 digest"文本（不含价格、不含预测）。"""
    monthly = load.load_monthly()
    annual = load.load_annual()
    district = load.load_district()
    area = load.load_area()
    daily = load.load_daily()

    monthly_yoy = metrics.add_yoy_mom(monthly)
    monthly_ma = metrics.add_moving_avg(monthly_yoy, windows=(12,))
    aua = metrics.avg_unit_area(monthly)
    weekly = metrics.weekly_aggregate(daily) if not daily.empty else pd.DataFrame()

    n_m = len(monthly)
    n_d = district['period'].nunique() if not district.empty else 0
    n_a = area['period'].nunique() if not area.empty else 0
    span_m = f"{_period_label(monthly['period'].min())} ~ {_period_label(monthly['period'].max())}"
    span_d = (f"{_period_label(district['period'].min())} ~ {_period_label(district['period'].max())}"
              if not district.empty else '—')

    L = []
    L.append('# 数据摘要（digest · 客观事实）\n')
    L.append('> 口径：北京市住建委官方自爬。**仅成交量与面积，不含价格**。'
             'AI 叙述只能引用本摘要中的数字，不得编造或推算新数字。\n')

    L.append('## 数据范围')
    L.append(f"- 全市月度：{span_m}（{n_m} 个月）")
    L.append(f"- 区县月度：{span_d}（{n_d} 个月）")
    L.append(f"- 面积段月度：{n_a} 个月（缺 2026-04，序列不连续）")
    if not weekly.empty:
        L.append(f"- 周度：{weekly['week_start'].iloc[0].date()} ~ "
                 f"{weekly['week_start'].iloc[-1].date()}（{len(weekly)} 周）")
    L.append("- 年度：2020–2024（官方）；上半年网签锚点含媒体引用（已标注）")
    L.append("- **不含任何价格数据**\n")

    L.append('## 全市月度（住宅网签套数）')
    L.append(_monthly_block(monthly_yoy, monthly_ma))
    L.append('')

    L.append('## 套均成交面积')
    L.append(_avg_area_block(aua))
    L.append('')

    L.append('## 周度成交')
    L.append(_weekly_block(weekly))
    L.append('')

    L.append('## 区域格局')
    L.append(_district_block(district))
    L.append('')

    L.append('## 面积段结构（非价格）')
    L.append(_area_block(area))
    L.append('')

    L.append('## 历史坐标')
    L.append('- 年度二手住宅（万套，官方）：'
             + '、'.join(f"{int(r['年份'])}={r['住宅套数万']:.2f}" for _, r in annual.iterrows()))
    L.append('- 上半年网签（套，**媒体引用·非自爬**）：'
             + '、'.join(f"{y}={v:,}" for y, v in _H1_ANCHORS))
    L.append('')

    L.append('## 必须在叙述中体现的局限')
    L.append("- 月度仅 17 个月、区县 6 个月、面积段 5 个月且缺 2026-04：趋势判断须谨慎，多用\"样本有限/尚不能确认\"等限定。")
    L.append("- 2 月为春节季节性塌量（非趋势拐点）；3 月、年末通常冲量。区分\"趋势\"与\"季节性\"。")
    L.append("- **无价格数据**：不得谈论价格涨跌、贵贱、 affordability。")
    L.append("- **预判纪律**：可对未来 1–3 个月做预判，但须区分事实与判断、给方向+幅度区间+置信度+证伪条件、"
             "情景化、短 horizon；政策/信贷作为条件摇摆因素提及，不编造具体政策。预判非事实、非定论。")
    L.append("- 不给买卖建议。")
    return '\n'.join(L)


PROMPT_INSTRUCTIONS = """# 任务

你是一名**资深的**北京二手住宅市场分析师/咨询专家。请基于下方"数据摘要"，写一份市场趋势分析：前半客观陈述已发生的成交走势，后半以资深视角给出**前瞻预判**。供报告读者理解"北京二手住宅成交在怎么走、接下来可能怎么走"。

# 硬性规则（必须遵守）

1. **事实只引用摘要中出现的数字**，不得编造、不得自行推算新数字、不得引用外部信息。预判可以外推方向，但所依据的事实必须来自摘要。
2. **薄数据必须 hedge**：区县仅 6 个月、面积段仅 5 个月且缺 2026-04、月度仅 17 个月——涉及这些的判断要加限定（"样本有限""尚不能确认趋势"）。
3. **区分趋势与季节性**：2 月塌量是春节季节性、非拐点；3 月/年末冲量亦属季节性。不要把季节性误读为趋势转折。
4. **可做预判，按资深分析师/咨询专家纪律**：
   - 明确区分「事实」（须来自摘要数字）与「判断/预判」（你的外推）。
   - 预判基于可解释的机制（季节性、基数效应、动量），给**方向 + 幅度区间 + 置信度（高/中/低）+ 证伪条件**。
   - 仅做**短 horizon（未来 1–3 个月）**；薄数据下不做长期精确数字。
   - **情景化**：给基准/乐观/悲观，或 if-then。
   - 政策/信贷是重要摇摆因素，可作为**条件变量**提及，但不得编造具体政策内容。
   - 预判一律标注为判断、非事实、非定论。
5. **不涉及价格**：数据不含任何价格信息，禁止谈论价格涨跌、贵贱、性价比、 affordability（"量价关系"也仅能在不引用价格数字的前提下，作条件性、定性提及）。
6. **不给买卖建议**：不做"该买/该等/该卖"类个体决策指导。
7. 数字引用保持与摘要一致（套数带千分位、百分比保留一位小数）。

# 输出结构（markdown，可直接贴回报告）

## 客观分析与预判（AI 生成 · 仅供参考）

> 本节由 AI 基于确定性计算的数据摘要生成。事实部分来自摘要数字；预判部分为**分析师判断、非事实、非定论**。非投资建议、不含价格。

### 一、市场总体（事实）
（2–4 句：最新月份量级、环比同比、放到区间峰谷里的位置；剔除季节性后的真实方向）

### 二、成交节奏（事实）
（2–3 句：周度近况、短期动能方向，注意不完整周的说明）

### 三、结构变化（事实）
（2–4 句：套均面积走向、面积段占比迁移——成交往哪段集中；标注样本有限）

### 四、区域格局（事实）
（2–4 句：哪些区走强/走弱、份额与排名变化；标注仅 6 个月）

### 五、历史坐标与局限（事实）
（2–3 句：放到年度/上半年锚点里的位置；列出数据局限：样本短、缺 2026-04、无价格）

### 六、预判（资深视角 · 判断而非事实）
（5–7 句：未来 1–3 个月的方向+幅度区间+置信度；核心驱动机制（季节性/基数效应/动量）；情景与风险（如政策摇摆、9 月与 12 月的高基数压力）；证伪条件。明确标注这是分析师判断。）

---

# 数据摘要

"""


def build_prompt():
    """返回完整可粘贴文本 = 指令 + digest。"""
    return PROMPT_INSTRUCTIONS + build_digest_text() + '\n'


def render_file(out_path=None):
    """写 prompt+digest 到 out_path（默认 report/ai_digest.md）。返回路径。"""
    if out_path is None:
        os.makedirs(config.REPORT_DIR, exist_ok=True)
        out_path = os.path.join(config.REPORT_DIR, 'ai_digest.md')
    else:
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(build_prompt())
    return out_path
