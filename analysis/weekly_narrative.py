# -*- coding: utf-8 -*-
"""周度叙述渲染（需求 v0.3 / 设计 v1.2 §2.2）——纯函数：facts dict → md 段落。

只消费 metrics.weekly_trend_facts 的字段，不反查任何 DataFrame；
数字与枚举全部来自事实层，本模块只负责措辞（普通读者口径：
结论前置、数字少、口语时间锚点、无预测词）。
输出以 <!-- wk-narrative --> 起止包裹，供 verify 定界与测试断言
（html_render 对 HTML 注释直通，不渲染）。
"""
from datetime import datetime, timedelta

import chinese_calendar as cc

DELIM = '<!-- wk-narrative -->'

_FESTIVAL = {"New Year's Day": '元旦', 'Spring Festival': '春节',
             'Tomb-sweeping Day': '清明', 'Labour Day': '五一',
             'Dragon Boat Festival': '端午', 'Mid-autumn Festival': '中秋',
             'National Day': '国庆'}


def _d(s):
    return datetime.strptime(s, '%Y-%m-%d')


def _xun(week_str):
    """周起始日期 → 'X 月上/中/下旬'（1–10 / 11–20 / 21–月末）。"""
    d = _d(week_str)
    x = '上' if d.day <= 10 else ('中' if d.day <= 20 else '下')
    return f'{d.month} 月{x}旬'


def _festival(week_str):
    """节假日周 → 口语节日名（国庆/春节/…）；未知名或库未覆盖 → None。"""
    d0 = _d(week_str)
    for i in range(7):
        try:
            on, name = cc.get_holiday_detail(d0 + timedelta(days=i))
        except NotImplementedError:
            return None
        if on and name:
            return _FESTIVAL.get(name)
    return None


def _date_code(week_str):
    """末周值后的全段唯一日期码：'10 月 19–25 日当周'（跨月两段式）。"""
    a, b = _d(week_str), _d(week_str) + timedelta(days=6)
    if a.month == b.month:
        return f'{a.month} 月 {a.day}–{b.day} 日当周'
    return f'{a.month} 月 {a.day} 日–{b.month} 月 {b.day} 日当周'


def _level_phrase(f):
    """水位短语（方向中立、带差距幅度；基准 <26 周退化'有数据以来'）。空串=无水位句。"""
    if f.get('level') is None:
        return ''
    base = '最近半年' if f['avg26_weeks'] >= 26 else '有数据以来'
    gap = f.get('level_gap_pct')
    if f['level'] == 'above':
        return f'，比{base}的周均高 {gap}%'
    if f['level'] == 'below':
        return f'，仍比{base}的周均低 {abs(gap)}%'
    return f'，与{base}的周均大体相当'


def _anchor_phrase(f):
    """锚句（avg4 基准的事实已由 metrics 算好，这里只措辞）。"""
    k = f.get('anchor_kind')
    if k == 'week':
        return f'大约回到 {_xun(f["anchor_week"])}的水平'
    if k == 'high':
        return '处于近三个月高位'
    if k == 'low':
        return '处于近三个月低位'
    return ''


def _s1(f):
    lvl = _level_phrase(f)
    if f['direction'] is None:
        r = f['null_reason']
        if r == 'holiday_last':
            if f['trough13_week'] == f['last_full_week']:
                return '**最近一周恰逢假期，涨跌暂不比较。**'
            fest = _festival(f['last_full_week']) or '法定'
            return f'**最近一周恰逢{fest}假期，签约受假期影响，本周不作涨跌比较{lvl}。**'
        if r == 'holiday_prev':
            return f'**节后首周，涨跌走势还看不出来{lvl}。**'
        if r == 'tie':
            return f'**最近两周基本持平{lvl}。**'
        return f'**此前一周数据缺失，暂无可比走势{lvl}。**'
    verb = '回升' if f['direction'] == 'up' else '回落'
    post = '节后' if f['post_holiday'] else ''
    pct = f.get('streak_change_pct')
    if f['streak_weeks'] >= 2:
        return f'**{post}成交连续 {f["streak_weeks"]} 周{verb}，累计 {pct}%{lvl}。**'
    est = '（含估算）' if f['streak_contains_est'] else ''
    v = f'{f["last_full_units"]:,}'
    if f['last_pair_reversal']:
        return f'**{post}近几周有涨有跌，最近一周{verb} {pct}%，成交 {v} 套{est}{lvl}。**'
    return f'**{post}最近一周{verb} {pct}%，成交 {v} 套{est}{lvl}。**'


def _s2(f):
    """峰谷时间线故事（任一周值只出现一次；N=1 锚句附尾）。"""
    if f['peak13_week'] is None:
        return ''
    if f['peak13_week'] == f['trough13_week']:
        u = f'{f["peak13_units"]:,}'
        est = '（含估算）' if f['peak13_est'] else ''
        return f'近三个月每周成交大体持平，都在 {u} 套上下{est}'
    segs = []
    for kind in ('peak13', 'trough13'):
        ws, u = f[f'{kind}_week'], f[f'{kind}_units']
        fest = _festival(ws) if f[f'{kind}_holiday'] else None
        est = '（含估算）' if f[f'{kind}_est'] else ''
        val = f'{u:,} 套'
        if kind == 'peak13':
            seg = (f'{fest}假期那周签约 {val}{est}，是近期高点' if fest
                   else f'{_xun(ws)}曾冲到单周 {val}{est}的高点')
        else:
            seg = (f'{fest}假期那周只签了 {val}{est}'
                   '——假期签约少是每年惯例，不是市场转冷' if fest
                   else f'{_xun(ws)}见到近期低点（{val}）')
        segs.append((ws, seg))
    segs.sort()
    out = '；'.join(s for _, s in segs)
    if f['direction'] is not None and f['streak_weeks'] == 1:
        a = _anchor_phrase(f)
        if a:
            out += f'，{a}'
    return out


def _s3(f):
    """链值细节 + 日期码 + 锚句（N≥2；链首已见于 S2 时去重）。"""
    if f['direction'] is None or f['streak_weeks'] < 2:
        return ''
    verb = '回升' if f['direction'] == 'up' else '回落'
    tail = f'{f["last_full_units"]:,} 套（{_date_code(f["last_full_week"])}）'
    if f['chain_first_week'] in (f['peak13_week'], f['trough13_week']):
        body = f'此间{verb}到 {tail}'
    else:
        body = f'从 {f["chain_first_units"]:,} 套{verb}到 {tail}'
    a = _anchor_phrase(f)
    return body + (f'，{a}' if a else '')


def _s4(f):
    """最新：进行中对比（同期对齐）/ 仅报本周 / 滞后。"""
    st = f.get('last_week_state')
    if st not in ('in_progress', 'lagged'):
        return ''
    if not f.get('ip_days') or f['ip_days'] < 3 or not f.get('ip_contiguous'):
        return ''
    if st == 'lagged':
        return f'本周只统计到 {f["ip_days"]} 天，共 {f["ip_units"]:,} 套（官方数据更新延迟）'
    a = f.get('ip_daily_avg')
    if f.get('ip_prev_same_ok') and f.get('ip_prev_same_avg') is not None:
        b = f['ip_prev_same_avg']
        still = '仍' if f['direction'] == 'up' else ''
        if a > b:
            tail = f'{still}高于上周同期的日均 {b} 套'
        elif a < b:
            tail = f'低于上周同期的日均 {b} 套'
        else:
            tail = f'与上周同期的日均 {b} 套持平'
        return f'本周刚过 {f["ip_days"]} 天，日均 {a} 套，{tail}'
    return f'本周刚过 {f["ip_days"]} 天，已签 {f["ip_units"]:,} 套'


def _s5(f):
    """段末注（不计数字预算；措辞避开禁用词'口径'）。"""
    if f.get('streak_contains_est') and f['direction'] is not None and f['streak_weeks'] >= 2:
        return (f'*（文中"连续 {f["streak_weeks"]} 周"含 1 个估算周，'
                '估算日清单见"数据说明"。）*')
    return ''


def render(facts):
    """facts dict → md 段落（界定符包裹）。narrative=false → 空串（无界定符）。"""
    if not facts or not facts.get('narrative'):
        return ''
    parts = [p for p in (_s1(facts), _s2(facts), _s3(facts), _s4(facts)) if p]
    para = parts[0] + ''.join(f'{p}。' for p in parts[1:])
    note = _s5(facts)
    body = para + (f'\n\n{note}' if note else '')
    return f'{DELIM}\n{body}\n{DELIM}'
