# -*- coding: utf-8 -*-
"""图表叙述渲染（chart-narrative 设计 v1.1b §5）——纯函数：只读 facts dict，
不反查 DataFrame，文案全在此层。

与周度叙述同纪律：每图整段一行、<!-- cn-<stem> --> 界定符起止包裹（verify 定界）、
narrative=false → 空串（无界定符）；首句粗体=结论前置；幅度渲染取绝对值、方向由
词表达（继承 W2.3 裁决，与月度明细表箭头同构）。禁用词封闭表 CN_BANNED_WORDS
在 verify.py（C5 接线，单一来源）。句式为设计 §5 冻结文案，改动须同步设计文档。
"""

_STEMS = ('district_mom', 'area_segment_share', 'refund_rate')


def _delim(stem):
    return f'<!-- cn-{stem} -->'


def render(stem, facts):
    """facts dict → md 段落（界定符包裹）。narrative=false/未知 stem → 空串。"""
    if stem not in _STEMS or not facts or not facts.get('narrative'):
        return ''
    body = {'district_mom': _district, 'area_segment_share': _area,
            'refund_rate': _refund}[stem](facts)
    d = _delim(stem)
    return f'{d}\n{body}\n{d}'


def _district(f):
    """≤2 句正文：粗体领涨/领跌合一（括注门槛）+ 家数句；全跌月（n_down==17）
    只出全跌句（家数信息已由其承载，并列即自相矛盾）。数字上限 9、满配恰 9。"""
    if f['n_down'] == 17:
        return '**17 区全部下跌。**'
    out = ''
    segs = []
    if f['leader_name'] is not None:
        segs.append(f"{f['leader_name']}领涨，环比 {abs(f['leader_pct']):.1f}%"
                    f"（当月 {f['leader_units']:,} 套）")
    if f['laggard_name'] is not None:
        segs.append(f"{f['laggard_name']}领跌，环比 {abs(f['laggard_pct']):.1f}%"
                    f"（当月 {f['laggard_units']:,} 套）")
    if segs:
        out = f"**{'；'.join(segs)}。**（均按上月签约 500 套以上筛选）"
    if f['broad_up']:
        cnt = f"{f['n_up']}/17 区上涨（普涨）"
    elif f['broad_down']:
        cnt = f"{f['n_down']}/17 区下跌（普跌）"
    else:
        cnt = f"17 区中 {f['n_up']} 区上涨、{f['n_down']} 区下跌"
        if f['n_flat']:
            cnt += f"、{f['n_flat']} 区持平"
    return out + cnt + '。'


def _area(f):
    """≤2 句正文：粗体主句 + 变化句（易主/方向）或摆事实句。数字上限 2、满配恰 2。
    触发判定读 facts.top_trigger（禁 render 自算——round1 会假触发，设计 v1.1b）。"""
    out = f"**{f['top_seg']}占 {f['top_share']:.1f}%（{f['last_month']}）。**"
    if f['top_trigger']:
        if f['top_changed']:
            out += f"{f['top_seg']}份额升至第一（{f['top_share']:.1f}%）。"
        else:
            direction = '升' if f['top_delta_pp'] > 0 else '降'
            out += (f"比 {f['prev_month']} {direction} "
                    f"{abs(f['top_delta_pp']):.1f} 个百分点。")
    else:
        out += (f"各段份额变化均在 ±{f['max_abs_delta_pp']:.1f} 个百分点内"
                f"（{f['prev_month']}→{f['last_month']}）。")
    return out


def _refund(f):
    """≤2 句正文：粗体主句（断档月自动无环比半句）+ 滚动句（末两有效点相邻才有）。
    数字上限 5、满配恰 5；环比句带上月值（可回推验算）；方向词：回落/上升。"""
    out = f"**{f['last_month']} 上榜机构合计退房率 {f['cur_rate']:.2f}%"
    if f['delta_pp'] is not None:
        direction = '回落' if f['delta_pp'] < 0 else '上升'
        out += (f"，比上月 {f['prev_rate']:.2f}% {direction} "
                f"{abs(f['delta_pp']):.2f} 个百分点")
    out += '。**'
    if f['roll_months_adjacent']:
        d = f['roll_last'] - f['roll_prev']
        direction = '回落' if d < 0 else '上升'
        out += (f"近 3 个月滚动退房率 {f['roll_last']:.2f}%，"
                f"比前一窗口{direction} {abs(d):.2f} 个百分点。")
    return out
