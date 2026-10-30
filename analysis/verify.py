# -*- coding: utf-8 -*-
"""生成后验收门（设计 v1.1 §2.8；开发计划 T1.7）。

原则（QA/架构评审共识）：**期望值从 CSV 独立重算**（csv 模块直读），
不经 load/metrics——同源比对只验序列化，验不出计算错。

校验项：
  1. charts/*.json ↔ *.png 同 stem 成对（双向：缺一不可、多余报错）
  2. JSON 合法且无 NaN/Infinity 字面量（浏览器解析门）
  3. 锚点：city_trend 首柱序列末值 == CSV 尾月住宅签约套数（独立重算）；
     md 中出现该月千分位数字
  4. HTML 体积门（2MB）+ 交互标记（NO_JS 模式则查 PNG 内嵌标记）
  5. vendor 组件标记自检

analyze.py 在 render 之后、开浏览器之前调用；失败由调用方把 HTML 改名
*.bad 隔离并非零退出（防残缺/过期报告被当新产物打开）。
"""
import csv
import json
import os
import re
import sys
from datetime import datetime, timedelta

import chinese_calendar as cc

import quality

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VENDOR_PATH = os.path.join(_ROOT, 'assets', 'vendor', 'echarts.min.js')
HTML_SIZE_LIMIT = 2 * 1024 * 1024

# 周度叙述禁用词封闭表（需求 v0.5 §2.2；单一来源，tests 从此导入，扫描仅限叙述段）
WK_BANNED_WORDS = ['预计', '有望', '或将', '拐点', '动能', '窗口均值', '口径', '回暖', '走强', '走弱']

# wk_facts.json 全字段（直比用；与 metrics.weekly_trend_facts 默认 dict 逐字对齐）
_WK_FIELDS = ['narrative', 'n_full_weeks', 'last_week_state', 'last_full_week',
              'last_full_units', 'direction', 'null_reason', 'streak_weeks',
              'streak_change_pct', 'last_pair_reversal', 'streak_contains_est',
              'post_holiday', 'chain_first_week', 'chain_first_units',
              'anchor_kind', 'anchor_week', 'avg4', 'avg26', 'avg26_weeks',
              'level', 'level_gap_pct', 'peak13_week', 'peak13_units',
              'peak13_est', 'peak13_est_days', 'peak13_holiday', 'trough13_week',
              'trough13_units', 'trough13_est', 'trough13_est_days',
              'trough13_holiday', 'ip_week', 'ip_units', 'ip_days',
              'ip_contiguous', 'ip_daily_avg', 'ip_prev_same_avg', 'ip_prev_same_ok']

_WK_CC_WARNED = {'flag': False}

CHART_STEMS = ['city_trend', 'seasonality', 'avg_unit_area', 'weekly_trend',
               'weekly_avg_area', 'district_stacked', 'district_share',
               'district_smallmul', 'area_segment_count', 'area_segment_share',
               'cum_view', 'structure_share', 'new_weekly', 'resale_vs_new',
               'new_inventory', 'district_mom', 'annual_anchor', 'agency_top',
               'refund_rate']

_VENDOR_MARKERS = ['heatmap', 'dataZoom', 'toolbox', 'markLine',
                   'visualMap', 'legend', 'tooltip']


def read_last_monthly(data_dir):
    """csv 直读 resale_monthly 尾行 → (年月字符串, 住宅签约套数)。独立路径。"""
    path = os.path.join(data_dir, 'resale_monthly.csv')
    with open(path, encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))
    last = rows[-1]
    return str(last['月份']), int(last['住宅签约套数'])


# ---------------------------------------------------------------------------
# 周度叙述事实独立重算（W4，设计 v1.4 §2.4）——csv 直读、自建周分桶，不经
# load/metrics；与 metrics.weekly_trend_facts 构成双实现，交叉验证防漂移
# （两边规则必须逐字对齐，改任一侧先同步设计文档 §2.1）。

def _wk_expand_est(mapping):
    """quality.ESTIMATED_DAYS 键展开为具体日期（'2026-07-25/26' 简写兼容）。"""
    days = []
    for k in mapping:
        parts = [p.strip() for p in str(k).split('/')]
        days.append(parts[0])
        for p in parts[1:]:
            days.append(p if len(p) > 2 else parts[0][:8] + p)
    return sorted(set(days))


def _wk_net_workdays(week_start):
    """周内工作日净额（chinese_calendar 含调休；未覆盖回退纯 weekday + 一次性告警）。"""
    n = 0
    for i in range(7):
        d = week_start + timedelta(days=i)
        try:
            if cc.is_workday(d):
                n += 1
        except NotImplementedError:
            if not _WK_CC_WARNED['flag']:
                _WK_CC_WARNED['flag'] = True
                print(f'警告: chinese_calendar 未覆盖日期 {d}，节假日周判定回退纯 weekday',
                      file=sys.stderr)
            if d.weekday() < 5:
                n += 1
    return n


def _wk_facts_from_csv(data_dir, estimated_dates=None):
    """从 resale_daily.csv 独立重算周度叙述事实（与 metrics 同 schema；float 字段
    不 round——QA 裁决：重算保持原值对 json round1 值比，容差 0.1）。"""
    if estimated_dates is None:
        estimated_dates = _wk_expand_est(quality.ESTIMATED_DAYS)
    est = {str(d)[:10] for d in estimated_dates}
    units = {}
    with open(os.path.join(data_dir, 'resale_daily.csv'), encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            try:
                v = int(float(r.get('住宅签约套数') or 0))
            except (TypeError, ValueError):
                continue
            ds = str(r.get('日期') or '')[:10]
            if v > 0 and ds:
                units[datetime.strptime(ds, '%Y-%m-%d').date()] = v
    f = {k: None for k in _WK_FIELDS}
    f.update({'narrative': False, 'n_full_weeks': 0, 'streak_weeks': 0,
              'last_pair_reversal': False, 'streak_contains_est': False,
              'post_holiday': False, 'avg26_weeks': 0, 'anchor_kind': 'none'})
    if not units:
        return f
    weeks = {}
    for d, u in units.items():
        ws = d - timedelta(days=d.weekday())
        w = weeks.setdefault(ws, {'u': 0, 'days': set(), 'est': 0})
        w['u'] += u
        w['days'].add(d)
        if d.isoformat() in est:
            w['est'] += 1
    ws_sorted = sorted(weeks)
    fw = [(ws, weeks[ws]['u'], weeks[ws]['est']) for ws in ws_sorted
          if len(weeks[ws]['days']) >= 7]

    hol_cache = {}

    def hol(ws):
        if ws not in hol_cache:
            hol_cache[ws] = _wk_net_workdays(ws) <= 3
        return hol_cache[ws]

    f['n_full_weeks'] = len(fw)
    if fw:
        last_ws, last_u, _ = fw[-1]
        f['last_full_week'] = last_ws.isoformat()
        f['last_full_units'] = last_u
        chain = [len(fw) - 1]
        chain_dir = None
        first_break = None
        i = len(fw) - 1
        while i >= 1:
            a, b = fw[i - 1], fw[i]
            if (b[0] - a[0]).days > 7:
                first_break = 'gap'
                break
            if hol(a[0]) or hol(b[0]):
                first_break = 'holiday'
                break
            if a[1] == b[1]:
                first_break = 'tie'
                break
            d = 1 if b[1] > a[1] else -1
            if chain_dir is None:
                chain_dir = d
            elif d != chain_dir:
                break
            chain.insert(0, i - 1)
            i -= 1
        if chain_dir is not None:
            f['direction'] = 'up' if chain_dir == 1 else 'down'
            f['streak_weeks'] = len(chain) - 1
            cfw, cfu = fw[chain[0]][0], fw[chain[0]][1]
            f['streak_change_pct'] = (last_u - cfu) / cfu * 100
            if len(chain) >= 2:
                f['chain_first_week'] = cfw.isoformat()
                f['chain_first_units'] = cfu
        else:
            f['null_reason'] = {'gap': 'gap',
                                'holiday': 'holiday_last' if hol(last_ws) else 'holiday_prev',
                                'tie': 'tie'}.get(first_break)
        if len(chain) == 2 and chain[0] >= 1:
            a, b = fw[chain[0] - 1], fw[chain[0]]
            ok = ((b[0] - a[0]).days <= 7 and not hol(a[0]) and not hol(b[0])
                  and a[1] != b[1])
            if ok:
                d = 1 if b[1] > a[1] else -1
                f['last_pair_reversal'] = bool(d != chain_dir)
        f['streak_contains_est'] = any(fw[j][2] > 0 for j in chain)
        j0 = chain[0]
        f['post_holiday'] = bool(j0 >= 1 and hol(fw[j0 - 1][0]))

        if len(fw) >= 13:
            w13 = fw[-13:]
            for key, want_max in (('peak13', True), ('trough13', False)):
                best = None
                for t in w13:
                    if best is None:
                        best = t
                    elif t[1] == best[1]:
                        if not hol(t[0]) and hol(best[0]):
                            best = t
                    elif (t[1] > best[1]) == want_max:
                        best = t
                f[f'{key}_week'] = best[0].isoformat()
                f[f'{key}_units'] = best[1]
                f[f'{key}_est'] = best[2] > 0
                f[f'{key}_est_days'] = best[2]
                f[f'{key}_holiday'] = hol(best[0])

        if len(fw) >= 4:
            raw4 = sum(t[1] for t in fw[-4:]) / 4
            base = fw[:-4][-26:]
            f['avg4'] = raw4
            f['avg26_weeks'] = len(base)
            if base:
                raw26 = sum(t[1] for t in base) / len(base)
                f['avg26'] = raw26
                if len(base) >= 13:
                    gap = (raw4 - raw26) / raw26 * 100
                    f['level_gap_pct'] = gap
                    f['level'] = ('parity' if abs(gap) <= 5
                                  else 'above' if gap > 0 else 'below')
            if len(fw) >= 13:
                w13 = fw[-13:]
                chain_set = {fw[j][0] for j in chain}
                last4_start = fw[-4][0]
                cand = [t for t in w13 if t[0] not in chain_set
                        and not hol(t[0]) and t[0] < last4_start]
                hit = None
                if cand:
                    close = min(cand, key=lambda t: abs(t[1] - raw4))
                    if abs(close[1] - raw4) <= 0.10 * raw4:
                        hit = close
                if hit is not None:
                    f['anchor_kind'] = 'week'
                    f['anchor_week'] = hit[0].isoformat()
                elif all(raw4 > t[1] for t in w13):
                    f['anchor_kind'] = 'high'
                elif all(raw4 < t[1] for t in w13):
                    f['anchor_kind'] = 'low'

    lw = ws_sorted[-1]
    if len(weeks[lw]['days']) == 7:
        f['last_week_state'] = 'complete'
    else:
        prev = ws_sorted[-2] if len(ws_sorted) >= 2 else None
        if prev is not None and len(weeks[prev]['days']) < 7 and not hol(prev):
            f['last_week_state'] = 'lagged'
        else:
            f['last_week_state'] = 'in_progress'
        dates = sorted(weeks[lw]['days'])
        f['ip_week'] = lw.isoformat()
        f['ip_days'] = len(dates)
        f['ip_units'] = weeks[lw]['u']
        offs = [(d - lw).days for d in dates]
        f['ip_contiguous'] = bool(max(offs) - min(offs) + 1 == len(offs))
        f['ip_daily_avg'] = round(weeks[lw]['u'] / len(dates))
        prev_dates = [d - timedelta(days=7) for d in dates]
        if all(d in units for d in prev_dates):
            psum = sum(units[d] for d in prev_dates)
            f['ip_prev_same_avg'] = round(psum / len(prev_dates))
            f['ip_prev_same_ok'] = True
        else:
            f['ip_prev_same_ok'] = False

    f['narrative'] = len(fw) >= 8
    if not f['narrative']:
        for k in f:
            if k not in ('narrative', 'n_full_weeks', 'last_week_state'):
                f[k] = None
    return f


def check(report_dir, data_dir, no_js=False):
    """跑全部断言。返回 (ok, issues:list[str])。"""
    issues = []
    charts_dir = os.path.join(report_dir, 'charts')

    # 1) 成对契约（双向）
    for stem in CHART_STEMS:
        if not os.path.isfile(os.path.join(report_dir, stem + '.png')):
            issues.append(f'缺 PNG: {stem}.png')
        if not os.path.isfile(os.path.join(charts_dir, stem + '.json')):
            issues.append(f'缺 charts JSON: {stem}.json')
    if os.path.isdir(charts_dir):
        for fn in os.listdir(charts_dir):
            if fn.endswith('.json') and fn[:-5] not in CHART_STEMS:
                issues.append(f'多余 charts JSON: {fn}')

    # 2) JSON 合法 + 无 NaN/Infinity
    if os.path.isdir(charts_dir):
        for fn in os.listdir(charts_dir):
            if not fn.endswith('.json'):
                continue
            p = os.path.join(charts_dir, fn)
            text = open(p, encoding='utf-8').read()
            if 'NaN' in text or 'Infinity' in text:
                issues.append(f'{fn}: 含 NaN/Infinity 字面量')
            try:
                json.loads(text)
            except ValueError as e:
                issues.append(f'{fn}: JSON 解析失败（{e}）')

    # 3) 锚点独立重算：CSV 尾月套数 == city_trend 首柱末值；md 含千分位数字
    ym, units = read_last_monthly(data_dir)
    city_js = os.path.join(charts_dir, 'city_trend.json')
    if os.path.isfile(city_js):
        payload = json.loads(open(city_js, encoding='utf-8').read())
        try:
            bar = payload['option']['series'][0]['data']
            if not bar or bar[-1] != units:
                issues.append(f'锚点不符: city_trend 柱末值 {bar[-1] if bar else None}'
                              f' != CSV {ym} 住宅签约 {units}')
        except (KeyError, IndexError, TypeError):
            issues.append('city_trend option 结构异常，锚点断言无法执行')
    md_path = os.path.join(report_dir, 'trend_report.md')
    if os.path.isfile(md_path):
        md = open(md_path, encoding='utf-8').read()
        if f'{units:,}' not in md:
            issues.append(f'锚点不符: md 未出现 {ym} 套数 {units:,}')

    # 4) HTML 体积门 + 交互标记（NO_JS 模式查 PNG 标记）
    html_path = os.path.join(report_dir, 'trend_report.html')
    if os.path.isfile(html_path):
        size = os.path.getsize(html_path)
        if size > HTML_SIZE_LIMIT:
            issues.append(f'HTML 体积超限: {size:,} > {HTML_SIZE_LIMIT:,}')
        html = open(html_path, encoding='utf-8').read()
        marker = 'data:image/png;base64' if no_js else 'id="chart-data"'
        if marker not in html:
            issues.append(f'HTML 缺 {"PNG 内嵌" if no_js else "图表数据块"}标记: {marker}')

    # 5) vendor 组件自检
    if os.path.isfile(VENDOR_PATH):
        v = open(VENDOR_PATH, encoding='utf-8').read()
        missing = [m for m in _VENDOR_MARKERS if m not in v]
        if missing:
            issues.append(f'vendor 缺组件标记: {missing}')
    else:
        issues.append(f'vendor 缺失: {VENDOR_PATH}（交互模式不可用）')

    # 6) kpi.json 直比（CSV 独立重算：latest=尾月套数；mtd=日度当月求和/天数）
    kpi_path = os.path.join(report_dir, 'kpi.json')
    if os.path.isfile(kpi_path):
        kpi = json.loads(open(kpi_path, encoding='utf-8').read())
        lat = kpi.get('latest') or {}
        if lat.get('units') != units:
            issues.append(f'kpi.json latest.units {lat.get("units")} != CSV {ym} {units}')
        mtd = kpi.get('mtd')
        if mtd:
            msum, mrows = 0, 0
            with open(os.path.join(data_dir, 'resale_daily.csv'),
                      encoding='utf-8-sig') as f:
                for r in csv.DictReader(f):
                    if str(r['日期']).startswith(str(mtd.get('period', ''))):
                        v2 = int(float(r['住宅签约套数']))
                        if v2 > 0:
                            msum += v2
                            mrows += 1
            if msum != mtd.get('units') or mrows != mtd.get('days'):
                issues.append(f'kpi.json mtd 不符: 独立重算 {mtd.get("period")}'
                              f'={msum}/{mrows} 天，json={mtd.get("units")}/{mtd.get("days")}')

    # 7) R5 累计口径独立重算（csv 直读）：TTM 末值=近 12 个连续月套数和；
    #    YTD 末值=最新年 1..n 月 vs 上年 1..n 月等窗同比
    cum_js = os.path.join(charts_dir, 'cum_view.json')
    if os.path.isfile(cum_js):
        with open(os.path.join(data_dir, 'resale_monthly.csv'), encoding='utf-8-sig') as f:
            rows_m = list(csv.DictReader(f))
        payload = json.loads(open(cum_js, encoding='utf-8').read())
        try:
            seqs = []
            for r in rows_m[-12:]:
                y, m = str(r['月份']).split('-')
                seqs.append(int(y) * 12 + int(m))
            if all(b - a == 1 for a, b in zip(seqs, seqs[1:])):
                ttm_csv = sum(int(r['住宅签约套数']) for r in rows_m[-12:])
                line = [v for v in payload['option']['series'][0]['data']
                        if v is not None]
                if line and abs(line[-1] - ttm_csv) > 0.5:
                    issues.append(f'TTM 不符: 图末值 {line[-1]} != 独立重算 {ttm_csv}')
            by = {}
            for r in rows_m:
                y, m = str(r['月份']).split('-')
                by.setdefault(int(y), {})[int(m)] = int(r['住宅签约套数'])
            ys = sorted(by)
            if len(ys) >= 2:
                cy, py_ = ys[-1], ys[-2]
                n = max(by[cy])
                if set(by[py_]) >= set(range(1, n + 1)):
                    c = sum(by[cy][m] for m in range(1, n + 1))
                    p = sum(by[py_][m] for m in range(1, n + 1))
                    ytd_csv = (c - p) / p * 100
                    bvals = [d['value'] if isinstance(d, dict) else d
                             for d in payload['option']['series'][1]['data']]
                    bv = [v for v in bvals if v is not None]
                    if bv and abs(bv[-1] - ytd_csv) > 0.1:
                        issues.append(f'YTD 不符: 图末值 {bv[-1]:.1f} != 独立重算 {ytd_csv:.1f}')
        except (KeyError, IndexError, TypeError, ValueError):
            issues.append('cum_view option 结构异常，累计口径断言无法执行')

    # 8) 非住宅占比值域（设计 §2.8 承诺）：structure_share 全部非空值 ∈(0,100)
    sh_js = os.path.join(charts_dir, 'structure_share.json')
    if os.path.isfile(sh_js):
        try:
            payload = json.loads(open(sh_js, encoding='utf-8').read())
            vals = [v for s in payload['option']['series'] for v in s['data']
                    if v is not None]
            bad_v = [v for v in vals if not (0 < v < 100)]
            if bad_v:
                issues.append(f'非住宅占比越界: {bad_v[:3]}（应 ∈(0,100)）')
        except (KeyError, IndexError, TypeError):
            issues.append('structure_share option 结构异常')

    # 9) 新房周签约独立重算（csv 直读 ≥2026-08-23 期房+现房签约住宅和 == 图序列总和）
    nw_js = os.path.join(charts_dir, 'new_weekly.json')
    nw_csv = os.path.join(data_dir, 'new_daily.csv')
    if os.path.isfile(nw_js) and os.path.isfile(nw_csv):
        try:
            payload = json.loads(open(nw_js, encoding='utf-8').read())
            bar = payload['option']['series'][0]['data']
            chart_sum = sum((b['value'] if isinstance(b, dict) else b)
                            for b in bar if b is not None)
            csv_sum = 0.0
            with open(nw_csv, encoding='utf-8-sig') as f:
                for r in csv.DictReader(f):
                    if str(r.get('日期', '')) >= '2026-08-23':
                        for c in ('期房签约住宅套数', '现房签约住宅套数'):
                            v = float(r.get(c) or -1)
                            if v > 0:
                                csv_sum += v
            if abs(chart_sum - csv_sum) > 0.5:
                issues.append(f'新房周签约不符: 图总和 {chart_sum:.0f} '
                              f'!= 独立重算 {csv_sum:.0f}')
        except (KeyError, IndexError, TypeError):
            issues.append('new_weekly option 结构异常')

    # 10) 周度叙述事实门（W4，设计 v1.4 §2.4）：存在性 fail-closed / 降级三方
    #     一致性 / 独立重算直比（float 容差 0.1）/ 段内条件化锚点 / 措辞门
    daily_csv = os.path.join(data_dir, 'resale_daily.csv')
    daily_nonempty = False
    if os.path.isfile(daily_csv):
        with open(daily_csv, encoding='utf-8-sig') as fh:
            for r in csv.DictReader(fh):
                try:
                    if int(float(r.get('住宅签约套数') or 0)) > 0:
                        daily_nonempty = True
                        break
                except (TypeError, ValueError):
                    continue
    wk_path = os.path.join(report_dir, 'wk_facts.json')
    has_wk = os.path.isfile(wk_path)
    if daily_nonempty and not has_wk:
        issues.append('wk_facts.json 缺失（daily 非空必写，fail-closed）')
    if not daily_nonempty and has_wk:
        issues.append('wk_facts.json 残留（daily 为空应删除）')
    if has_wk and daily_nonempty:
        try:
            wk = json.loads(open(wk_path, encoding='utf-8').read())
            re_f = _wk_facts_from_csv(data_dir)
        except Exception as e:
            wk, re_f = None, None
            issues.append(f'周度事实重算/读取失败: {e}')
        if wk is not None:
            for k in _WK_FIELDS:
                a, b = wk.get(k), re_f.get(k)
                if isinstance(b, float):
                    if a is None or abs(float(a) - b) > 0.1:
                        issues.append(f'wk_facts.{k} 不符: json={a} != 重算={round(b, 3)}')
                elif a != b:
                    issues.append(f'wk_facts.{k} 不符: json={a} != 重算={b}')
            if os.path.isfile(md_path):
                n_delim = md.count('<!-- wk-narrative -->')
                if wk.get('narrative') and n_delim != 2:
                    issues.append(f'叙述界定符 {n_delim} 个（narrative=true 应恰 2）')
                if not wk.get('narrative') and n_delim != 0:
                    issues.append(f'叙述界定符 {n_delim} 个（narrative=false 应为 0）')
                if n_delim == 2:
                    seg = md.split('<!-- wk-narrative -->')[1]
                    for bad in WK_BANNED_WORDS:
                        if bad in seg:
                            issues.append(f'叙述段含禁用词: {bad}')
                    nums = re.findall(r'\d{1,3}(?:,\d{3})+|日均 \d+|\d+(?:\.\d+)?%', seg)
                    if len(nums) > 8:
                        issues.append(f'叙述段数字预算超限: {len(nums)} > 8')
                    if wk.get('direction') in ('up', 'down'):
                        if f"{wk.get('last_full_units'):,}" not in seg:
                            issues.append('叙述段缺末完整周套数千分位锚点')
                    for pk in ('peak13_units', 'trough13_units'):
                        if wk.get(pk) is not None and f'{wk[pk]:,}' not in seg:
                            issues.append(f'叙述段缺 {pk} 千分位锚点')

    return (not issues), issues
