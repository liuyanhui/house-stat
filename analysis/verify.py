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

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VENDOR_PATH = os.path.join(_ROOT, 'assets', 'vendor', 'echarts.min.js')
HTML_SIZE_LIMIT = 2 * 1024 * 1024

CHART_STEMS = ['city_trend', 'seasonality', 'avg_unit_area', 'weekly_trend',
               'weekly_avg_area', 'district_stacked', 'district_share',
               'district_smallmul', 'area_segment_count', 'area_segment_share']

_VENDOR_MARKERS = ['heatmap', 'dataZoom', 'toolbox', 'markLine',
                   'visualMap', 'legend', 'tooltip']


def read_last_monthly(data_dir):
    """csv 直读 resale_monthly 尾行 → (年月字符串, 住宅签约套数)。独立路径。"""
    path = os.path.join(data_dir, 'resale_monthly.csv')
    with open(path, encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))
    last = rows[-1]
    return str(last['月份']), int(last['住宅签约套数'])


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

    return (not issues), issues
