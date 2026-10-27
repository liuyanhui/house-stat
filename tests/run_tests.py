# -*- coding: utf-8 -*-
"""零依赖测试入口（开发计划 T1.a）。

用法：PYTHONPATH= py -3.14 tests/run_tests.py [名称过滤串]

用例 = 本文件内 test_* 函数；任一失败非零退出。
fixture 由 tests/fixtures/make_fixtures.py 生成到临时目录（随用随删）。
"""
import importlib.util
import os
import sys
import tempfile
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

_spec = importlib.util.spec_from_file_location(
    'make_fixtures', os.path.join(HERE, 'fixtures', 'make_fixtures.py'))
make_fixtures = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(make_fixtures)


def test_fixtures_generate_and_read_back():
    """四分支 fixture 可生成、可被 pandas 读回、关键性质成立（行为断言随 T1.2/T1.3 补）。"""
    import pandas as pd
    with tempfile.TemporaryDirectory() as tmp:
        paths = make_fixtures.write_all(tmp)
        assert len(paths) == 5

        m = pd.read_csv(paths['gap_monthly'], encoding='utf-8-sig')
        months = {str(x) for x in m['月份']}
        assert '2025-7' not in months and '2025-07' not in months and len(m) == 9

        d = pd.read_csv(paths['est_week'], encoding='utf-8-sig')
        assert len(d) == 7 and str(d['日期'].iloc[0]) == '2025-05-05'

        a = pd.read_csv(paths['agency_gap'], encoding='utf-8-sig')
        assert set(a['年月']) == {'2026-04', '2026-07', '2026-08'}

        n = pd.read_csv(paths['short_new'], encoding='utf-8-sig')
        assert len(n) == 61
        assert (n['可售期房住宅套数'] > 0).all()          # 库存列全程有效
        before = n[n['日期'] < '2026-08-23']
        after = n[n['日期'] >= '2026-08-23']
        assert (before['期房签约住宅套数'] == -1).all()   # 签约列 8-23 前占位
        assert (after['期房签约住宅套数'] > 0).all()


def test_quality_single_source():
    """根 quality.py 可导入、零项目内依赖、内容完整（T1.1）。"""
    import quality
    assert quality.ESTIMATED_DAYS and quality.KNOWN_ISSUES
    assert '2026-09-16' in quality.ESTIMATED_DAYS
    assert ('2025-01', '住宅签约面积>网上签约面积') in quality.KNOWN_ISSUES

    assert quality.span_text(['2026-02', '2026-09', '2026-02']) == '2026-02 ~ 2026-09'
    assert quality.span_text([]) == '—'

    meta = quality.dataset_meta(
        {'resale_monthly': ['2025-01', '2026-09'], 'area_monthly': ['2025-12', '2026-09']},
        generated_at='2026-10-26 12:00')
    assert meta['resale_monthly']['coverage'] == '2025-01 ~ 2026-09'
    assert meta['resale_monthly']['generated_at'] == '2026-10-26 12:00'
    assert meta['resale_monthly']['known_issues']
    assert meta['resale_monthly']['estimated_days'] == {}  # 估算日只挂 resale_daily
    assert meta['area_monthly']['missing_months'] == ['2026-04']

    meta_d = quality.dataset_meta({'resale_daily': ['2025-04-21', '2026-10-24']}, generated_at='x')
    assert '2026-09-16' in meta_d['resale_daily']['estimated_days']


def test_load_new_daily_column_level():
    """T1.2：new_daily 列级清洗——库存列全程有效，签约列 8-23 前为 NaN（禁行级过滤）。"""
    from analysis import load
    with tempfile.TemporaryDirectory() as tmp:
        make_fixtures.write_short_new(tmp)
        n = load.load_new_daily(data_dir=tmp)
        assert len(n) == 61
        assert n['可售期房住宅套数'].notna().all()          # 库存列 8-23 前也有值
        before = n[n['日期'] < '2026-08-23']
        after = n[n['日期'] >= '2026-08-23']
        assert before['期房签约住宅套数'].isna().all()      # 签约列占位→NaN
        assert after['期房签约住宅套数'].notna().all()


def test_load_agency_gap():
    """T1.2：agency 载入弃发布套数列、断档月自然缺席（2026-05/06）。"""
    from analysis import load
    with tempfile.TemporaryDirectory() as tmp:
        make_fixtures.write_agency_gap(tmp)
        a = load.load_agency(data_dir=tmp)
        assert '发布套数' not in a.columns
        assert set(a['period'].astype(str)) == {'2026-04', '2026-07', '2026-08'}
        assert len(a) == 9


def test_gap_monthly_pct_change_nan():
    """T1.3：断档环比——缺 07 月后，2025-08 环比必须 NaN，连续段内正常。"""
    import numpy as np
    from analysis import load, metrics
    with tempfile.TemporaryDirectory() as tmp:
        make_fixtures.write_gap_monthly(tmp)
        m = metrics.add_yoy_mom(load.load_monthly(tmp))
        m = m.set_index(m['period'].astype(str))
        assert np.isnan(m.loc['2025-08', '环比'])       # 上一个是 06，跨档
        assert not np.isnan(m.loc['2025-03', '环比'])


def test_yoy_requires_continuous_13m():
    """T1.3：同比门控——有去年同月但连续段不足 13 个月 → NaN。"""
    from analysis import load, metrics
    with tempfile.TemporaryDirectory() as tmp:
        make_fixtures.write_yoy_gap_monthly(tmp)  # 2024-01~04 + 2025-01~04
        m = metrics.add_yoy_mom(load.load_monthly(tmp))
        assert m['同比'].isna().all()


def test_weekly_est_propagation():
    """T1.3：估算标记随聚合传播——周 est_days、MA4 窗口标记。"""
    from analysis import load, metrics
    with tempfile.TemporaryDirectory() as tmp:
        make_fixtures.write_est_week_daily(tmp)
        w = metrics.weekly_aggregate(
            load.load_daily(tmp), estimated_dates=['2025-05-06', '2025-05-07'])
        assert len(w) == 1
        assert w['est_days'].iloc[0] == 2
        assert bool(w['MA4含估算'].iloc[0])


def test_sanitize_bad_values():
    """T1.3：2025-01 网上签约面积坏值 → NaN，其余月份不动（真实数据）。"""
    from analysis import load, metrics
    s = metrics.sanitize_bad_values(load.load_monthly())
    row = s[s['period'].astype(str) == '2025-01']
    assert row['网上签约面积(m2)'].isna().all()
    assert s['网上签约面积(m2)'].notna().sum() == len(s) - 1


def test_expand_estimated_days():
    """T1.3：'25/26' 简写展开为具体日期。"""
    from analysis import metrics
    days = metrics.expand_estimated_days({'2026-07-25/26': 'x', '2026-09-16': 'y'})
    assert days == ['2026-07-25', '2026-07-26', '2026-09-16']


def test_charts_engine_bars_lines():
    """T1.4a：bars_lines 双 sink——PNG 产出、JSON 合法无 NaN 字面量、正负着色正确。"""
    import json as _json
    import os
    import palette
    from analysis import charts
    spec = {
        'name': 'ut_bars_lines', 'kind': 'bars_lines', 'title': '单测', 'figsize': (8, 4),
        'x': {'labels': ['25-01', '25-02', '25-03'], 'rot': 45},
        'left': {'label': '套数'}, 'right': {'label': '同比 %'},
        'series': [
            {'type': 'bar', 'name': '套数', 'values': [100.0, float('nan'), 300.0],
             'color': palette.PALETTE['bar_main'], 'alpha': 0.55},
            {'type': 'line', 'name': 'MA', 'values': [None, 200.0, 300.0],
             'color': palette.PALETTE['line_ma']},
            {'type': 'bar', 'name': '同比', 'values': [5.0, -3.0, None], 'axis': 'right',
             'sign_color': True, 'alpha': 0.35, 'width': 0.5},
        ],
        'annotations': [],
    }
    with tempfile.TemporaryDirectory() as tmp:
        charts.write_chart(spec, out_dir=tmp)
        assert os.path.getsize(os.path.join(tmp, 'ut_bars_lines.png')) > 0
        text = open(os.path.join(tmp, 'charts', 'ut_bars_lines.json'), encoding='utf-8').read()
        assert 'NaN' not in text and 'Infinity' not in text
        data = _json.loads(text)
        assert data['option']['series'][0]['data'] == [100.0, None, 300.0]
        assert data['height'] > 0
        opt = charts.to_echarts(spec)
        assert opt['series'][2]['data'][0]['itemStyle']['color'] == palette.PALETTE['up']
        assert opt['series'][2]['data'][1]['itemStyle']['color'] == palette.PALETTE['down']
        assert opt['series'][2]['yAxisIndex'] == 1


def test_charts_engine_families():
    """T1.4a：lines/stacked_area/heatmap/smallmul 四家族双 sink 均可产出。"""
    import os
    from analysis import charts
    specs = [
        {'name': 'ut_lines', 'kind': 'lines', 'title': 't', 'figsize': (8, 3),
         'x': {'labels': ['a', 'b', 'c']},
         'left': {'label': 'm²'},
         'series': [{'type': 'line', 'name': '套均', 'values': [89.0, 90.0, None], 'marker': True},
                    {'type': 'line', 'name': '趋势', 'values': [88.5, 89.5, 90.5], 'dashed': True}]},
        {'name': 'ut_stack', 'kind': 'stacked_area', 'title': 't', 'figsize': (8, 4),
         'x': {'labels': ['a', 'b', 'c']}, 'left': {'label': '份额 %'}, 'y_max': 100,
         'series': [{'name': 'A', 'values': [50.0, 40.0, 30.0]},
                    {'name': 'B', 'values': [50.0, 60.0, 70.0]}]},
        {'name': 'ut_heat', 'kind': 'heatmap', 'title': 't',
         'x': {'labels': ['1月', '2月']}, 'y': {'labels': ['2025', '2026']},
         'matrix': [[1000.0, 1200.0], [float('nan'), 1500.0]],
         'value_fmt': 'int', 'colorbar_label': '套数'},
        {'name': 'ut_smallmul', 'kind': 'smallmul', 'title': 't', 'ncols': 2,
         'x': {'labels': ['a', 'b', 'c']},
         'panels': [{'title': '朝阳', 'values': [1.0, 2.0, 3.0]},
                    {'title': '海淀', 'values': [3.0, 2.0, 1.0]}]},
    ]
    with tempfile.TemporaryDirectory() as tmp:
        for spec in specs:
            charts.write_chart(spec, out_dir=tmp)
            assert os.path.getsize(os.path.join(tmp, spec['name'] + '.png')) > 0
            opt = charts.to_echarts(spec)   # 不抛异常即契约成立
            assert opt['series']
        heat_opt = charts.to_echarts(specs[2])
        assert heat_opt['visualMap']['max'] == 1500.0


def test_html_render_chart_injection():
    """T1.6a：同 stem charts/*.json + vendor → 注入 div+数据块+bootstrap；</ 转义。"""
    import json as _json
    import os
    from analysis import html_render as hr
    with tempfile.TemporaryDirectory() as tmp:
        os.makedirs(os.path.join(tmp, 'charts'))
        with open(os.path.join(tmp, 'a.png'), 'wb') as f:
            f.write(b'\x89PNG\r\n\x1a\n')
        payload = {'name': 'a', 'kind': 'lines', 'height': 320,
                   'option': {'title': {'text': 'x</script>'}, 'series': []}}
        with open(os.path.join(tmp, 'charts', 'a.json'), 'w', encoding='utf-8') as f:
            _json.dump(payload, f, ensure_ascii=False)
        vendor = os.path.join(tmp, 'v.js')
        with open(vendor, 'w', encoding='utf-8') as f:
            f.write('/*vendor*/')
        html = hr.md_to_html('![t](a.png)\n', image_dir=tmp,
                             charts_dir=os.path.join(tmp, 'charts'),
                             vendor_path=vendor, no_js=False)
        assert '<div class="chart" data-chart="a"' in html
        assert 'id="chart-data"' in html and '/*vendor*/' in html
        assert 'IntersectionObserver' in html
        assert '<\\/script' in html          # </ 转义防 <script> 提前闭合
        assert '<img' not in html            # 交互模式不再内嵌 PNG


def test_html_render_nojs_fallback():
    """T1.6b：no_js → 纯 PNG 模式（base64 内嵌，无任何 JS）。"""
    import os
    from analysis import html_render as hr
    with tempfile.TemporaryDirectory() as tmp:
        os.makedirs(os.path.join(tmp, 'charts'))
        with open(os.path.join(tmp, 'a.png'), 'wb') as f:
            f.write(b'\x89PNG\r\n\x1a\n')
        with open(os.path.join(tmp, 'charts', 'a.json'), 'w', encoding='utf-8') as f:
            f.write('{"name":"a","kind":"lines","height":320,"option":{}}')
        vendor = os.path.join(tmp, 'v.js')
        with open(vendor, 'w', encoding='utf-8') as f:
            f.write('/*vendor*/')
        html = hr.md_to_html('![t](a.png)\n', image_dir=tmp,
                             charts_dir=os.path.join(tmp, 'charts'),
                             vendor_path=vendor, no_js=True)
        assert 'data:image/png;base64' in html
        assert 'chart-data' not in html and '<script>' not in html


def test_html_render_comment_passthrough_and_toc():
    """T1.6：HTML 注释直通不被转义；<!-- toc --> 生成计数器 id 目录。"""
    from analysis import html_render as hr
    md = '# T\n\n<!-- keep -->\n\n<!-- toc -->\n\n## 一、A\n\ntext\n\n### 小节\n'
    html = hr.md_to_html(md)
    assert '<!-- keep -->' in html and '&lt;!--' not in html
    assert '<nav class="toc">' in html and 'href="#sec-0"' in html
    assert '<h2 id="sec-0"' in html and '<h3 id="sec-1"' in html


def test_verify_gate():
    """T1.7：验收门——锚点独立重算（CSV 直读 vs 图表 JSON/md），坏值被抓住。"""
    import json as _json
    import os
    from analysis import verify


    def build(tmp, last_units, tamper=None):
        os.makedirs(os.path.join(tmp, 'charts'), exist_ok=True)
        for stem in verify.CHART_STEMS:
            with open(os.path.join(tmp, stem + '.png'), 'wb') as f:
                f.write(b'x')
            with open(os.path.join(tmp, 'charts', stem + '.json'), 'w', encoding='utf-8') as f:
                _json.dump({'name': stem, 'kind': 'lines', 'height': 100,
                            'option': {'series': []}}, f)
        city = {'name': 'city_trend', 'kind': 'bars_lines', 'height': 100,
                'option': {'series': [{'type': 'bar', 'data': [1, last_units]}]}}
        if tamper == 'nan':
            with open(os.path.join(tmp, 'charts', 'city_trend.json'), 'w', encoding='utf-8') as f:
                f.write('{"option": {"series": [{"data": [1, NaN]}]}}')
        else:
            with open(os.path.join(tmp, 'charts', 'city_trend.json'), 'w', encoding='utf-8') as f:
                _json.dump(city, f, ensure_ascii=False)
        with open(os.path.join(tmp, 'trend_report.md'), 'w', encoding='utf-8') as f:
            f.write(f'最新月 {last_units:,} 套')
        with open(os.path.join(tmp, 'trend_report.html'), 'w', encoding='utf-8') as f:
            f.write('<script id="chart-data"></script>')

    with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as data:
        with open(os.path.join(data, 'resale_monthly.csv'), 'w', encoding='utf-8-sig') as f:
            f.write('月份,网上签约套数,网上签约面积(m2),住宅签约套数,住宅签约面积(m2)\n'
                    '2026-08,1,1,1,1\n2026-09,2,2,12345,2\n')
        build(tmp, 12345)
        ok, issues = verify.check(tmp, data)
        assert ok, issues                       # 全对 → 过
        build(tmp, 99999)                       # 锚点错 → 抓住
        ok, issues = verify.check(tmp, data)
        assert not ok and any('锚点不符' in i for i in issues)
        build(tmp, 12345, tamper='nan')         # NaN 字面量 → 抓住
        ok, issues = verify.check(tmp, data)
        assert not ok and any('NaN' in i for i in issues)


def test_baseline_diff_whitelist_gate():
    """T1.8 补：白名单机制本身可测——白名单外差异必须非零退出。"""
    import subprocess
    import sys as _sys
    script = os.path.join(HERE, 'baseline_diff.py')
    with tempfile.TemporaryDirectory() as tmp:
        base = os.path.join(tmp, 'base.md')
        cur = os.path.join(tmp, 'cur.md')
        wl = os.path.join(tmp, 'wl.txt')
        with open(base, 'w', encoding='utf-8') as f:
            f.write('a\n')
        with open(cur, 'w', encoding='utf-8') as f:
            f.write('a\nB2\n')   # 纯增行：diff 只有 +B2 一行
        with open(wl, 'w', encoding='utf-8') as f:
            f.write('B2\n')
        r = subprocess.run([_sys.executable, script, base, cur, wl],
                           capture_output=True, text=True, encoding='utf-8')
        assert r.returncode == 0, r.stdout + r.stderr      # 差异命中白名单
        with open(wl, 'w', encoding='utf-8') as f:
            f.write('zzz\n')
        r = subprocess.run([_sys.executable, script, base, cur, wl],
                           capture_output=True, text=True, encoding='utf-8')
        assert r.returncode == 1                            # 白名单外 → 非零
        assert '>>' in r.stdout


def test_pixel_diff_exit_codes():
    """T1.8 补：像素 diff 三分支退出码（0 相同 / 1 超容差 / 2 尺寸不一致）。"""
    import subprocess
    import sys as _sys
    from PIL import Image
    script = os.path.join(HERE, 'pixel_diff.py')
    with tempfile.TemporaryDirectory() as tmp:
        p1, p2, p3, p4 = (os.path.join(tmp, n) for n in ('a.png', 'b.png', 'c.png', 'd.png'))
        Image.new('RGB', (20, 20), (255, 255, 255)).save(p1)
        Image.new('RGB', (20, 20), (255, 255, 255)).save(p2)
        Image.new('RGB', (20, 20), (0, 0, 0)).save(p3)
        Image.new('RGB', (30, 20), (255, 255, 255)).save(p4)

        def run(x, y):
            return subprocess.run([_sys.executable, script, x, y],
                                  capture_output=True, text=True,
                                  encoding='utf-8').returncode
        assert run(p1, p2) == 0
        assert run(p1, p3) == 1
        assert run(p1, p4) == 2


def test_verify_gate_branches():
    """T1.8 补：缺 PNG / 多余 JSON / 体积门 / vendor 缺失分支。"""
    import json as _json
    from analysis import verify

    def build(tmp):
        os.makedirs(os.path.join(tmp, 'charts'), exist_ok=True)
        for stem in verify.CHART_STEMS:
            with open(os.path.join(tmp, stem + '.png'), 'wb') as f:
                f.write(b'x')
            with open(os.path.join(tmp, 'charts', stem + '.json'), 'w', encoding='utf-8') as f:
                _json.dump({'name': stem, 'kind': 'lines', 'height': 100,
                            'option': {'series': [{'type': 'bar', 'data': [12345]}]}}, f)
        with open(os.path.join(tmp, 'trend_report.md'), 'w', encoding='utf-8') as f:
            f.write('12,345')
        with open(os.path.join(tmp, 'trend_report.html'), 'w', encoding='utf-8') as f:
            f.write('<script id="chart-data"></script>')

    with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as data:
        with open(os.path.join(data, 'resale_monthly.csv'), 'w', encoding='utf-8-sig') as f:
            f.write('月份,网上签约套数,网上签约面积(m2),住宅签约套数,住宅签约面积(m2)\n'
                    '2026-09,2,2,12345,2\n')
        build(tmp)
        ok, _ = verify.check(tmp, data)
        assert ok
        os.remove(os.path.join(tmp, 'seasonality.png'))
        with open(os.path.join(tmp, 'charts', 'extra.json'), 'w', encoding='utf-8') as f:
            f.write('{}')
        with open(os.path.join(tmp, 'trend_report.html'), 'w', encoding='utf-8') as f:
            f.write('x' * (verify.HTML_SIZE_LIMIT + 1))
        old_vendor = verify.VENDOR_PATH
        verify.VENDOR_PATH = os.path.join(tmp, 'nope.js')
        try:
            ok, issues = verify.check(tmp, data)
        finally:
            verify.VENDOR_PATH = old_vendor
        assert not ok
        assert any('缺 PNG' in i for i in issues)
        assert any('多余' in i for i in issues)
        assert any('体积超限' in i for i in issues)
        assert any('vendor 缺失' in i for i in issues)


def test_vendor_missing_degrades():
    """T1.8 补：vendor 缺失（非 NO_JS 显式开关）→ 自动退化 PNG 模式。"""
    from analysis import html_render as hr
    with tempfile.TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, 'a.png'), 'wb') as f:
            f.write(b'\x89PNG\r\n\x1a\n')
        html = hr.md_to_html('![t](a.png)\n', image_dir=tmp,
                             charts_dir=os.path.join(tmp, 'charts'),
                             vendor_path=os.path.join(tmp, 'nope.js'), no_js=False)
        assert 'data:image/png;base64' in html and 'chart-data' not in html


def test_preps_smoke():
    """T1.8 补：十个 prep 真实数据全量冒烟（spec 结构完整、option 可产）。"""
    from analysis import load, metrics, charts
    monthly = load.load_monthly()
    monthly_ma = metrics.add_moving_avg(metrics.add_yoy_mom(monthly), windows=(12,))
    weekly = metrics.weekly_aggregate(load.load_daily())
    aua = metrics.avg_unit_area(monthly)
    district, area = load.load_district(), load.load_area()
    specs = [charts.prep_city_trend(monthly_ma), charts.prep_seasonality(monthly),
             charts.prep_avg_area_trend(aua), charts.prep_weekly_trend(weekly),
             charts.prep_weekly_avg_area(weekly), charts.prep_district_stacked(district),
             charts.prep_district_share(district), charts.prep_district_smallmul(district),
             charts.prep_area_segment_count(area), charts.prep_area_segment_share(area)]
    assert all(s is not None for s in specs)
    for s in specs:
        assert s['name'] and s['kind'] in ('bars_lines', 'lines', 'stacked_area',
                                           'heatmap', 'smallmul')
        assert 'annotations' in s and 'title' in s
        charts.to_echarts(s)


def test_injection_guards():
    """T2.7：缺 option 键的 JSON 退化 PNG；行内图片注入标签不被转义。"""
    import json as _json
    import os
    from analysis import html_render as hr
    with tempfile.TemporaryDirectory() as tmp:
        os.makedirs(os.path.join(tmp, 'charts'))
        with open(os.path.join(tmp, 'a.png'), 'wb') as f:
            f.write(b'\x89PNG\r\n\x1a\n')
        with open(os.path.join(tmp, 'charts', 'a.json'), 'w', encoding='utf-8') as f:
            f.write('{"name":"a","kind":"lines","height":320}')   # 无 option 键
        vendor = os.path.join(tmp, 'v.js')
        with open(vendor, 'w', encoding='utf-8') as f:
            f.write('/*v*/')
        kw = dict(image_dir=tmp, charts_dir=os.path.join(tmp, 'charts'),
                  vendor_path=vendor, no_js=False)
        html = hr.md_to_html('![t](a.png)\n', **kw)
        assert 'data:image/png;base64' in html and 'class="chart"' not in html

        # 行内图片（段中）：正常 option → 注入标签且不被转义成可见文本
        with open(os.path.join(tmp, 'charts', 'a.json'), 'w', encoding='utf-8') as f:
            _json.dump({'name': 'a', 'kind': 'lines', 'height': 320,
                        'option': {'series': [{'type': 'line', 'data': [1]}]}}, f)
        html = hr.md_to_html('前文 ![t](a.png) 后文\n', **kw)
        assert '<div class="chart"' in html and '&lt;div' not in html
        assert 'data-chart="a"' in html


def test_kpi_cards_render():
    """T2.3：kpi.json → 静态卡片；占位清空路径。"""
    import json as _json
    import os
    from analysis import html_render as hr
    with tempfile.TemporaryDirectory() as tmp:
        kpi = {'latest': {'period': '2026-09', 'units': 15756, 'mom': 13.7,
                          'yoy': -0.5, 'avg_area': 89.0},
               'mtd': {'period': '2026-10', 'units': 8000, 'days': 24,
                       'note': '当月至今·非全月'}}
        with open(os.path.join(tmp, 'kpi.json'), 'w', encoding='utf-8') as f:
            _json.dump(kpi, f, ensure_ascii=False)
        html = hr.md_to_html('# T\n\n<!-- kpi -->\n', kpi_path=os.path.join(tmp, 'kpi.json'))
        assert 'class="kpi-grid"' in html and '15,756' in html and '+13.7%' in html
        assert '当月至今·非全月' in html and '勿与完整月直接比较' in html
        assert '<!-- kpi -->' not in html
        html2 = hr.md_to_html('# T\n\n<!-- kpi -->\n', kpi_path=os.path.join(tmp, 'nope.json'))
        assert '<!-- kpi -->' not in html2 and 'class="kpi-grid"' not in html2


def test_verify_kpi_branch():
    """T2.3：验收门 kpi.json 直比分支。"""
    import json as _json
    from analysis import verify

    with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as data:
        with open(os.path.join(data, 'resale_monthly.csv'), 'w', encoding='utf-8-sig') as f:
            f.write('月份,网上签约套数,网上签约面积(m2),住宅签约套数,住宅签约面积(m2)\n'
                    '2026-09,2,2,12345,2\n')
        with open(os.path.join(data, 'resale_daily.csv'), 'w', encoding='utf-8-sig') as f:
            f.write('日期,签约套数,签约面积,住宅签约套数,住宅签约面积\n'
                    '2026-10-01,1,1,100,1\n2026-10-02,1,1,50,1\n')
        os.makedirs(os.path.join(tmp, 'charts'))
        for stem in verify.CHART_STEMS:
            with open(os.path.join(tmp, stem + '.png'), 'wb') as f:
                f.write(b'x')
            with open(os.path.join(tmp, 'charts', stem + '.json'), 'w', encoding='utf-8') as f:
                _json.dump({'name': stem, 'kind': 'lines', 'height': 100,
                            'option': {'series': [{'type': 'bar', 'data': [12345]}]}}, f)
        with open(os.path.join(tmp, 'trend_report.md'), 'w', encoding='utf-8') as f:
            f.write('12,345')
        with open(os.path.join(tmp, 'trend_report.html'), 'w', encoding='utf-8') as f:
            f.write('<script id="chart-data"></script>')
        kpi = {'latest': {'units': 12345}, 'mtd': {'period': '2026-10', 'units': 150, 'days': 2}}
        with open(os.path.join(tmp, 'kpi.json'), 'w', encoding='utf-8') as f:
            _json.dump(kpi, f)
        ok, issues = verify.check(tmp, data)
        assert ok, issues
        kpi['latest']['units'] = 999
        kpi['mtd']['units'] = 999
        with open(os.path.join(tmp, 'kpi.json'), 'w', encoding='utf-8') as f:
            _json.dump(kpi, f)
        ok, issues = verify.check(tmp, data)
        assert not ok
        assert any('kpi.json latest.units' in i for i in issues)
        assert any('kpi.json mtd' in i for i in issues)


def test_quality_annotations():
    """T2.2：估算周红框（双 sink）、面积段缺失月可见断口、quality.json 产出。"""
    import os
    from analysis import load, metrics, charts
    # 估算周：fixture 周（2 天估算）→ 柱带红框
    with tempfile.TemporaryDirectory() as tmp:
        make_fixtures.write_est_week_daily(tmp)
        w = metrics.weekly_aggregate(load.load_daily(tmp),
                                     estimated_dates=['2025-05-06', '2025-05-07'])
        spec = charts.prep_weekly_trend(w)
        assert spec['annotations']['est_weeks']
        opt = charts.to_echarts(spec)
        bars = opt['series'][0]['data']
        assert any(isinstance(b, dict) and b['itemStyle'].get('borderColor')
                   for b in bars)
        charts.write_chart(spec, out_dir=tmp)   # mpl 边框路径不抛错

    # 面积段：2026-04 以可见空列出现在 x 轴，该列全 None
    spec = charts.prep_area_segment_count(load.load_area())
    assert '26-04' in spec['x']['labels']
    assert spec['annotations']['missing_labels'] == ['26-04']
    j = spec['x']['labels'].index('26-04')
    assert all(row[j] is None for row in charts._sanitize(spec['matrix']))

    # quality.json：现场生成后验六字段（自足，不依赖历史产物）
    from analysis import report as report_mod
    report_mod.generate()
    assert os.path.isfile('report/quality.json')
    import json as _json
    q = _json.loads(open('report/quality.json', encoding='utf-8').read())
    assert 'resale_monthly' in q and q['resale_monthly']['coverage']
    assert all(k in q['resale_daily'] for k in
               ('coverage', 'estimated_days', 'missing_months', 'known_issues',
                'notes', 'generated_at'))


def main():
    only = sys.argv[1] if len(sys.argv) > 1 else ''
    tests = [(n, f) for n, f in sorted(globals().items())
             if n.startswith('test_') and callable(f)]
    fails = []
    for name, fn in tests:
        if only and only not in name:
            continue
        try:
            fn()
            print(f'PASS {name}')
        except Exception:
            print(f'FAIL {name}')
            traceback.print_exc()
            fails.append(name)
    print(f'\n{len(tests) - len(fails)}/{len(tests)} passed')
    sys.exit(1 if fails else 0)


if __name__ == '__main__':
    main()
