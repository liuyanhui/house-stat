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
    md = '# T\n\n<!-- kpi -->\n\n<!-- toc -->\n\n## 一、A\n\ntext\n\n### 小节\n'
    html = hr.md_to_html(md)
    assert '<!-- kpi -->' in html and '&lt;!--' not in html
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
