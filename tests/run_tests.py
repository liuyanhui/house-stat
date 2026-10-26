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
