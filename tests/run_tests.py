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
        assert len(paths) == 4

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
