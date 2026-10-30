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


def _write_chart_stubs(tmp):
    """verify 夹具共用：全 stems 占位 PNG+JSON（structure_share 须 ∈(0,100)）。"""
    import json as _json
    import os
    from analysis import verify
    os.makedirs(os.path.join(tmp, 'charts'), exist_ok=True)
    for stem in verify.CHART_STEMS:
        with open(os.path.join(tmp, stem + '.png'), 'wb') as f:
            f.write(b'x')
        vals = [50] if stem == 'structure_share' else [12345]
        with open(os.path.join(tmp, 'charts', stem + '.json'), 'w', encoding='utf-8') as f:
            _json.dump({'name': stem, 'kind': 'lines', 'height': 100,
                        'option': {'series': [{'type': 'bar', 'data': vals}]}}, f)


def test_chart_stems_registry():
    """verify.CHART_STEMS 与 charts.prep_* 一一对应（防手抄清单漂移）。"""
    from analysis import charts, verify
    preps = {n[5:] for n in dir(charts) if n.startswith('prep_')}
    assert preps == set(verify.CHART_STEMS), preps ^ set(verify.CHART_STEMS)


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


def _mk_weekly(starts, units, est=None):
    """W1 测试共用：最小周表（weekly_trend_facts 只消费这 4 列）。"""
    import pandas as pd
    n = len(starts)
    return pd.DataFrame({
        'week_start': pd.to_datetime(starts),
        '套数': units,
        'days': [7] * n,
        'est_days': est if est is not None else [0] * n,
    })


def _mondays(start_iso, n):
    """从 start_iso（须为周一）起连续 n 个周一的日期串列表。"""
    import pandas as pd
    t0 = pd.Timestamp(start_iso)
    return [(t0 + pd.Timedelta(weeks=k)).strftime('%Y-%m-%d') for k in range(n)]


def test_weekly_facts_direction():
    """W1a：方向链——N=链长−1（差一修正）、节假日/持平/缺口中断、估算不中断、
    null 成因（tie/holiday_last/holiday_prev/gap）、last_pair_reversal、post_holiday、
    峰谷 tie-break。节假日用真实日期（2025 国庆：9-28 周调休上班、9-29/10-06 周假期）。
    注：各用例均 ≥8 完整周（narrative 门 <8 会字段最小化）。"""
    from analysis import metrics
    import pandas as pd

    # 节假日周原语：2025-09-29 周（含 10-01~03）与 10-06 周为假期周；
    # 9-22 周（含 9-28 调休上班）净额 6 → 非假期周（调休覆盖验证）
    assert metrics._is_holiday_week(pd.Timestamp('2025-09-29'))
    assert metrics._is_holiday_week(pd.Timestamp('2025-10-06'))
    assert not metrics._is_holiday_week(pd.Timestamp('2025-09-22'))

    # ① 连涨：N = 链长−1（8 周连涨 → N=7，非 8）
    f = metrics.weekly_trend_facts(_mk_weekly(_mondays('2025-06-02', 8),
                                              [3000, 3100, 3200, 3300, 3400, 3500, 3600, 3700]))
    assert f['direction'] == 'up' and f['streak_weeks'] == 7
    assert f['null_reason'] is None and not f['post_holiday']
    assert f['peak13_units'] is None            # 不足 13 周峰谷全 None

    # ② N=1 + 互现：末对涨、再前一对跌 → last_pair_reversal
    f = metrics.weekly_trend_facts(_mk_weekly(_mondays('2025-06-02', 8),
                                              [2500, 2600, 3000, 3200, 3400, 3200, 100, 110]))
    assert f['direction'] == 'up' and f['streak_weeks'] == 1
    assert f['last_pair_reversal'] is True

    # ③ 连跌：链 5 周（4000 起）→ N=4
    f = metrics.weekly_trend_facts(_mk_weekly(_mondays('2025-06-02', 8),
                                              [3000, 3100, 3200, 4000, 3900, 3800, 3700, 3600]))
    assert f['direction'] == 'down' and f['streak_weeks'] == 4

    # ④ 持平中断 → null/tie
    f = metrics.weekly_trend_facts(_mk_weekly(_mondays('2025-06-02', 8),
                                              [2500, 2600, 2700, 2800, 3000, 3200, 3300, 3300]))
    assert f['direction'] is None and f['null_reason'] == 'tie' and f['streak_weeks'] == 0

    # ⑤ 节假日周中断 + post_holiday：国庆两周假期后 10-13/10-20 两周涨 → N=1、节后
    starts = _mondays('2025-09-01', 8)          # 09-01..10-27
    units = [3300, 3400, 3500, 3600, 2000, 2100, 3200, 3400]
    f = metrics.weekly_trend_facts(_mk_weekly(starts, units))
    assert f['direction'] == 'up' and f['streak_weeks'] == 1
    assert f['post_holiday'] is True
    assert f['last_pair_reversal'] is False     # 10-06 假期周 → 再前一对不可比

    # ⑥ 末完整周本身是假期周 → holiday_last
    f = metrics.weekly_trend_facts(_mk_weekly(_mondays('2025-08-04', 10),
                                              [3000, 3050, 3100, 3150, 3200, 3250, 3300, 3350, 2000, 2100]))
    assert f['direction'] is None and f['null_reason'] == 'holiday_last'

    # ⑦ 末完整周前一周是假期周 → holiday_prev（节后首周）
    f = metrics.weekly_trend_facts(_mk_weekly(_mondays('2025-08-11', 10),
                                              [3000, 3050, 3100, 3150, 3200, 3250, 3300, 2000, 2100, 3200]))
    assert f['direction'] is None and f['null_reason'] == 'holiday_prev'

    # ⑧ 周序缺口 >7 天中断 → gap（实施补充的第 4 成因；缺口须紧邻末周）
    f = metrics.weekly_trend_facts(_mk_weekly(
        ['2025-05-26', '2025-06-02', '2025-06-09', '2025-06-16',
         '2025-06-23', '2025-06-30', '2025-07-07', '2025-07-21'],
        [3000, 3100, 3200, 3300, 3400, 3500, 3600, 3700]))
    assert f['direction'] is None and f['null_reason'] == 'gap'

    # ⑨ 估算周不中断，标记随链传播
    f = metrics.weekly_trend_facts(_mk_weekly(_mondays('2025-06-02', 8),
                                              [2800, 2900, 3000, 3100, 3200, 3300, 3400, 3500],
                                              est=[0, 0, 0, 0, 0, 1, 0, 0]))
    assert f['direction'] == 'up' and f['streak_weeks'] == 7
    assert f['streak_contains_est'] is True

    # ⑩ 峰谷 tie-break：假期周 09-29 与正常周 10-13 同为最大 → 取非节假日周；
    #    谷值落在假期周 10-06（保留候选）→ trough13_holiday=True
    starts = _mondays('2025-08-04', 13)         # 08-04..10-27，含 13 个完整周
    units = [3000, 3050, 3100, 3150, 3200, 3250, 3300, 3350, 4000, 2100, 4000, 3900, 3800]
    est = [0] * 13
    est[10] = 1                                 # 10-13 周含估算 1 天
    f = metrics.weekly_trend_facts(_mk_weekly(starts, units, est=est))
    assert f['peak13_week'] == '2025-10-13' and f['peak13_units'] == 4000
    assert f['peak13_est'] is True and f['peak13_holiday'] is False
    assert f['trough13_week'] == '2025-10-06' and f['trough13_holiday'] is True

    # ⑪ 链首值 + 锚句事实（实施增补字段）：链=[10-13,10-20,10-27] 连跌 N=2；
    #    avg4=3450，候选=排除链/节假日/近4周的 08-04..09-22，最近 3350(09-22) 差≤10%
    assert f['direction'] == 'down' and f['streak_weeks'] == 2
    assert f['chain_first_week'] == '2025-10-13' and f['chain_first_units'] == 4000
    assert f['anchor_kind'] == 'week' and f['anchor_week'] == '2025-09-22'


def test_weekly_facts_level():
    """W1b：水位——基准=剔近 4 周的最近 ≤26 完整周（防自比稀释）、±5% 死区、
    基准 <13 → level=null、avg26_weeks 退化计数。"""
    from analysis import metrics

    # ① above + 窗口锚定：34 周 = 4×9000（窗外）+ 26×2000（基准窗）+ 4×4000（avg4）
    units = [9000] * 4 + [2000] * 26 + [4000] * 4
    f = metrics.weekly_trend_facts(_mk_weekly(_mondays('2025-01-06', 34), units))
    assert f['narrative'] is True
    assert f['avg4'] == 4000.0 and f['avg26'] == 2000.0 and f['avg26_weeks'] == 26
    assert f['level'] == 'above' and f['level_gap_pct'] == 100.0

    # ② parity：死区内
    units = [3000] * 30
    f = metrics.weekly_trend_facts(_mk_weekly(_mondays('2025-01-06', 30), units))
    assert f['level'] == 'parity' and f['level_gap_pct'] == 0.0

    # ③ below：死区外
    units = [3000] * 26 + [2000] * 4
    f = metrics.weekly_trend_facts(_mk_weekly(_mondays('2025-01-06', 30), units))
    assert f['level'] == 'below' and f['level_gap_pct'] < -5

    # ④ 基准 <13 → level=null（avg26 仍出值）
    units = [3000] * 12 + [4000] * 4
    f = metrics.weekly_trend_facts(_mk_weekly(_mondays('2025-01-06', 16), units))
    assert f['avg26_weeks'] == 12 and f['avg26'] == 3000.0
    assert f['level'] is None and f['level_gap_pct'] is None

    # ⑤ 基准 <26 的退化计数（渲染层"有数据以来"依据）
    units = [3000] * 22 + [4000] * 4
    f = metrics.weekly_trend_facts(_mk_weekly(_mondays('2025-01-06', 26), units))
    assert f['avg26_weeks'] == 22 and f['level'] == 'above'


def _mk_daily(start, per_day_units):
    """W1b 测试共用：从 start 起按 per_day_units 逐日构造日表（面积=套数×90）。"""
    import pandas as pd
    dates = [pd.Timestamp(start) + pd.Timedelta(days=k) for k in range(len(per_day_units))]
    return pd.DataFrame({'日期': dates, '住宅签约套数': per_day_units,
                         '住宅签约面积': [u * 90 for u in per_day_units]})


def _daily_with_holes(rows):
    """[(date_iso, units), ...] 显式构造日表（含缺口/跳日；面积=套数×90）。"""
    import pandas as pd
    units = [r[1] for r in rows]
    return pd.DataFrame({'日期': pd.to_datetime([r[0] for r in rows]),
                         '住宅签约套数': units,
                         '住宅签约面积': [u * 90 for u in units]})


def test_weekly_facts_last_week():
    """W1b：末周三态 + 同期对齐——complete / in_progress（位置对齐、contiguous）、
    lagged（前一周缺天且非节假日）、假期周前周缺天不判 lagged、ip_* 源自 daily 子集。"""
    from analysis import metrics
    import pandas as pd

    # ① complete：末周满 7 天 → ip_* 全 None
    d = _mk_daily('2025-06-02', [100] * (7 * 8))
    f = metrics.weekly_trend_facts(metrics.weekly_aggregate(d), daily=d)
    assert f['last_week_state'] == 'complete'
    assert f['ip_units'] is None and f['ip_daily_avg'] is None

    # ② in_progress：8 满周 @100/日 + 末周一二三 @200/日（≥8 完整周过 narrative 门）
    d = _mk_daily('2025-06-02', [100] * (7 * 8) + [200, 200, 200])
    f = metrics.weekly_trend_facts(metrics.weekly_aggregate(d), daily=d)
    assert f['last_week_state'] == 'in_progress'
    assert f['ip_week'] == '2025-07-28' and f['ip_days'] == 3
    assert f['ip_units'] == 600 and f['ip_daily_avg'] == 200
    assert f['ip_contiguous'] is True
    assert f['ip_prev_same_avg'] == 100 and f['ip_prev_same_ok'] is True  # 上周同位置 3×100

    # ③ 周中缺天 → contiguous=False
    rows = [(pd.Timestamp('2025-06-02') + pd.Timedelta(days=k)).strftime('%Y-%m-%d')
            for k in range(56)]
    rows = [(r, 100) for r in rows]
    rows += [('2025-07-28', 150), ('2025-07-29', 150), ('2025-07-31', 150)]  # 跳周三
    d = _daily_with_holes(rows)
    f = metrics.weekly_trend_facts(metrics.weekly_aggregate(d), daily=d)
    assert f['last_week_state'] == 'in_progress'
    assert f['ip_days'] == 3 and f['ip_contiguous'] is False

    # ④ lagged：前一周缺周三（6 天、非节假日）→ 三态 lagged + 同位置缺天 ok=False
    rows = [(pd.Timestamp('2025-06-02') + pd.Timedelta(days=k)).strftime('%Y-%m-%d')
            for k in range(56)]
    rows = [(r, 100) for r in rows]
    rows += [('2025-07-28', 100), ('2025-07-29', 100), ('2025-07-31', 100),
             ('2025-08-01', 100), ('2025-08-02', 100), ('2025-08-03', 100)]  # 缺周三
    rows += [('2025-08-04', 150), ('2025-08-05', 150), ('2025-08-06', 150)]
    d = _daily_with_holes(rows)
    f = metrics.weekly_trend_facts(metrics.weekly_aggregate(d), daily=d)
    assert f['last_week_state'] == 'lagged'
    assert f['ip_prev_same_ok'] is False and f['ip_prev_same_avg'] is None
    assert f['ip_units'] == 450 and f['ip_days'] == 3      # lagged 仍报 ip_*（S4 滞后变体用）

    # ⑤ 前一周是假期周且缺天 → 不判 lagged（节假日缺天非数据滞后）
    rows = [('2025-09-08', 100), ('2025-09-09', 100), ('2025-09-10', 100),
            ('2025-09-11', 100), ('2025-09-12', 100)]                     # 完整周一 9-08 周
    rows += [('2025-09-29', 60), ('2025-09-30', 60),
             ('2025-10-01', 60), ('2025-10-02', 60)]                       # 9-29 假期周 4 天
    rows += [('2025-10-06', 50), ('2025-10-07', 50), ('2025-10-08', 50)]   # 假期周 3 天
    d = _daily_with_holes(rows)
    f = metrics.weekly_trend_facts(metrics.weekly_aggregate(d), daily=d)
    assert f['last_week_state'] == 'in_progress'

    # ⑥ ip_days<3：字段保留（渲染层跳过 S4）
    d = _mk_daily('2025-06-02', [100] * (7 * 8) + [200, 200])
    f = metrics.weekly_trend_facts(metrics.weekly_aggregate(d), daily=d)
    assert f['ip_days'] == 2 and f['ip_daily_avg'] == 200


def test_weekly_facts_degrade():
    """W1b：降级——7 完整周 narrative=false 且字段最小化；恰好 8 启用；
    12/13 周峰谷边界；26/30 周基准窗边界。"""
    from analysis import metrics

    # ① 7 完整周 → narrative=false，仅 narrative/n_full_weeks/last_week_state 有值
    f = metrics.weekly_trend_facts(_mk_weekly(_mondays('2025-06-02', 7),
                                              [3000, 3100, 3200, 3300, 3400, 3500, 3600]))
    assert f['narrative'] is False and f['n_full_weeks'] == 7
    assert f['direction'] is None and f['avg4'] is None and f['peak13_units'] is None
    assert f['ip_units'] is None

    # ② 恰好 8 完整周 → narrative=true、方向可用、峰谷仍 null（<13）
    f = metrics.weekly_trend_facts(_mk_weekly(_mondays('2025-06-02', 8),
                                              [3000, 3100, 3200, 3300, 3400, 3500, 3600, 3700]))
    assert f['narrative'] is True and f['direction'] == 'up'
    assert f['peak13_units'] is None

    # ③ 恰好 13 完整周 → 峰谷启用
    f = metrics.weekly_trend_facts(_mk_weekly(_mondays('2025-06-02', 13),
                                              [3000 + 10 * k for k in range(13)]))
    assert f['peak13_units'] == 3120 and f['trough13_units'] == 3000


def _facts(**over):
    """W2 渲染测试共用：合成 facts dict（默认=满配上涨场景）。"""
    base = dict(
        narrative=True, n_full_weeks=20, last_week_state='in_progress',
        last_full_week='2026-10-19', last_full_units=3760,
        direction='up', null_reason=None, streak_weeks=2, streak_change_pct=7.4,
        last_pair_reversal=False, streak_contains_est=True, post_holiday=True,
        chain_first_week='2026-10-12', chain_first_units=3502,
        anchor_kind='week', anchor_week='2026-09-07',
        avg4=2972.3, avg26=3423.0, avg26_weeks=26, level='below', level_gap_pct=-13.2,
        peak13_week='2026-09-14', peak13_units=4257, peak13_est=True, peak13_est_days=1,
        peak13_holiday=False,
        trough13_week='2026-10-05', trough13_units=2098, trough13_est=False,
        trough13_est_days=0, trough13_holiday=True,
        ip_week='2026-10-26', ip_units=1879, ip_days=3, ip_contiguous=True,
        ip_daily_avg=626, ip_prev_same_avg=537, ip_prev_same_ok=True)
    base.update(over)
    return base


def test_weekly_narrative_render():
    """W2：渲染纯函数——S1 五变体、S2 时间线（节日/估算/叠加/去重）、S3 日期码+
    锚句、S4 三变体、S5 段末注、水位短语与退化档、预算 ≤6、界定符成对。"""
    import re
    from analysis import weekly_narrative as wn

    # ① 满配上涨：结论前置加粗、节后前缀、节日惯例句、估算括注+段末注、日期码唯一
    out = wn.render(_facts())
    assert out.count(wn.DELIM) == 2
    para = out.split(wn.DELIM)[1].strip()
    assert para.startswith('**节后成交连续 2 周回升，累计 7.4%；近 4 周的周均仍比此前半年低 13.2%。**')
    assert '国庆假期那周只签了 2,098 套，是近三个月的低点——假期签约少是每年惯例，不是市场转冷' in para
    assert '9 月中旬曾冲到近三个月的单周高点 4,257 套（其中 1 天为估算）' in para
    assert '从 3,502 套回升到 3,760 套（10 月 19–25 日当周）' in para
    assert '近 4 周的周均与 9 月上旬的水平相当。' in para   # 锚句独立成句、主语=周均
    assert '本周刚过 3 天，日均 626 套，仍高于上一完整周同期的日均 537 套' in para
    assert '含 1 个估算周，估算日清单见"数据说明"' in para
    assert para.count('当周）') == 1                       # 日期码全段唯一
    nums = re.findall(r'\d{1,3}(?:,\d{3})+|日均 \d+|\d+(?:\.\d+)?%', para)
    assert len(nums) <= 8, nums                            # 数字预算（含量化百分数，2026-10-30 用户裁决）
    for bad in ('预计', '有望', '或将', '拐点', '动能', '窗口均值', '口径', '回暖', '走强', '走弱'):
        assert bad not in para, bad                        # 禁用词（封闭表暂内联，W4 迁 verify）

    # ② est×节假日叠加（谷值含估算）：节日名在前、天数括注随后
    out = wn.render(_facts(trough13_est=True, trough13_est_days=1))
    assert '国庆假期那周只签了 2,098 套（其中 1 天为估算），是近三个月的低点' in out

    # ③ N=1 带值 / 互现：基准显式（比上一完整周）+ 单周波动限定语 + 日期码进 S1
    out = wn.render(_facts(streak_weeks=1, last_pair_reversal=False))
    assert ('**节后最近一周（10 月 19–25 日当周）成交 3,760 套（含估算），'
            '比上一完整周回升 7.4%（单周幅度，属正常波动）') in out
    assert '从 3,502 套' not in out                         # 无 S3
    assert '近 4 周的周均与 9 月上旬的水平相当' in out       # 锚句仍在（独立句）
    out = wn.render(_facts(streak_weeks=1, last_pair_reversal=True))
    assert '**节后近几周有涨有跌，最近一周（10 月 19–25 日当周）成交 3,760 套' in out

    # ④ null 四句式 + 同周谷值简化（holiday_last 场景末完整周=假期周，场景须自洽）
    out = wn.render(_facts(direction=None, null_reason='holiday_last',
                           last_full_week='2026-09-28'))
    assert '**最近一周恰逢国庆假期，签约受假期影响，本周不作涨跌比较' in out
    out = wn.render(_facts(direction=None, null_reason='holiday_last',
                           trough13_week='2026-10-19'))
    assert out.startswith(f'{wn.DELIM}\n**最近一周恰逢假期，涨跌暂不比较。**')
    out = wn.render(_facts(direction=None, null_reason='holiday_prev'))
    assert '**节后首周，涨跌走势还看不出来' in out
    out = wn.render(_facts(direction=None, null_reason='tie'))
    assert '**最近两周基本持平' in out
    out = wn.render(_facts(direction=None, null_reason='gap'))
    assert '**此前一周数据缺失，暂无可比走势' in out

    # ⑤ 水位短语三档（主语=近 4 周的周均） + 退化档；节假日不触发负例
    out = wn.render(_facts(level='above', level_gap_pct=9.9))
    assert '近 4 周的周均比此前半年高 9.9%' in out and '转冷' in out  # 谷值仍是节假日周（场景设定）
    out = wn.render(_facts(level='parity', avg26_weeks=20))
    assert '近 4 周的周均与有数据以来大体相当' in out
    out = wn.render(_facts(trough13_holiday=False))
    assert '假期' not in out.split('。')[1] if '。' in out else True
    assert '见到近三个月低点（2,098 套）' in out

    # ⑥ S4 变体：仅报本周 / lagged / days<3 跳过 / complete 不出现
    out = wn.render(_facts(ip_prev_same_ok=False))
    assert '本周刚过 3 天，已签 1,879 套' in out
    out = wn.render(_facts(last_week_state='lagged'))
    assert '本周只统计到 3 天，共 1,879 套（官方数据更新延迟）' in out
    assert '日均 626' not in out
    out = wn.render(_facts(ip_days=2))
    assert '本周' not in out
    out = wn.render(_facts(last_week_state='complete'))
    assert '本周刚过' not in out

    # ⑦ narrative=false → 空串（无界定符）
    assert wn.render(_facts(narrative=False)) == ''

    # ⑧ 峰谷同周（13 周全持平的极端）
    out = wn.render(_facts(peak13_week='2026-10-19', trough13_week='2026-10-19'))
    assert '大体持平' in out


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
        _write_chart_stubs(tmp)
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
                    '2026-08,1,1,0,1\n2026-09,2,2,12345,2\n')
        build(tmp, 12345)
        ok, issues = verify.check(tmp, data)
        assert ok, issues                       # 全对 → 过（TTM 独立重算=0+12345）
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
        _write_chart_stubs(tmp)
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
             charts.prep_avg_unit_area(aua), charts.prep_weekly_trend(weekly),
             charts.prep_weekly_avg_area(weekly), charts.prep_district_stacked(district),
             charts.prep_district_share(district), charts.prep_district_smallmul(district),
             charts.prep_area_segment_count(area), charts.prep_area_segment_share(area)]
    assert all(s is not None for s in specs)
    for s in specs:
        assert s['name'] and s['kind'] in ('bars_lines', 'lines', 'stacked_area',
                                           'heatmap', 'smallmul')
        assert 'annotations' in s and 'title' in s
        charts.to_echarts(s)
    # T2.6/R5 两张新图
    m_cum = metrics.add_ytd(metrics.add_ttm(monthly))
    for s2 in (charts.prep_cum_view(m_cum),
               charts.prep_structure_share(metrics.nonresidential_share(monthly))):
        assert s2 is not None and s2['kind'] in ('bars_lines', 'lines')
        charts.to_echarts(s2)


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
        _write_chart_stubs(tmp)
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
    # 回归守卫（架构评审 M2 抓过）：段数必须与单一来源一致，防静默丢行
    assert spec['y']['labels'] == charts.AREA_SEGMENTS
    assert len(spec['matrix']) == len(charts.AREA_SEGMENTS)

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
    # T2.4：新鲜度头部（与 quality.json 同源）
    md = open('report/trend_report.md', encoding='utf-8').read()
    assert '数据截止：日度' in md and '生成 20' in md and '进行中' in md


def test_table_sortable():
    """T2.5：可排序表——data-v 键 Python 推断、纯文字表不加 sortable、NO_JS 无脚本。"""
    from analysis import html_render as hr
    md = '| A | B |\n| --- | --- |\n| 15,756 | — |\n| 2,001 | +3.4% |\n| 2026-09 | 朝阳 |\n'
    html = hr.md_to_html(md)
    assert 'table-wrap sortable' in html
    assert 'data-v="15756"' in html and 'data-v="3.4"' in html
    assert 'data-v="20260900"' in html          # 'YYYY-MM' → YYYYMM00（单调即可比）
    assert 'data-dir' in html                      # 排序 JS 已注入
    plain = hr.md_to_html('| A |\n| --- |\n| 文字 |\n')
    assert 'table-wrap sortable' not in plain and 'data-dir' not in plain
    nojs = hr.md_to_html(md, no_js=True)
    assert 'table-wrap sortable' in nojs and 'data-dir' not in nojs
    # QA 评审补充：_cell_key 直接断言（M2 盲区）
    assert hr._cell_key('89.0') == 89.0
    assert hr._cell_key('+0.33pp') == 0.33
    assert hr._cell_key('6 ⚠️') == 6
    assert hr._cell_key('100～120m2') is None
    assert hr._cell_key('朝阳') is None


def test_ttm_ytd_share():
    """T2.6/R5：TTM 整窗规则、YTD 等窗、非住宅占比坏值守卫。"""
    import numpy as np
    import pandas as pd
    from analysis import metrics as M

    def mk(rows):
        return pd.DataFrame({
            'period': pd.PeriodIndex([f'{y}-{m:02d}' for y, m, _ in rows], freq='M'),
            'year': [y for y, _, _ in rows], 'month': [m for _, m, _ in rows],
            '住宅签约套数': [u for *_, u in rows]})

    rows = [(2025, m, 100 + m) for m in range(1, 13)] + [(2026, m, 200 + m) for m in (1, 2)]
    df = M.add_ttm(mk(rows))
    assert df['TTM套数'].isna().sum() == 11
    assert int(df['TTM套数'].iloc[-1]) == sum(u for *_, u in rows[-12:])

    rows2 = [(2025, m, 100) for m in range(1, 13) if m != 6] + [(2026, 1, 100)]
    assert M.add_ttm(mk(rows2))['TTM套数'].isna().all()   # 断档 → 整列 NaN

    rows3 = [(2025, m, 100) for m in (1, 2, 3)] + [(2026, m, 150) for m in (1, 2, 3)]
    df3 = M.add_ytd(mk(rows3))
    v = df3[df3['month'] == 3].iloc[-1]['YTD同比']
    assert abs(v - (450 - 300) / 300 * 100) < 1e-9

    rows4 = [(2025, m, 100) for m in (1, 3)] + [(2026, m, 150) for m in (1, 2)]
    df4 = M.add_ytd(mk(rows4))
    y26 = df4[df4['year'] == 2026].set_index('month')['YTD同比']
    assert not np.isnan(y26.loc[1])     # 1月对1月：等窗可算
    assert np.isnan(y26.loc[2])         # 去年缺2月 → 不等窗 NaN

    from analysis import load
    s = M.nonresidential_share(load.load_monthly())
    assert np.isnan(s[s['period'].astype(str) == '2025-01']['非住宅面积占比'].iloc[0])
    ok = s['非住宅套数占比'].dropna()
    assert ((ok > 0) & (ok < 100)).all()


def test_ui_fixes():
    """用户验收修复（2026-10-27）：热力图默认标签/套均 y_min=80/累计视角裁前导空。"""
    from analysis import load, metrics, charts
    monthly = load.load_monthly()
    # 热力图 label 无 '{c}' formatter（三元组问题，三张热力统一）
    for spec in (charts.prep_seasonality(monthly),
                 charts.prep_area_segment_count(load.load_area())):
        assert 'formatter' not in charts.to_echarts(spec)['series'][0]['label']
    # 套均面积：y_min 80 + 数值 2 位小数
    s = charts.prep_avg_unit_area(metrics.avg_unit_area(monthly))
    assert s['y_min'] == 80 and s['tick_decimals'] == 2
    assert all(round(v, 2) == v for v in s['series'][0]['values'])
    opt = charts.to_echarts(s)
    assert opt['yAxis'][0]['min'] == 80
    # 累计视角：首点即有数据（前导全空已裁；NaN 非 None，须用 _isna 判，
    # `is not None` 对 float64 列恒真——code-review 抓获的空转断言）
    m_cum = metrics.add_ytd(metrics.add_ttm(
        metrics.sanitize_bad_values(metrics.add_yoy_mom(monthly))))
    c = charts.prep_cum_view(m_cum)
    assert not charts._isna(c['series'][0]['values'][0])


def test_new_home_and_district_mom():
    """M3 R1/R2：新房周度聚合与前导裁剪、库存公式、区县环比防护、四 prep 冒烟。"""
    import numpy as np
    from analysis import load, metrics, charts
    with tempfile.TemporaryDirectory() as tmp:
        make_fixtures.write_short_new(tmp)
        nd = load.load_new_daily(data_dir=tmp)
        nw = metrics.new_home_weekly(nd)
        first = next(i for i, v in enumerate(nw['签约住宅'].tolist()) if v > 0)
        assert nw.iloc[first]['week_start'].strftime('%Y-%m-%d') == '2026-08-17'  # 8-23 属该周
        full_days = int((nd['日期'] >= '2026-08-23').sum())
        assert int(nw.iloc[first:]['签约住宅'].sum()) == 220 * full_days
        spec = charts.prep_new_weekly(nw)
        assert spec['series'][0]['values'][0] > 0
        charts.to_echarts(spec)
        inv = metrics.new_inventory(nd)
        assert (inv['库存住宅'] == 39000 + 2 * np.arange(len(inv))).all()

    dm = metrics.district_mom(load.load_district())
    assert dm.isna().iloc[:, 0].all()                       # 首月无环比
    vals = dm.values.flatten()
    # 真实区间约 -58%~+232%（小基数区翻倍属正常），界只防荒谬值
    assert (np.abs(vals[~np.isnan(vals)]) < 500).all()
    s = charts.prep_district_mom(dm)
    assert s['piecewise'] is True
    charts.to_echarts(s)

    nd_real = load.load_new_daily()
    nw_real = metrics.new_home_weekly(nd_real)
    for s2 in (charts.prep_new_weekly(nw_real),
               charts.prep_resale_vs_new(metrics.weekly_aggregate(load.load_daily()), nw_real),
               charts.prep_new_inventory(metrics.new_inventory(nd_real))):
        assert s2 is not None
        charts.to_echarts(s2)


def test_chart_numbers_two_decimals():
    """全局规则：图表 JSON 中所有浮点 ≤2 位小数（MA 尾数/浮点残渣/3 位库存全兜住）。"""
    from analysis import load, metrics, charts

    def floats(o):
        if isinstance(o, float):
            yield o
        elif isinstance(o, dict):
            for v in o.values():
                yield from floats(v)
        elif isinstance(o, list):
            for v in o:
                yield from floats(v)

    weekly = metrics.weekly_aggregate(load.load_daily())
    monthly = metrics.add_yoy_mom(load.load_monthly())
    m_cum = metrics.add_ytd(metrics.add_ttm(
        metrics.sanitize_bad_values(monthly)))
    specs = [
        charts.prep_weekly_trend(weekly),                       # MA4 浮点残渣
        charts.prep_weekly_avg_area(weekly),                    # MA8 除法尾数
        charts.prep_city_trend(metrics.add_moving_avg(monthly, windows=(12,))),
        charts.prep_cum_view(m_cum),                            # YTD 百分比
        charts.prep_new_inventory(metrics.new_inventory(load.load_new_daily())),  # 曾 3 位
    ]
    for spec in specs:
        vals = list(floats(charts.chart_payload(spec)))
        assert vals, spec['name']
        assert all(round(f, 2) == f for f in vals), spec['name']


def test_m3_agency_and_annual():
    """M3 R3/R4：退房率断档整窗空、Σ/Σ 合并比、Top10 份额、年度锚点拼接。"""
    import numpy as np
    from analysis import load, metrics, charts
    ref = metrics.agency_refund(load.load_agency())
    r = ref.set_index(ref['period'].astype(str))
    assert np.isnan(r.loc['2026-05', '退房率']) and np.isnan(r.loc['2026-06', '退房率'])
    # 跨断档滚动窗口（05/06 前后各 3 月窗）整窗 NaN
    assert np.isnan(r.loc['2026-06', '滚动3月'])
    # 合并比抽查：最新月当月值 = Σ退房/Σ签约
    g = load.load_agency()
    latest = g['period'].max()
    m = g[g['period'] == latest]
    expect = m['退房套数'].sum() / m['签约套数'].sum() * 100
    assert abs(r.loc[str(latest), '退房率'] - expect) < 1e-9

    top = metrics.agency_top(g)
    assert len(top) == 10
    assert (top['上榜份额'] > 0).all() and top['上榜份额'].sum() <= 100 + 1e-9
    assert top['上榜份额'].iloc[0] >= top['上榜份额'].iloc[-1]   # nlargest 降序

    anchor = metrics.annual_anchor(load.load_monthly(), load.load_annual())
    by_year = {a['年份']: a for a in anchor}
    assert 2025 in by_year and by_year[2025]['口径'] == '月度求和'
    monthly25 = load.load_monthly()
    monthly25 = monthly25[monthly25['period'].dt.year == 2025]
    assert abs(by_year[2025]['万套'] - monthly25['住宅签约套数'].sum() / 1e4) < 1e-9

    for s in (charts.prep_annual_anchor(anchor), charts.prep_agency_top(top),
              charts.prep_refund_rate(ref)):
        assert s is not None
        charts.to_echarts(s)


def test_ui_round2():
    """用户验收第二轮：纯线轴自适应下限 / 环比热力七档分色 / 红框说明自解释。"""
    import os
    from analysis import charts, load, metrics
    # 1) _axis_min：显式 y_min 优先；柱轴保持 0 基线；纯线下限低于数据最小值
    assert charts._axis_min({'series': [{'type': 'line', 'values': [100, 110]}]}) == 98.0
    assert charts._axis_min({'y_min': 80, 'series': [{'type': 'line', 'values': [100, 110]}]}) == 80
    assert charts._axis_min({'series': [{'type': 'bar', 'values': [1, 2]},
                                        {'type': 'line', 'values': [100, 110]}]}) is None
    assert charts._axis_min({'series': [{'type': 'line', 'values': [-5, 10]}]}) is None
    # 2) 环比热力 → piecewise 七档（ECharts + mpl 双 sink 不抛错）
    spec = charts.prep_district_mom(metrics.district_mom(load.load_district()))
    vm = charts.to_echarts(spec)['visualMap']
    assert vm['type'] == 'piecewise' and len(vm['splitList']) == 7
    with tempfile.TemporaryDirectory() as tmp:
        charts.write_chart(spec, out_dir=tmp)
        assert os.path.getsize(os.path.join(tmp, 'district_mom.png')) > 0
    # 3) 红框说明：md 图注置首自解释，近10周表含'估N天'标记
    md = open('report/trend_report.md', encoding='utf-8').read()
    assert '红框柱=该周含回推估算日' in md
    assert '估1天' in md or '估2天' in md or '含回推估算日' in md
    # 4) 纯线图 option 落了自适应 min（如年度锚点 < 数据最小值）
    anchor = charts.prep_annual_anchor(metrics.annual_anchor(load.load_monthly(), load.load_annual()))
    lo = min(v for v in anchor['series'][0]['values'] if v is not None)
    assert charts.to_echarts(anchor)['yAxis'][0]['min'] < lo


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
