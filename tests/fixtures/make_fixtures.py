# -*- coding: utf-8 -*-
"""合成 CSV fixture：覆盖真实数据打不到的四个分支（开发计划 T1.a）。

  gap_monthly   断档环比——resale_monthly 缺一个月，跨档环比必须 NaN
  est_week      估算周聚合——一周内含登记估算日（调用方按参数传入）
  agency_gap    退房率跨断档——agency_monthly 缺 05/06
  short_new     去化分母不足——new_daily 签约字段全量仅约 2 个月

生成器只写 CSV；new_daily 表头取自真实 data/ 文件（防表头漂移）。
断言用例见 tests/run_tests.py（随 T1.2/T1.3 补齐）。
"""
import csv
import datetime as dt
import os

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))

MONTHLY_HEADER = ['月份', '网上签约套数', '网上签约面积(m2)', '住宅签约套数', '住宅签约面积(m2)']
DAILY_HEADER = ['日期', '签约套数', '签约面积', '住宅签约套数', '住宅签约面积']
AGENCY_HEADER = ['年月', '序号', '经纪机构', '发布套数', '签约套数', '退房套数']


def write_gap_monthly(out_dir):
    """2025-01~06、08~10（缺 07）：2025-08 的环比跨档，必须 NaN。"""
    rows = []
    for m in range(1, 11):
        if m == 7:
            continue
        units = 1000 + m * 10
        rows.append([f'2025-{m}', int(units * 1.1), units, int(units * 0.95), units * 90])
    path = os.path.join(out_dir, 'resale_monthly.csv')
    _write(path, MONTHLY_HEADER, rows)
    return path


def write_est_week_daily(out_dir):
    """2025-05-05（周一）~ 05-11 一周；05-06/07 由调用方登记为估算日。"""
    rows = []
    for i, d in enumerate(range(5, 12)):
        n = 700 - i * 30
        rows.append([f'2025-05-{d:02d}', n, round(n * 95.0, 2), n - 40, round((n - 40) * 93.0, 2)])
    path = os.path.join(out_dir, 'resale_daily.csv')
    _write(path, DAILY_HEADER, rows)
    return path


def write_agency_gap(out_dir):
    """2026-04 与 07、08（缺 05/06）：跨档的 3 月滚动必须 NaN/—。"""
    rows = []
    for ym in ['2026-04', '2026-07', '2026-08']:
        for i, name in enumerate(['机构A', '机构B', '机构C'], 1):
            rows.append([ym, i, name, -1, 500 - i * 80, 10 + i])
    path = os.path.join(out_dir, 'agency_monthly.csv')
    _write(path, AGENCY_HEADER, rows)
    return path


def write_short_new(out_dir, repo_data_dir=None):
    """2026-08-01~09-30：库存列全程有效，认购/签约列仅 8-23 起有值。"""
    data_dir = repo_data_dir or os.path.join(ROOT, 'data')
    header = _new_daily_header(data_dir)
    full_from = dt.date(2026, 8, 23)
    rows = []
    d0 = dt.date(2026, 8, 1)
    for k in range(61):
        d = d0 + dt.timedelta(days=k)
        row = [str(d)] + ['-1'] * (len(header) - 1)

        def set_(col, v, _row=row, _h=header):
            _row[_h.index(col)] = v

        set_('可售期房住宅套数', 30000 + k)
        set_('可售期房住宅面积', 3000000.0 + k * 100)
        set_('未签约现房住宅套数', 9000 + k)
        set_('未签约现房住宅面积', 900000.0 + k * 10)
        if d >= full_from:
            set_('期房认购住宅套数', 150)
            set_('期房签约住宅套数', 200)
            set_('期房签约住宅面积', 20000.0)
            set_('现房签约住宅套数', 20)
            set_('现房签约住宅面积', 2200.0)
        rows.append(row)
    path = os.path.join(out_dir, 'new_daily.csv')
    _write(path, header, rows)
    return path


def _new_daily_header(data_dir):
    src = os.path.join(data_dir, 'new_daily.csv')
    with open(src, encoding='utf-8-sig') as f:
        return next(csv.reader(f))


def _write(path, header, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8-sig', newline='') as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def write_all(out_dir):
    """生成全部四分支 fixture 到 out_dir，返回 {分支名: 文件路径}。"""
    return {
        'gap_monthly': write_gap_monthly(out_dir),
        'est_week': write_est_week_daily(out_dir),
        'agency_gap': write_agency_gap(out_dir),
        'short_new': write_short_new(out_dir),
    }
