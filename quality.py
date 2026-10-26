# -*- coding: utf-8 -*-
"""质量元数据单一来源（M1 基建，设计 v1.1 §2.2）。

零项目内依赖：纯常量 + 纯函数。load / report / ai_digest / utils.validate
都从这里引用，新增缺陷只改本文件，渲染端零改动。
放在根目录（而非 analysis/ 内）是刻意的：analysis/__init__ 会连带导入
plots（matplotlib），utils 侧引用会拖入重依赖并造成潜在环。

DATASET_META 六字段：coverage / estimated_days / missing_months /
known_issues / notes / generated_at；coverage 由 span_text 从数据派生
（勿手维护），generated_at 由生成侧注入。
"""

# 日度估算日登记（根 CLAUDE.md 约定 8/11）：回推补全的非真实观测日期 → 成因与回推值。
# report 数据说明、周度"含估算 n 天"标注、ai_digest 局限均从此渲染，
# 新增估算日只需在这里登记一行。
ESTIMATED_DAYS = {
    '2026-07-25/26': '官方故障缺失，按 7 月月度回推（签约 154/154、住宅 147/146 套，签约面积 13,715.98、住宅面积 13,429.49/日）',
    '2026-09-16': '抓取缺失，按 9 月月度减其余 29 日之和回推（签约 779 套/68,450.30 m²、住宅 704 套/64,185.35 m²）',
}

# 已知历史数据异常（首爬即如此、无原始页面可考，白名单放行不算失败）。
# 2026-10-26 自 utils/validate.py 迁入（单一来源）。
KNOWN_ISSUES = {
    ('2025-01', '住宅签约面积>网上签约面积'):
        '网上签约面积 125475.17 < 住宅签约面积 1169916.55，疑为官方发布或首爬解析异常',
}

# 各已知异常归属的数据集（dataset_meta 按此挂载）。
_ISSUE_DATASET = {
    ('2025-01', '住宅签约面积>网上签约面积'): 'resale_monthly',
}

# 估算日所属数据集。
_EST_DAYS_DATASETS = ('resale_daily',)

# 各数据集永久缺失（官方无回溯，无法回补）。
MISSING_MONTHS = {
    'area_monthly': ['2026-04'],               # 解析器时间窗口 bug（约定 7）
    'agency_monthly': ['2026-05', '2026-06'],  # 页面删列致解析断流（约定 9）
    'new_daily': ['2026-09-16'],               # 无月度表可对账（约定 11）
}

# 数据集口径/边界说明（渲染数据说明页用，按需增补）。
NOTES = {
    'resale_monthly': '全市月度主序列，均为官方观测。',
    'resale_daily': '日度含估算日（见 estimated_days）；周/月聚合时标记随聚合传播。',
    'district_monthly': '区县月度，含"全市"汇总行（加载时过滤）。',
    'area_monthly': '面积段月度，缺 2026-04（永久）。',
    'price_monthly': '官方发布数据全占位，不用于分析。',
    'agency_monthly': '经纪机构月度排行（上榜口径）；发布套数列全 -1 不可用。',
    'new_daily': '商品房日度宽表；认购/签约字段 2026-08-23 起全量。',
    'annual': '官方年度表 2020–2024（万套口径）。',
}


def span_text(values):
    """把一组年月/日期（可比较字符串）汇总为 'min ~ max'；空返回 '—'。"""
    vals = sorted({str(v) for v in values
                   if str(v) not in ('', 'NaT', 'nan', 'None')})
    return f'{vals[0]} ~ {vals[-1]}' if vals else '—'


def dataset_meta(periods_by_name, generated_at):
    """汇总各数据集 meta（六字段）。periods_by_name: {数据集名: 覆盖期字符串列表}。"""
    meta = {}
    for name, periods in periods_by_name.items():
        issues = [f'{ym} {label}：{desc}'
                  for (ym, label), desc in KNOWN_ISSUES.items()
                  if _ISSUE_DATASET.get((ym, label)) == name]
        meta[name] = {
            'coverage': span_text(periods),
            'estimated_days': dict(ESTIMATED_DAYS) if name in _EST_DAYS_DATASETS else {},
            'missing_months': list(MISSING_MONTHS.get(name, [])),
            'known_issues': issues,
            'notes': NOTES.get(name, ''),
            'generated_at': generated_at,
        }
    return meta
