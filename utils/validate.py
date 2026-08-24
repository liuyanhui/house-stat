"""数据完整性校验

三道防线：
1. 分段加总校验：面积段 / 价格段各月成交套数加总 ≈ 区县"全市"（阈值 5%）。
2. 月度一致性：resale_monthly 住宅 ≤ 总计；日度加总 vs 月度对账（仅日度完整覆盖的月份）。
3. 断流门 / 滞后告警：月度表解析为空且页面月份更新 → 失败；日度数据落后 → 告警。

既被 main.py 在抓取后调用（不一致则非零退出，让 cron 可见），
也可被 script/validate.py 单独调用，还被 analysis/load.py 在合并
外部历史数据时复用做交叉验证。
"""
import calendar
import os
import pandas as pd
import config

# 校验阈值
THRESHOLD = 0.05  # 5%（分段加总 vs 全市）

# 日月对账阈值：实测完整覆盖月份偏差 ≤0.02%（官方微调），0.5% 留余量
DAILY_MONTH_TOLERANCE = 0.005

# 已知历史数据异常（首爬即如此、无原始页面可考，白名单放行不算失败）。
# 数据修复前保持登记，避免存量异常卡死校验门。
KNOWN_ISSUES = {
    ('2025-01', '住宅签约面积>网上签约面积'):
        '网上签约面积 125475.17 < 住宅签约面积 1169916.55，疑为官方发布或首爬解析异常',
}


def _clean_district_name(s):
    """统一区县名：去全角/半角空格。"""
    return str(s).strip().replace('　', '').replace(' ', '')


def _city_totals(district_df):
    """从 district 数据取每月"全市"签约套数，返回 {年月: 套数}。"""
    if district_df.empty:
        return {}
    d = district_df.copy()
    d['区县_clean'] = d['区县'].apply(_clean_district_name)
    city = d[d['区县_clean'] == '全市']
    return dict(zip(city['年月'].astype(str), city['签约套数']))


def _check_segment(seg_df, value_col, city_totals, label):
    """校验某个分段表（面积/价格）每月加总 vs 全市。返回 issues 列表。"""
    issues = []
    if seg_df.empty:
        return issues
    # 只校验值为正的行（-1 是抓取失败占位，不能计入加总）
    valid = seg_df[seg_df[value_col] > 0]
    for ym, grp in valid.groupby(seg_df['年月'].astype(str)):
        seg_sum = grp[value_col].sum()
        city = city_totals.get(ym)
        if city is None or city <= 0:
            continue
        diff = abs(seg_sum - city) / city
        if diff > THRESHOLD:
            issues.append({
                '月份': ym,
                '类型': label,
                '分段加总': int(seg_sum),
                '全市': int(city),
                '偏差': f'{diff * 100:.1f}%',
            })
    return issues


def _norm_month(ym):
    """'2025-1' / '2025-01' -> '2025-01'（resale_monthly 月份未补零）。"""
    y, m = str(ym).split('-')
    return f'{y}-{m.zfill(2)}'


def _check_monthly_consistency(data_dir, logger=None):
    """resale_monthly 一致性：
    a) 住宅签约套数/面积 ≤ 对应总计（能抓住总/住错位或丢位数字）；
    b) 日度加总 vs 月度对账——仅当该月日度覆盖完整（行数=自然天数）时校验，
       覆盖不完整的月份跳过（历史空洞不是数据错误）。
    """
    issues = []
    monthly_path = os.path.join(data_dir, 'resale_monthly.csv')
    if not os.path.exists(monthly_path):
        return issues

    m = pd.read_csv(monthly_path, encoding=config.CSV_ENCODING)

    daily_sum = {}
    daily_path = os.path.join(data_dir, 'resale_daily.csv')
    if os.path.exists(daily_path):
        d = pd.read_csv(daily_path, encoding=config.CSV_ENCODING)
        d = d.assign(ym=pd.to_datetime(d['日期']).dt.strftime('%Y-%m'))
        daily_sum = d.groupby('ym')['签约套数'].sum().to_dict()
        daily_rows = d.groupby('ym').size().to_dict()
    else:
        daily_rows = {}

    for _, r in m.iterrows():
        ym = _norm_month(r['月份'])

        # a) 住宅 ≤ 总计
        checks = [
            ('住宅签约套数>网上签约套数', r['住宅签约套数'], r['网上签约套数']),
            ('住宅签约面积>网上签约面积', r['住宅签约面积(m2)'], r['网上签约面积(m2)']),
        ]
        for label, sub, total in checks:
            if total > 0 and sub > total:
                if (ym, label) in KNOWN_ISSUES:
                    if logger:
                        logger.warning(
                            f"已知数据异常（白名单放行）[{ym}] {label}：{sub} vs {total}，"
                            f"{KNOWN_ISSUES[(ym, label)]}"
                        )
                    continue
                issues.append({
                    '月份': ym,
                    '类型': label,
                    '分段加总': int(sub),
                    '全市': int(total),
                    '偏差': '-',
                })

        # b) 日月对账（仅完整覆盖月份）
        y, mo = int(ym[:4]), int(ym[5:])
        if daily_rows.get(ym) == calendar.monthrange(y, mo)[1]:
            total = r['网上签约套数']
            dsum = daily_sum.get(ym, 0)
            if total > 0 and abs(dsum - total) / total > DAILY_MONTH_TOLERANCE:
                issues.append({
                    '月份': ym,
                    '类型': '日月对账',
                    '分段加总': int(dsum),
                    '全市': int(total),
                    '偏差': f'{(dsum - total) / total * 100:+.1f}%',
                })

    return issues


def check_known_issues(data_dir=None):
    """返回当前数据中仍然存在的已知历史异常（白名单项），供校验脚本提示。"""
    data_dir = data_dir or config.DATA_DIR
    present = []
    monthly_path = os.path.join(data_dir, 'resale_monthly.csv')
    if not os.path.exists(monthly_path):
        return present
    try:
        m = pd.read_csv(monthly_path, encoding=config.CSV_ENCODING)
    except Exception:
        return present
    for _, r in m.iterrows():
        ym = _norm_month(r['月份'])
        if (ym, '住宅签约面积>网上签约面积') in KNOWN_ISSUES:
            if r['网上签约面积(m2)'] > 0 and r['住宅签约面积(m2)'] > r['网上签约面积(m2)']:
                present.append((ym, '住宅签约面积>网上签约面积', KNOWN_ISSUES[(ym, '住宅签约面积>网上签约面积')]))
    return present


def validate_integrity(data_dir=None, logger=None):
    """校验 data_dir 下各 CSV 的内部一致性。

    返回 (ok: bool, issues: list[dict])。
    ok 为 True 表示无超阈值问题；issues 为问题明细（每项含 月份/类型/分段加总/全市/偏差）。
    KNOWN_ISSUES 白名单内的历史异常不算失败（由 check_known_issues 单独提示）。
    logger 可选：若提供，会把每个问题 error 出来。
    """
    data_dir = data_dir or config.DATA_DIR
    issues = []

    district_path = os.path.join(data_dir, 'district_monthly.csv')
    area_path = os.path.join(data_dir, 'area_monthly.csv')
    price_path = os.path.join(data_dir, 'price_monthly.csv')

    if os.path.exists(district_path):
        district = pd.read_csv(district_path, encoding=config.CSV_ENCODING)
        city_totals = _city_totals(district)

        if os.path.exists(area_path):
            area = pd.read_csv(area_path, encoding=config.CSV_ENCODING)
            issues += _check_segment(area, '成交套数', city_totals, '面积段')

        if os.path.exists(price_path):
            price = pd.read_csv(price_path, encoding=config.CSV_ENCODING)
            issues += _check_segment(price, '成交套数', city_totals, '价格段')

    issues += _check_monthly_consistency(data_dir, logger=logger)

    if logger is not None:
        for it in issues:
            logger.error(
                f"数据不一致 [{it['类型']}] {it['月份']}: "
                f"分段加总 {it['分段加总']} vs 全市 {it['全市']}，偏差 {it['偏差']}"
            )

    return len(issues) == 0, issues


def check_monthly_feeds(year_month, feeds, logger=None):
    """断流门：月度表本次解析为空，且页面年月比库内最新月份新。

    说明本月数据正在错过（页面改版或解析器故障，如经纪机构表
    2026-05 起改版导致断流 3 个月未被发现），升级为失败。

    参数:
        year_month: 页面数据年月（如 '2026-07'）
        feeds: {数据名: (本次解析的 DataFrame, 对应 CSV 路径)}

    返回 (ok, issues)，issues 为字符串列表。
    """
    issues = []
    ym = str(year_month)
    for name, (df, csv_path) in feeds.items():
        if df is not None and not df.empty:
            continue
        if not os.path.exists(csv_path):
            continue  # 无历史基线（首次运行），无法判定断流
        try:
            existing = pd.read_csv(csv_path, encoding=config.CSV_ENCODING)
        except Exception:
            continue
        if existing.empty or '年月' not in existing.columns:
            continue
        latest = existing['年月'].astype(str).max()
        if ym > latest:
            issues.append(
                f"{name}：页面月份 {ym} 新于库内最新 {latest}，但本次解析为空"
                f"（疑似页面改版或解析器故障）"
            )
    if logger is not None:
        for it in issues:
            logger.error(f"断流门 {it}")
    return len(issues) == 0, issues


def check_daily_freshness(data_dir=None, logger=None, today=None):
    """滞后告警：resale_daily / new_daily 最新日期早于昨天。

    页面约每日上午更新 T-1 数据且只显示一天；爬完仍落后说明当日漏爬
    或页面未更新。仅告警，不作硬门（today 可注入便于测试）。
    """
    data_dir = data_dir or config.DATA_DIR
    today = pd.Timestamp(today) if today is not None else pd.Timestamp.now().normalize()
    yesterday = today - pd.Timedelta(days=1)

    warnings = []
    for fname, label in [('resale_daily.csv', '存量房日度'), ('new_daily.csv', '商品房日度')]:
        path = os.path.join(data_dir, fname)
        if not os.path.exists(path):
            continue
        try:
            df = pd.read_csv(path, encoding=config.CSV_ENCODING)
            if df.empty or '日期' not in df.columns:
                continue
            latest = pd.to_datetime(df['日期']).max().normalize()
            if latest < yesterday:
                warnings.append(
                    f"{label}数据最新日期 {latest:%Y-%m-%d} 早于昨天 {yesterday:%Y-%m-%d}"
                    f"（页面未更新当日数据或本次未抓到）"
                )
        except Exception:
            continue
    if logger is not None:
        for w in warnings:
            logger.warning(f"滞后告警 {w}")
    return warnings
