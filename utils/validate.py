"""数据完整性校验

逐月校验分类汇总数据（面积段 / 价格段）的成交套数加总是否与
区县数据"全市"行一致。阈值 5%。

既被 main.py 在抓取后调用（不一致则非零退出，让 cron 可见），
也可被 script/validate.py 单独调用，还被 analysis/load.py 在合并
外部历史数据时复用做交叉验证。
"""
import os
import pandas as pd
import config

# 校验阈值
THRESHOLD = 0.05  # 5%


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


def validate_integrity(data_dir=None, logger=None):
    """校验 data_dir 下各 CSV 的内部一致性。

    返回 (ok: bool, issues: list[dict])。
    ok 为 True 表示无超阈值问题；issues 为问题明细（每项含 月份/类型/分段加总/全市/偏差）。
    logger 可选：若提供，会把每个问题 error 出来。
    """
    data_dir = data_dir or config.DATA_DIR
    issues = []

    district_path = os.path.join(data_dir, 'district_monthly.csv')
    area_path = os.path.join(data_dir, 'area_monthly.csv')
    price_path = os.path.join(data_dir, 'price_monthly.csv')

    if not os.path.exists(district_path):
        # 没有全市基准，无法校验
        return True, issues

    district = pd.read_csv(district_path, encoding=config.CSV_ENCODING)
    city_totals = _city_totals(district)

    if os.path.exists(area_path):
        area = pd.read_csv(area_path, encoding=config.CSV_ENCODING)
        issues += _check_segment(area, '成交套数', city_totals, '面积段')

    if os.path.exists(price_path):
        price = pd.read_csv(price_path, encoding=config.CSV_ENCODING)
        issues += _check_segment(price, '成交套数', city_totals, '价格段')

    if logger is not None:
        for it in issues:
            logger.error(
                f"数据不一致 [{it['类型']}] {it['月份']}: "
                f"分段加总 {it['分段加总']} vs 全市 {it['全市']}，偏差 {it['偏差']}"
            )

    return len(issues) == 0, issues
