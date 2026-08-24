"""每日数据解析器"""
import re
import pandas as pd
from .base_parser import safe_int, safe_float


def parse_daily_data(soup, logger):
    """
    解析每日存量房网上签约数据（日期+签约 4 指标，共 5 列）。

    历史上还解析"可售房源统计""新发布房源"两表（8 列），页面已删除
    该栏目，相关列恒为 -1 占位，已一并清理（2026-08）。
    日期从"YYYY/M/D存量房网上签约"表格标题中提取。
    """
    logger.info("开始解析每日数据...")

    try:
        # 从"存量房网上签约"表标题中提取日期
        date_str = None
        date_pattern = re.compile(r'(\d{4})/(\d{1,2})/(\d{1,2})存量房网上签约')
        for element in soup.find_all(text=True):
            match = date_pattern.search(str(element))
            if match:
                year = match.group(1)
                month = match.group(2).zfill(2)
                day = match.group(3).zfill(2)
                date_str = f"{year}-{month}-{day}"
                logger.info(f"从存量房网上签约标题提取日期：{date_str}")
                break

        if not date_str:
            logger.error("无法提取日期，跳过每日数据解析")
            return pd.DataFrame()

        # 初始化数据字典
        data = {
            '日期': date_str,
            '签约套数': -1,
            '签约面积': -1,
            '住宅签约套数': -1,
            '住宅签约面积': -1
        }

        # 解析存量房网上签约数据（逐 td 对扫描 label/value）
        try:
            for table in soup.find_all('table'):
                table_text = table.get_text()
                if date_pattern.search(table_text):
                    rows = table.find_all('tr')
                    for row in rows:
                        cols = row.find_all('td')
                        for i in range(len(cols) - 1):
                            label = cols[i].get_text(strip=True)
                            value = cols[i + 1].get_text(strip=True)

                            if '网上签约套数' in label and '住宅' not in label:
                                data['签约套数'] = safe_int(value)
                            elif '网上签约面积' in label and '住宅' not in label:
                                area_value = value.replace('m²', '').replace('M2', '').replace(' ', '').strip()
                                data['签约面积'] = safe_float(area_value)
                            elif '住宅签约套数' in label:
                                data['住宅签约套数'] = safe_int(value)
                            elif '住宅签约面积' in label:
                                area_value = value.replace('m²', '').replace('M2', '').replace(' ', '').strip()
                                data['住宅签约面积'] = safe_float(area_value)
                    break
        except Exception as e:
            logger.warning(f"解析存量房网上签约数据失败：{e}")

        df = pd.DataFrame([data])
        df = df[['日期', '签约套数', '签约面积', '住宅签约套数', '住宅签约面积']]

        logger.info(f"成功解析每日数据，日期：{date_str}")
        return df

    except Exception as e:
        logger.error(f"解析每日数据失败：{e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()


def _clean_label(label):
    """清洗单元格标签：去冒号/空格/全角空格/其中/(M2) 单位，便于按文本精确匹配。"""
    s = label.replace(' ', '').replace('　', '').replace(' ', '')
    s = re.sub(r'[（(]\s*[MmＭ]2\s*[)）]', '', s)
    s = s.replace('其中', '')
    return s.rstrip('：:').strip()


# 商品房 8 张表：标题关键字、是否带日期标题、{清洗后label: 数据列}、{计数列: 紧随其后的面积列}
# 页面改版后标题独占一行（如"2026/8/23期房网上认购"），数据行内不含"期房/现房"等字样，
# 故按表格首行标题定位表、行内纯按 label 匹配；旧的 '期房' in str(row) 类条件已失效。
_COMMERCIAL_TABLES = [
    ('可售期房统计', False,
     {'可售房屋套数': '可售期房套数', '可售房屋面积': '可售期房面积',
      '住宅套数': '可售期房住宅套数', '商业单元': '可售期房商业单元',
      '办公单元': '可售期房办公单元', '车位个数': '可售期房车位个数'},
     {'可售期房住宅套数': '可售期房住宅面积', '可售期房商业单元': '可售期房商业面积',
      '可售期房办公单元': '可售期房办公面积', '可售期房车位个数': '可售期房车位面积'}),
    ('未签约现房统计', False,
     {'未签约套数': '未签约现房套数', '未签约面积': '未签约现房面积',
      '住宅套数': '未签约现房住宅套数', '商业单元': '未签约现房商业单元'},
     {'未签约现房住宅套数': '未签约现房住宅面积', '未签约现房商业单元': '未签约现房商业面积'}),
    ('现房项目情况', False,
     {'现房项目个数': '现房项目个数', '初始登记面积': '现房初始登记面积',
      '住宅套数': '现房住宅套数', '商业单元': '现房商业单元'},
     {'现房住宅套数': '现房住宅面积', '现房商业单元': '现房商业面积'}),
    ('预售许可', False,
     {'批准预售许可证': '预售许可证', '批准预售面积': '预售许可面积',
      '住宅套数': '预售住宅套数'},
     {'预售住宅套数': '预售住宅面积'}),
    ('期房网上认购', True,
     {'网上认购套数': '期房认购套数', '网上认购面积': '期房认购面积',
      '住宅套数': '期房认购住宅套数'},
     {'期房认购住宅套数': '期房认购住宅面积'}),
    ('期房网上签约', True,
     {'网上签约套数': '期房签约套数', '网上签约面积': '期房签约面积',
      '住宅套数': '期房签约住宅套数'},
     {'期房签约住宅套数': '期房签约住宅面积'}),
    ('现房网上认购', True,
     {'网上认购套数': '现房认购套数', '网上认购面积': '现房认购面积',
      '住宅套数': '现房认购住宅套数'},
     {'现房认购住宅套数': '现房认购住宅面积'}),
    ('现房网上签约', True,
     {'网上签约套数': '现房签约套数', '网上签约面积': '现房签约面积',
      '住宅套数': '现房签约住宅套数'},
     {'现房签约住宅套数': '现房签约住宅面积'}),
]

_DATE_IN_TITLE = re.compile(r'(\d{4})/(\d{1,2})/(\d{1,2})')

# 输出列顺序（与既有 CSV 表头一致）：日期 → 各表按"计数列、紧跟其面积列"展开
_COMMERCIAL_COL_ORDER = ['日期']
for _kw, _dated, _lm, _am in _COMMERCIAL_TABLES:
    for _c in _lm.values():
        _COMMERCIAL_COL_ORDER.append(_c)
        if _c in _am:
            _COMMERCIAL_COL_ORDER.append(_am[_c])


def parse_commercial_data(soup, logger):
    """
    解析商品房数据统计
    包括8个部分：可售期房统计、未签约现房统计、现房项目情况、预售许可、
             期房网上认购、期房网上签约、现房网上认购、现房网上签约

    按表格首行标题（含日期关键字，如"2026/8/23期房网上认购"）定位表，
    避免命中 <script> 内同名文本导致定位到外层容器表；
    行内按清洗后的 label 精确匹配，"面积"行归属上一个计数行。
    """
    logger.info("开始解析商品房数据...")

    try:
        data = None
        date_str = None

        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            if not rows:
                continue
            title = rows[0].get_text(strip=True)

            for keyword, dated, label_map, area_map in _COMMERCIAL_TABLES:
                if keyword not in title:
                    continue

                # 带日期的表须在标题中匹配到日期；容器表标题无日期，自然排除
                if dated:
                    m = _DATE_IN_TITLE.search(title)
                    if not m:
                        continue
                    if date_str is None:
                        date_str = f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
                        logger.info(f"从{keyword}标题提取日期：{date_str}")

                if data is None:
                    data = {'日期': date_str}

                # 逐行按 label 解析；独立"面积"行跟随上一个计数列
                last_count_col = None
                for row in rows[1:]:
                    cols = row.find_all('td')
                    if len(cols) < 2:
                        continue
                    label = _clean_label(cols[0].get_text(strip=True))
                    value = cols[1].get_text(strip=True)

                    if label in label_map:
                        col = label_map[label]
                        data[col] = safe_int(value) if '面积' not in col else safe_float(value)
                        last_count_col = col if col in area_map else None
                    elif label == '面积' and last_count_col:
                        data[area_map[last_count_col]] = safe_float(value)
                        last_count_col = None
                break

        if not date_str:
            logger.error("无法从商品房表标题提取日期，跳过商品房数据解析")
            return pd.DataFrame()

        # 按既有 CSV 列顺序输出，缺失字段以 -1 占位，保证列结构稳定
        row = {'日期': date_str, **{c: data.get(c, -1) for c in _COMMERCIAL_COL_ORDER[1:]}}
        df = pd.DataFrame([row])[_COMMERCIAL_COL_ORDER]
        logger.info(f"成功解析商品房数据，日期：{date_str}")
        return df

    except Exception as e:
        logger.error(f"解析商品房数据失败：{e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()
