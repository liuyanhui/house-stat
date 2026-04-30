"""每日数据解析器"""
import re
import pandas as pd
from .base_parser import safe_int, safe_float


def parse_daily_data(soup, logger):
    """
    解析每日数据，包括：
    1. 存量房网上签约数据
    2. 可售房源统计数据
    3. 新发布房源数据
    日期从"新发布房源"标题中提取
    """
    logger.info("开始解析每日数据...")

    try:
        # 从"新发布房源"标题中提取日期
        date_from_new_listing = None
        new_listing_pattern = re.compile(r'(\d{4})/(\d{1,2})/(\d{1,2}).*新发布房源')
        for element in soup.find_all(text=True):
            match = new_listing_pattern.search(str(element))
            if match:
                year = match.group(1)
                month = match.group(2).zfill(2)
                day = match.group(3).zfill(2)
                date_from_new_listing = f"{year}-{month}-{day}"
                logger.info(f"从新发布房源标题提取日期：{date_from_new_listing}")
                break

        if not date_from_new_listing:
            logger.warning("未找到新发布房源日期，尝试使用存量房网上签约日期")
            date_pattern = re.compile(r'(\d{4})/(\d{1,2})/(\d{1,2})存量房网上签约')
            for element in soup.find_all(text=True):
                match = date_pattern.search(str(element))
                if match:
                    year = match.group(1)
                    month = match.group(2).zfill(2)
                    day = match.group(3).zfill(2)
                    date_from_new_listing = f"{year}-{month}-{day}"
                    logger.info(f"从存量房网上签约标题提取日期：{date_from_new_listing}")
                    break

        if not date_from_new_listing:
            logger.error("无法提取日期，跳过每日数据解析")
            return pd.DataFrame()

        # 初始化数据字典
        data = {
            '日期': date_from_new_listing,
            '签约套数': -1,
            '签约面积': -1,
            '住宅签约套数': -1,
            '住宅签约面积': -1,
            '可售房源套数': -1,
            '可售房源面积': -1,
            '可售住宅套数': -1,
            '可售住宅面积': -1,
            '新发布房源套数': -1,
            '新发布房源面积': -1,
            '新发布住宅套数': -1,
            '新发布住宅面积': -1
        }

        # 解析存量房网上签约数据
        try:
            date_pattern = re.compile(r'(\d{4})/(\d{1,2})/(\d{1,2})存量房网上签约')
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

        # 解析可售房源统计数据
        try:
            for element in soup.find_all(text=True):
                if '可售房源统计' in str(element):
                    parent = element.find_parent('table')
                    if parent:
                        rows = parent.find_all('tr')
                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) >= 2:
                                label = cols[0].get_text(strip=True)
                                value = cols[1].get_text(strip=True)

                                if '可售房源套数' in label:
                                    data['可售房源套数'] = safe_int(value)
                                elif '可售房源面积' in label:
                                    data['可售房源面积'] = safe_float(value)
                                elif '可售住宅套数' in label:
                                    data['可售住宅套数'] = safe_int(value)
                                elif '可售住宅面积' in label:
                                    data['可售住宅面积'] = safe_float(value)
                    break
        except Exception as e:
            logger.warning(f"解析可售房源统计失败：{e}")

        # 解析新发布房源数据
        try:
            for element in soup.find_all(text=True):
                text = str(element)
                if '新发布房源套数' in text or '新发布住宅套数' in text:
                    parent = element.find_parent('table')
                    if parent:
                        rows = parent.find_all('tr')
                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) >= 2:
                                label = cols[0].get_text(strip=True)
                                value = cols[1].get_text(strip=True)

                                if '新发布房源套数' in label:
                                    data['新发布房源套数'] = safe_int(value)
                                elif '新发布房源面积' in label:
                                    data['新发布房源面积'] = safe_float(value)
                                elif '新发布住宅套数' in label:
                                    data['新发布住宅套数'] = safe_int(value)
                                elif '新发布住宅面积' in label:
                                    data['新发布住宅面积'] = safe_float(value)
                    break
        except Exception as e:
            logger.warning(f"解析新发布房源数据失败：{e}")

        df = pd.DataFrame([data])
        df = df[['日期', '签约套数', '签约面积', '住宅签约套数', '住宅签约面积',
                 '可售房源套数', '可售房源面积', '可售住宅套数', '可售住宅面积',
                 '新发布房源套数', '新发布房源面积', '新发布住宅套数', '新发布住宅面积']]

        logger.info(f"成功解析每日数据，日期：{date_from_new_listing}")
        return df

    except Exception as e:
        logger.error(f"解析每日数据失败：{e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()


def parse_commercial_data(soup, logger):
    """
    解析商品房数据统计
    包括8个部分：可售期房统计、未签约现房统计、现房项目情况、预售许可、
             期房网上认购、期房网上签约、现房网上认购、现房网上签约
    """
    logger.info("开始解析商品房数据...")

    try:
        # 从期房网上认购标题中提取日期
        date_pattern = re.compile(r'(\d{4})/(\d{1,2})/(\d{1,2}).*期房网上认购')
        date_from_pre_sale = None
        for element in soup.find_all(text=True):
            match = date_pattern.search(str(element))
            if match:
                year = match.group(1)
                month = match.group(2).zfill(2)
                day = match.group(3).zfill(2)
                date_from_pre_sale = f"{year}-{month}-{day}"
                logger.info(f"从期房网上认购标题提取日期：{date_from_pre_sale}")
                break

        if not date_from_pre_sale:
            logger.error("无法提取日期，跳过商品房数据解析")
            return pd.DataFrame()

        # 初始化数据字典
        data = {
            '日期': date_from_pre_sale,
            # 可售期房统计
            '可售期房套数': -1,
            '可售期房面积': -1,
            '可售期房住宅套数': -1,
            '可售期房住宅面积': -1,
            '可售期房商业单元': -1,
            '可售期房商业面积': -1,
            '可售期房办公单元': -1,
            '可售期房办公面积': -1,
            '可售期房车位个数': -1,
            '可售期房车位面积': -1,
            # 未签约现房统计
            '未签约现房套数': -1,
            '未签约现房面积': -1,
            '未签约现房住宅套数': -1,
            '未签约现房住宅面积': -1,
            '未签约现房商业单元': -1,
            '未签约现房商业面积': -1,
            # 现房项目情况
            '现房项目个数': -1,
            '现房初始登记面积': -1,
            '现房住宅套数': -1,
            '现房住宅面积': -1,
            '现房商业单元': -1,
            '现房商业面积': -1,
            # 预售许可
            '预售许可证': -1,
            '预售许可面积': -1,
            '预售住宅套数': -1,
            '预售住宅面积': -1,
            # 期房网上认购
            '期房认购套数': -1,
            '期房认购面积': -1,
            '期房认购住宅套数': -1,
            '期房认购住宅面积': -1,
            # 期房网上签约
            '期房签约套数': -1,
            '期房签约面积': -1,
            '期房签约住宅套数': -1,
            '期房签约住宅面积': -1,
            # 现房网上认购
            '现房认购套数': -1,
            '现房认购面积': -1,
            '现房认购住宅套数': -1,
            '现房认购住宅面积': -1,
            # 现房网上签约
            '现房签约套数': -1,
            '现房签约面积': -1,
            '现房签约住宅套数': -1,
            '现房签约住宅面积': -1
        }

        # 解析可售期房统计
        try:
            for element in soup.find_all(text=True):
                if '可售期房统计' in str(element):
                    parent = element.find_parent('table')
                    if parent:
                        rows = parent.find_all('tr')
                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) >= 2:
                                label = cols[0].get_text(strip=True)
                                value = cols[1].get_text(strip=True)

                                if '可售房屋套数' in label and '其中' not in label:
                                    data['可售期房套数'] = safe_int(value)
                                elif '可售房屋面积' in label and '其中' not in label:
                                    data['可售期房面积'] = safe_float(value)
                                elif '其中' in label and '住宅套数' in label:
                                    data['可售期房住宅套数'] = safe_int(value)
                                elif '商业单元' in label:
                                    data['可售期房商业单元'] = safe_int(value)
                                elif '办公单元' in label:
                                    data['可售期房办公单元'] = safe_int(value)
                                elif '车位个数' in label:
                                    data['可售期房车位个数'] = safe_int(value)
                    break
        except Exception as e:
            logger.warning(f"解析可售期房统计失败：{e}")

        # 解析未签约现房统计
        try:
            for element in soup.find_all(text=True):
                if '未签约现房统计' in str(element):
                    parent = element.find_parent('table')
                    if parent:
                        rows = parent.find_all('tr')
                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) >= 2:
                                label = cols[0].get_text(strip=True)
                                value = cols[1].get_text(strip=True)

                                if '未签约套数' in label and '其中' not in label:
                                    data['未签约现房套数'] = safe_int(value)
                                elif '未签约面积' in label and '其中' not in label:
                                    data['未签约现房面积'] = safe_float(value)
                                elif '其中' in label and '住宅套数' in label:
                                    data['未签约现房住宅套数'] = safe_int(value)
                                elif '面积(M' in label and '其中' in str(row) and '住宅' in str(row):
                                    data['未签约现房住宅面积'] = safe_float(value)
                                elif '商业单元' in label and '现房' in str(row):
                                    data['未签约现房商业单元'] = safe_int(value)
                                elif '商业单元' in str(row) and '面积(M' in label and '现房' in str(row):
                                    data['未签约现房商业面积'] = safe_float(value)
                    break
        except Exception as e:
            logger.warning(f"解析未签约现房统计失败：{e}")

        # 解析现房项目情况
        try:
            for element in soup.find_all(text=True):
                if '现房项目情况' in str(element):
                    parent = element.find_parent('table')
                    if parent:
                        rows = parent.find_all('tr')
                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) >= 2:
                                label = cols[0].get_text(strip=True)
                                value = cols[1].get_text(strip=True)

                                if '现房项目个数' in label:
                                    data['现房项目个数'] = safe_int(value)
                                elif '初始登记面积' in label:
                                    data['现房初始登记面积'] = safe_float(value)
                                elif '其中' in label and '住宅套数' in label and '现房' in str(row):
                                    data['现房住宅套数'] = safe_int(value)
                                elif '面积(M' in label and '其中' in str(row) and '住宅' in str(row) and '现房' in str(row):
                                    data['现房住宅面积'] = safe_float(value)
                                elif '商业单元' in label and '现房' in str(row):
                                    data['现房商业单元'] = safe_int(value)
                                elif '商业单元' in str(row) and '面积(M' in label and '现房' in str(row):
                                    data['现房商业面积'] = safe_float(value)
                    break
        except Exception as e:
            logger.warning(f"解析现房项目情况失败：{e}")

        # 解析预售许可
        try:
            for element in soup.find_all(text=True):
                if '预售许可' in str(element) and '年' in str(element) and '月' in str(element):
                    parent = element.find_parent('table')
                    if parent:
                        rows = parent.find_all('tr')
                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) >= 2:
                                label = cols[0].get_text(strip=True)
                                value = cols[1].get_text(strip=True)

                                if '批准预售许可证' in label:
                                    data['预售许可证'] = safe_int(value)
                                elif '批准预售面积' in label:
                                    data['预售许可面积'] = safe_float(value)
                                elif '其中' in label and '住宅套数' in label:
                                    data['预售住宅套数'] = safe_int(value)
                                elif '面积(M' in label and '其中' in str(row) and '预售' in str(row):
                                    data['预售住宅面积'] = safe_float(value)
                    break
        except Exception as e:
            logger.warning(f"解析预售许可失败：{e}")

        # 解析期房网上认购
        try:
            for element in soup.find_all(text=True):
                text = str(element)
                if '期房网上认购' in text and date_pattern.search(text):
                    parent = element.find_parent('table')
                    if parent:
                        rows = parent.find_all('tr')
                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) >= 2:
                                label = cols[0].get_text(strip=True)
                                value = cols[1].get_text(strip=True)

                                if '网上认购套数' in label and '期房' in str(row):
                                    data['期房认购套数'] = safe_int(value)
                                elif '网上认购面积' in label and '期房' in str(row):
                                    data['期房认购面积'] = safe_float(value)
                                elif '其中' in label and '住宅套数' in label:
                                    data['期房认购住宅套数'] = safe_int(value)
                                elif '面积(M' in label and '其中' in str(row) and '认购' in str(row):
                                    data['期房认购住宅面积'] = safe_float(value)
                    break
        except Exception as e:
            logger.warning(f"解析期房网上认购失败：{e}")

        # 解析期房网上签约
        try:
            for element in soup.find_all(text=True):
                text = str(element)
                if '期房网上签约' in text and date_pattern.search(text):
                    parent = element.find_parent('table')
                    if parent:
                        rows = parent.find_all('tr')
                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) >= 2:
                                label = cols[0].get_text(strip=True)
                                value = cols[1].get_text(strip=True)

                                if '网上签约套数' in label and '期房' in str(row):
                                    data['期房签约套数'] = safe_int(value)
                                elif '网上签约面积' in label and '期房' in str(row):
                                    data['期房签约面积'] = safe_float(value)
                                elif '其中' in label and '住宅套数' in label:
                                    data['期房签约住宅套数'] = safe_int(value)
                                elif '面积(M' in label and '其中' in str(row) and '签约' in str(row):
                                    data['期房签约住宅面积'] = safe_float(value)
                    break
        except Exception as e:
            logger.warning(f"解析期房网上签约失败：{e}")

        # 解析现房网上认购
        try:
            for element in soup.find_all(text=True):
                text = str(element)
                if '现房网上认购' in text and date_pattern.search(text):
                    parent = element.find_parent('table')
                    if parent:
                        rows = parent.find_all('tr')
                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) >= 2:
                                label = cols[0].get_text(strip=True)
                                value = cols[1].get_text(strip=True)

                                if '网上认购套数' in label and '现房' in str(row):
                                    data['现房认购套数'] = safe_int(value)
                                elif '网上认购面积' in label and '现房' in str(row):
                                    data['现房认购面积'] = safe_float(value)
                                elif '其中' in label and '住宅套数' in label:
                                    data['现房认购住宅套数'] = safe_int(value)
                                elif '面积(M' in label and '其中' in str(row) and '认购' in str(row):
                                    data['现房认购住宅面积'] = safe_float(value)
                    break
        except Exception as e:
            logger.warning(f"解析现房网上认购失败：{e}")

        # 解析现房网上签约
        try:
            for element in soup.find_all(text=True):
                text = str(element)
                if '现房网上签约' in text and date_pattern.search(text):
                    parent = element.find_parent('table')
                    if parent:
                        rows = parent.find_all('tr')
                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) >= 2:
                                label = cols[0].get_text(strip=True)
                                value = cols[1].get_text(strip=True)

                                if '网上签约套数' in label and '现房' in str(row):
                                    data['现房签约套数'] = safe_int(value)
                                elif '网上签约面积' in label and '现房' in str(row):
                                    data['现房签约面积'] = safe_float(value)
                                elif '其中' in label and '住宅套数' in label:
                                    data['现房签约住宅套数'] = safe_int(value)
                                elif '面积(M' in label and '其中' in str(row) and '签约' in str(row):
                                    data['现房签约住宅面积'] = safe_float(value)
                    break
        except Exception as e:
            logger.warning(f"解析现房网上签约失败：{e}")

        df = pd.DataFrame([data])
        logger.info(f"成功解析商品房数据，日期：{date_from_pre_sale}")
        return df

    except Exception as e:
        logger.error(f"解析商品房数据失败：{e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()
