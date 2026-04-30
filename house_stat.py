"""
房地产签约数据抓取主程序
从北京市住建委网站抓取存量房网上签约月统计数据
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
import logging
from datetime import datetime
import time
import config


def ensure_directories():
    """
    确保所有必需的目录存在
    如果目录不存在，则创建它们
    """
    for directory in config.DIRECTORIES:
        if not os.path.exists(directory):
            try:
                os.makedirs(directory, exist_ok=True)
                print(f"[INFO] 创建目录: {directory}")
            except OSError as e:
                print(f"[ERROR] 创建目录失败: {directory}, 错误: {e}")
                raise
        else:
            print(f"[INFO] 目录已存在: {directory}")


def setup_logging():
    """初始化日志系统"""
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(config.LOG_FILE, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def fetch_html(logger):
    """
    抓取网页HTML内容
    失败时自动重试
    """
    for attempt in range(config.MAX_RETRIES):
        try:
            logger.info(f"正在抓取数据（第 {attempt + 1} 次尝试）...")
            response = requests.get(
                config.BASE_URL,
                headers=config.HEADERS,
                timeout=config.TIMEOUT
            )
            response.raise_for_status()
            response.encoding = 'utf-8'

            logger.info("成功获取网页内容")
            return response.text

        except requests.RequestException as e:
            logger.error(f"请求失败：{e}")
            if attempt < config.MAX_RETRIES - 1:
                logger.info(f"等待 {config.RETRY_DELAY} 秒后重试...")
                time.sleep(config.RETRY_DELAY)
            else:
                logger.error("已达到最大重试次数，放弃抓取")
                raise


def extract_data_month(soup, logger):
    """
    从页面标题中提取数据年月
    例如：2025年12月存量房网上签约 -> (2025, 12)
    """
    try:
        # 查找包含年月信息的标题
        title_pattern = re.compile(r'(\d{4})年(\d{1,2})月存量房网上签约')

        # 在页面中搜索匹配的文本
        for text in soup.stripped_strings:
            match = title_pattern.search(text)
            if match:
                year = match.group(1)
                month = match.group(2).zfill(2)  # 补零为两位数
                year_month = f"{year}-{month}"
                logger.info(f"提取到数据年月：{year_month}")
                return year_month

        # 如果找不到，尝试从页面标题标签获取
        title_tag = soup.find('title')
        if title_tag:
            logger.warning(f"未找到标准格式年月，页面标题：{title_tag.text}")

        logger.warning("未能从页面提取年月信息，使用当前日期的上月")
        return None

    except Exception as e:
        logger.error(f"提取年月信息失败：{e}")
        return None


def get_previous_month():
    """获取当前日期的上一个月"""
    from dateutil.relativedelta import relativedelta
    today = datetime.now()
    last_month = today - relativedelta(months=1)
    return last_month.strftime("%Y-%m")


def parse_agency_data(soup, year_month, logger):
    """
    解析按经纪机构统计的数据
    表格ID: table_clf1
    列：序号、房地产经纪机构名称、发布套数、签约套数、退房套数
    注意：页面有两个50%宽度的列，各显示5条数据
    """
    logger.info("开始解析经纪机构数据...")

    try:
        # 查找经纪机构表格
        outer_table = soup.find('table', id='table_clf1')
        if not outer_table:
            # 尝试查找包含"房地产经纪机构名称"的表格
            table = soup.find('td', string=re.compile('房地产经纪机构名称'))
            if table:
                outer_table = table.find_parent('table')

        if not outer_table:
            logger.error("未找到经纪机构数据表格")
            return pd.DataFrame()

        # 查找所有内层的带border的表格（应该有2个，每个显示5条数据）
        inner_tables = outer_table.find_all('table', border='1')
        if len(inner_tables) == 0:
            # 备用方案：查找所有内层table
            all_tables = outer_table.find_all('table')
            inner_tables = [t for t in all_tables if len(t.find_all('tr')) > 2]

        data = []
        for inner_table in inner_tables:
            rows = inner_table.find_all('tr')
            for row in rows[1:]:  # 跳过表头
                cols = row.find_all('td')
                if len(cols) >= 5:
                    try:
                        seq_num = cols[0].text.strip()
                        agency_name = cols[1].text.strip()
                        list_count = cols[2].text.strip()
                        sign_count = cols[3].text.strip()
                        refund_count = cols[4].text.strip()

                        # 跳过空行或非数据行
                        if not seq_num or not seq_num.isdigit():
                            continue

                        data.append({
                            '年月': year_month,
                            '序号': int(seq_num),
                            '经纪机构': agency_name,
                            '发布套数': int(list_count) if list_count.isdigit() else -1,
                            '签约套数': int(sign_count) if sign_count.isdigit() else -1,
                            '退房套数': int(refund_count) if refund_count.isdigit() else -1
                        })
                    except (ValueError, IndexError) as e:
                        logger.warning(f"跳过异常行：{e}")
                        continue

        df = pd.DataFrame(data)
        logger.info(f"成功解析经纪机构数据 {len(df)} 条")
        return df

    except Exception as e:
        logger.error(f"解析经纪机构数据失败：{e}")
        return pd.DataFrame()


def parse_district_data(soup, year_month, logger):
    """
    解析按区县统计的数据
    表格ID: table_clf2
    需要转置：横向表格转换为竖向
    表格分为两部分：每部分包含区县名、套数、面积三行
    """
    logger.info("开始解析区县数据...")

    try:
        # 查找区县表格
        outer_table = soup.find('table', id='table_clf2')
        if not outer_table:
            logger.error("未找到区县数据表格")
            return pd.DataFrame()

        # table_clf2 是嵌套表格，需要找到内层的带边框的表格
        # 查找内层table（有bordercolor属性的）
        inner_table = outer_table.find('table', bordercolor=lambda x: x and '#4a9ee0' in x)
        if not inner_table:
            # 如果找不到，尝试找第一个table
            inner_table = outer_table.find('table')
            if not inner_table:
                logger.error("未找到内层区县数据表格")
                return pd.DataFrame()

        # 获取内层表格的所有行
        rows = inner_table.find_all('tr')

        if len(rows) < 6:
            logger.error(f"区县表格行数不足，期望至少6行，实际{len(rows)}行")
            return pd.DataFrame()

        data = []

        # 处理第一部分（前9个区县）
        # 第1行：区县名称
        district_row1 = rows[0].find_all('td')
        districts1 = [td.get_text(strip=True) for td in district_row1[1:]]  # 跳过第一个"所在区"列

        # 第2行：套数
        count_row1 = rows[1].find_all('td')
        counts1 = [td.get_text(strip=True) for td in count_row1[1:]]

        # 第3行：面积
        area_row1 = rows[2].find_all('td')
        areas1 = [td.get_text(strip=True) for td in area_row1[1:]]

        # 处理第二部分（后9个区县）
        # 第4行：区县名称
        district_row2 = rows[3].find_all('td')
        districts2 = [td.get_text(strip=True) for td in district_row2[1:]]

        # 第5行：套数
        count_row2 = rows[4].find_all('td')
        counts2 = [td.get_text(strip=True) for td in count_row2[1:]]

        # 第6行：面积
        area_row2 = rows[5].find_all('td')
        areas2 = [td.get_text(strip=True) for td in area_row2[1:]]

        # 合并两部分数据
        all_districts = districts1 + districts2
        all_counts = counts1 + counts2
        all_areas = areas1 + areas2

        logger.info(f"区县数据：找到{len(all_districts)}个区县，{len(all_counts)}个套数，{len(all_areas)}个面积")

        # 转置数据：横向变竖向
        for i, district in enumerate(all_districts):
            if i < len(all_counts) and i < len(all_areas):
                try:
                    count_str = all_counts[i].replace(',', '').strip()
                    area_str = all_areas[i].replace(',', '').strip()

                    count_val = float(count_str) if count_str else -1
                    area_val = float(area_str) if area_str else -1

                    data.append({
                        '年月': year_month,
                        '区县': district,
                        '签约套数': int(count_val) if count_val == int(count_val) else count_val,
                        '成交面积': area_val
                    })
                except (ValueError, IndexError) as e:
                    logger.warning(f"跳过异常数据：区县={district}, 套数={all_counts[i] if i < len(all_counts) else 'N/A'}, 面积={all_areas[i] if i < len(all_areas) else 'N/A'}, 错误={e}")
                    continue

        df = pd.DataFrame(data)
        logger.info(f"成功解析区县数据 {len(df)} 条")
        return df

    except Exception as e:
        logger.error(f"解析区县数据失败：{e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()


def parse_area_data(soup, year_month, logger):
    """
    解析按面积统计的数据
    表格ID: table_clf3
    需要转置：横向表格转换为竖向
    """
    logger.info("开始解析面积数据...")

    try:
        # 查找面积表格
        outer_table = soup.find('table', id='table_clf3')
        if not outer_table:
            logger.error("未找到面积数据表格")
            return pd.DataFrame()

        # table_clf3 也是嵌套表格，需要找到内层的带边框的表格
        inner_table = outer_table.find('table', bordercolor=lambda x: x and '#4a9ee0' in x)
        if not inner_table:
            inner_table = outer_table.find('table')
            if not inner_table:
                logger.error("未找到内层面积数据表格")
                return pd.DataFrame()

        # 获取内层表格的所有行
        rows = inner_table.find_all('tr')

        if len(rows) < 3:
            logger.error(f"面积表格行数不足，期望至少3行，实际{len(rows)}行")
            return pd.DataFrame()

        # 第一行：面积区间（横向表头）
        area_header_row = rows[0].find_all('td')
        area_ranges = []
        for td in area_header_row[1:]:  # 跳过第一个"面积"列
            # 清理文本，移除HTML标签和多余空格
            text = td.get_text(strip=True)
            area_ranges.append(text)

        # 第二行：成交套数
        count_row = rows[1].find_all('td')
        counts = [td.get_text(strip=True) for td in count_row[1:]]

        # 第三行：成交面积
        area_row = rows[2].find_all('td')
        areas = [td.get_text(strip=True) for td in area_row[1:]]

        logger.info(f"面积数据：找到{len(area_ranges)}个区间，{len(counts)}个套数，{len(areas)}个面积")

        # 转置数据：横向变竖向
        data = []
        for i, area_range in enumerate(area_ranges):
            if i < len(counts) and i < len(areas):
                try:
                    # 清理数值字符串
                    count_str = counts[i].replace(',', '').strip()
                    area_str = areas[i].replace(',', '').strip()

                    count_val = float(count_str) if count_str else -1
                    area_val = float(area_str) if area_str else -1

                    data.append({
                        '年月': year_month,
                        '面积区间': area_range,
                        '成交套数': int(count_val) if count_val == int(count_val) else count_val,
                        '成交面积': area_val
                    })
                except (ValueError, IndexError) as e:
                    logger.warning(f"跳过异常数据：面积区间={area_range}, 套数={counts[i] if i < len(counts) else 'N/A'}, 面积={areas[i] if i < len(areas) else 'N/A'}, 错误={e}")
                    continue

        df = pd.DataFrame(data)
        logger.info(f"成功解析面积数据 {len(df)} 条")
        return df

    except Exception as e:
        logger.error(f"解析面积数据失败：{e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()


def parse_price_data(soup, year_month, logger):
    """
    解析按价格统计的数据
    表格ID: table_clf4
    需要转置：横向表格转换为竖向
    行：价格区间、发布套数、发布面积、成交套数、成交面积
    """
    logger.info("开始解析价格数据...")

    try:
        # 查找价格表格
        outer_table = soup.find('table', id='table_clf4')
        if not outer_table:
            logger.error("未找到价格数据表格")
            return pd.DataFrame()

        # table_clf4 也是嵌套表格，需要找到内层的带边框的表格
        inner_table = outer_table.find('table', bordercolor=lambda x: x and '#4a9ee0' in x)
        if not inner_table:
            inner_table = outer_table.find('table')
            if not inner_table:
                logger.error("未找到内层价格数据表格")
                return pd.DataFrame()

        # 获取内层表格的所有行
        rows = inner_table.find_all('tr')

        if len(rows) < 5:
            logger.error(f"价格表格行数不足，期望至少5行，实际{len(rows)}行")
            return pd.DataFrame()

        # 第一行：价格区间（横向表头）
        price_header_row = rows[0].find_all('td')
        price_ranges = []
        for td in price_header_row[1:]:  # 跳过第一个"价格"列
            text = td.get_text(strip=True)
            price_ranges.append(text)

        # 第二行：发布套数
        list_count_row = rows[1].find_all('td')
        list_counts = [td.get_text(strip=True) for td in list_count_row[1:]]

        # 第三行：发布面积
        list_area_row = rows[2].find_all('td')
        list_areas = [td.get_text(strip=True) for td in list_area_row[1:]]

        # 第四行：成交套数
        deal_count_row = rows[3].find_all('td')
        deal_counts = [td.get_text(strip=True) for td in deal_count_row[1:]]

        # 第五行：成交面积
        deal_area_row = rows[4].find_all('td')
        deal_areas = [td.get_text(strip=True) for td in deal_area_row[1:]]

        logger.info(f"价格数据：找到{len(price_ranges)}个区间")

        # 转置数据：横向变竖向
        data = []
        for i, price_range in enumerate(price_ranges):
            if i < len(list_counts) and i < len(list_areas) and i < len(deal_counts) and i < len(deal_areas):
                try:
                    # 清理数值字符串
                    list_count_str = list_counts[i].replace(',', '').strip()
                    list_area_str = list_areas[i].replace(',', '').strip()
                    deal_count_str = deal_counts[i].replace(',', '').strip()
                    deal_area_str = deal_areas[i].replace(',', '').strip()

                    list_count_val = float(list_count_str) if list_count_str else -1
                    list_area_val = float(list_area_str) if list_area_str else -1
                    deal_count_val = float(deal_count_str) if deal_count_str else -1
                    deal_area_val = float(deal_area_str) if deal_area_str else -1

                    data.append({
                        '年月': year_month,
                        '价格区间': price_range,
                        '发布套数': int(list_count_val) if list_count_val == int(list_count_val) else list_count_val,
                        '发布面积': list_area_val,
                        '成交套数': int(deal_count_val) if deal_count_val == int(deal_count_val) else deal_count_val,
                        '成交面积': deal_area_val
                    })
                except (ValueError, IndexError) as e:
                    logger.warning(f"跳过异常数据：价格区间={price_range}, 错误={e}")
                    continue

        df = pd.DataFrame(data)
        logger.info(f"成功解析价格数据 {len(df)} 条")
        return df

    except Exception as e:
        logger.error(f"解析价格数据失败：{e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()


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
        # 辅助函数：安全转换数值，失败返回-1
        def safe_int(value, default=-1):
            try:
                clean_val = str(value).replace(',', '').strip()
                return int(clean_val) if clean_val else default
            except (ValueError, AttributeError):
                return default

        def safe_float(value, default=-1):
            try:
                clean_val = str(value).replace(',', '').strip()
                return float(clean_val) if clean_val else default
            except (ValueError, AttributeError):
                return default

        # 1. 从"新发布房源"标题中提取日期
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
            # 备用：从存量房网上签约标题提取日期
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

        # 初始化数据字典，默认值为-1
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

        # 2. 解析存量房网上签约数据（原有逻辑）
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

        # 3. 解析可售房源统计数据
        try:
            # 查找"可售房源统计"标题
            for element in soup.find_all(text=True):
                if '可售房源统计' in str(element):
                    # 找到包含该标题的表格
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

        # 4. 解析新发布房源数据
        try:
            # 查找"新发布房源"相关内容
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

        # 创建DataFrame
        df = pd.DataFrame([data])
        # 调整列顺序
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
    日期从期房网上认购标题中提取（格式：2026/4/29期房网上认购）
    """
    logger.info("开始解析商品房数据...")

    try:
        # 辅助函数：安全转换数值
        def safe_int(value, default=-1):
            try:
                clean_val = str(value).replace(',', '').strip()
                return int(clean_val) if clean_val else default
            except (ValueError, AttributeError):
                return default

        def safe_float(value, default=-1):
            try:
                clean_val = str(value).replace(',', '').strip()
                return float(clean_val) if clean_val else default
            except (ValueError, AttributeError):
                return default

        # 1. 从期房网上认购标题中提取日期
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

        # 初始化数据字典，默认值为-1
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

        # 2. 解析可售期房统计
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

        # 3. 解析未签约现房统计
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

        # 4. 解析现房项目情况
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

        # 5. 解析预售许可
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

        # 6. 解析期房网上认购
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

        # 7. 解析期房网上签约
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

        # 8. 解析现房网上认购
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

        # 9. 解析现房网上签约
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


def parse_month_summary(soup, logger):
    """
    解析月度存量房网上签约汇总数据
    查找"YYYY年MM月存量房网上签约"格式的表格
    提取：网上签约套数、网上签约面积、住宅签约套数、住宅签约面积
    """
    logger.info("开始解析月度汇总数据...")

    try:
        # 查找包含月度汇总数据的表格
        # 格式：2025年12月存量房网上签约
        # 注意：要排除"YYYY/MM/DD存量房网上签约"格式（每日数据）
        month_pattern = re.compile(r'(\d{4})年(\d{1,2})月存量房网上签约')
        daily_pattern = re.compile(r'\d{4}/\d{1,2}/\d{1,2}存量房网上签约')

        best_table = None
        best_month = None
        max_sign_count = 0

        # 在所有表格中搜索
        for table in soup.find_all('table'):
            table_text = table.get_text()
            match = month_pattern.search(table_text)

            # 只匹配月度格式，不匹配日格式
            if match and not daily_pattern.search(table_text):
                year = match.group(1)
                month = match.group(2).zfill(2)
                month_str = f"{year}-{month}"

                # 先提取数据看看签约套数，选择最大的（月度汇总应该远大于每日数据）
                temp_sign_count = 0
                rows = table.find_all('tr')
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 2:
                        label = cols[0].get_text(strip=True)
                        value_text = cols[1].get_text(strip=True)
                        if '网上签约套数' in label and '住宅' not in label:
                            try:
                                temp_sign_count = int(value_text) if value_text.isdigit() else -1
                            except:
                                temp_sign_count = 0
                            break

                # 选择签约套数最大的表格（月度汇总通常有上万套）
                if temp_sign_count > max_sign_count:
                    max_sign_count = temp_sign_count
                    best_table = table
                    best_month = month_str

        if best_table and best_month:
            logger.info(f"找到月度汇总数据：{best_month}，签约套数：{max_sign_count}")

            # 解析表格中的数据
            data = {
                '月份': best_month,
                '网上签约套数': 0,
                '网上签约面积(m2)': 0.0,
                '住宅签约套数': 0,
                '住宅签约面积(m2)': 0.0
            }

            rows = best_table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    label = cols[0].get_text(strip=True)
                    value_text = cols[1].get_text(strip=True)

                    # 根据标签提取对应的值
                    if '网上签约套数' in label and '住宅' not in label:
                        data['网上签约套数'] = int(value_text) if value_text.isdigit() else -1
                    elif '网上签约面积' in label and '住宅' not in label:
                        # 移除可能的单位和空格
                        area_value = value_text.replace('m²', '').replace(' ', '').strip()
                        data['网上签约面积(m2)'] = float(area_value) if area_value else -1
                    elif '住宅签约套数' in label:
                        data['住宅签约套数'] = int(value_text) if value_text.isdigit() else -1
                    elif '住宅签约面积' in label:
                        area_value = value_text.replace('m²', '').replace(' ', '').strip()
                        data['住宅签约面积(m2)'] = float(area_value) if area_value else -1

            # 验证是否成功提取到有效数据
            if data['网上签约套数'] > 0 or data['住宅签约套数'] > 0:
                df = pd.DataFrame([data])
                logger.info(f"成功解析月度汇总数据：月份={best_month}, "
                          f"网上签约套数={data['网上签约套数']}, "
                          f"住宅签约套数={data['住宅签约套数']}")
                return df
            else:
                logger.warning(f"未能从表格中提取到有效的月度汇总数据：{best_month}")
                return pd.DataFrame()

        logger.warning("未找到月度汇总数据表格")
        return pd.DataFrame()

    except Exception as e:
        logger.error(f"解析月度汇总数据失败：{e}")
        return pd.DataFrame()


def parse_five_year_commercial(soup, logger):
    """
    解析近五年新建商品房网签情况
    表格ID: table_001
    数据：2020-2024年，每年住宅套数、住宅面积、非住宅面积
    """
    logger.info("开始解析近五年新建商品房数据...")

    try:
        table = soup.find('table', id='table_001')
        if not table:
            logger.error("未找到近五年新建商品房数据表格")
            return pd.DataFrame()

        # 获取当前日期作为数据更新日期
        update_date = datetime.now().strftime("%Y-%m-%d")

        data = []

        # 方法1：遍历数据行（跳过前5行的header）
        rows = table.find_all('tr')
        for row in rows[5:]:  # 跳过前5行header
            cells = row.find_all('td')
            if len(cells) >= 4:
                # 检查第一个单元格是否包含年份
                first_cell_text = cells[0].get_text(strip=True)
                year_match = re.search(r'(\d{4})年', first_cell_text)
                if year_match:
                    year = int(year_match.group(1))
                    if 2020 <= year <= 2024:  # 确保是目标年份范围
                        try:
                            housing_units = float(cells[1].get_text(strip=True))
                            housing_area = float(cells[2].get_text(strip=True))
                            commercial_area = float(cells[3].get_text(strip=True))

                            data.append({
                                '年份': year,
                                '住宅套数万': housing_units,
                                '住宅面积万m2': housing_area,
                                '非住宅面积万m2': commercial_area,
                                '数据更新日期': update_date
                            })
                        except (ValueError, IndexError) as e:
                            logger.warning(f"跳过异常行：{first_cell_text}, 错误：{e}")
                            continue

        # 如果方法1失败，使用方法2：正则表达式从表格文本提取
        if not data or len(data) < 5:
            logger.info("方法1未找到完整数据，使用方法2（正则表达式）")
            data = []
            table_text = table.get_text()
            years = ['2020', '2021', '2022', '2023', '2024']

            for year in years:
                pattern = re.compile(rf'{year}年\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)')
                match = pattern.search(table_text)
                if match:
                    data.append({
                        '年份': int(year),
                        '住宅套数万': float(match.group(1)),
                        '住宅面积万m2': float(match.group(2)),
                        '非住宅面积万m2': float(match.group(3)),
                        '数据更新日期': update_date
                    })

        df = pd.DataFrame(data)
        if not df.empty:
            # 按年份排序
            df = df.sort_values('年份').reset_index(drop=True)
            logger.info(f"成功解析近五年新建商品房数据 {len(df)} 条")
        else:
            logger.warning("未能解析到近五年新建商品房数据")
        return df

    except Exception as e:
        logger.error(f"解析近五年新建商品房数据失败：{e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()


def parse_five_year_existing(soup, logger):
    """
    解析近五年存量房交易情况
    表格ID: table_002
    数据：2020-2024年，每年住宅套数、住宅面积、非住宅面积
    """
    logger.info("开始解析近五年存量房数据...")

    try:
        table = soup.find('table', id='table_002')
        if not table:
            logger.error("未找到近五年存量房数据表格")
            return pd.DataFrame()

        # 获取当前日期作为数据更新日期
        update_date = datetime.now().strftime("%Y-%m-%d")

        data = []

        # 方法1：遍历数据行（跳过前5行的header）
        rows = table.find_all('tr')
        for row in rows[5:]:  # 跳过前5行header
            cells = row.find_all('td')
            if len(cells) >= 4:
                # 检查第一个单元格是否包含年份
                first_cell_text = cells[0].get_text(strip=True)
                year_match = re.search(r'(\d{4})年', first_cell_text)
                if year_match:
                    year = int(year_match.group(1))
                    if 2020 <= year <= 2024:  # 确保是目标年份范围
                        try:
                            housing_units = float(cells[1].get_text(strip=True))
                            housing_area = float(cells[2].get_text(strip=True))
                            commercial_area = float(cells[3].get_text(strip=True))

                            data.append({
                                '年份': year,
                                '住宅套数万': housing_units,
                                '住宅面积万m2': housing_area,
                                '非住宅面积万m2': commercial_area,
                                '数据更新日期': update_date
                            })
                        except (ValueError, IndexError) as e:
                            logger.warning(f"跳过异常行：{first_cell_text}, 错误：{e}")
                            continue

        # 如果方法1失败，使用方法2：正则表达式从表格文本提取
        if not data or len(data) < 5:
            logger.info("方法1未找到完整数据，使用方法2（正则表达式）")
            data = []
            table_text = table.get_text()
            years = ['2020', '2021', '2022', '2023', '2024']

            for year in years:
                pattern = re.compile(rf'{year}年\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)')
                match = pattern.search(table_text)
                if match:
                    data.append({
                        '年份': int(year),
                        '住宅套数万': float(match.group(1)),
                        '住宅面积万m2': float(match.group(2)),
                        '非住宅面积万m2': float(match.group(3)),
                        '数据更新日期': update_date
                    })

        df = pd.DataFrame(data)
        if not df.empty:
            # 按年份排序
            df = df.sort_values('年份').reset_index(drop=True)
            logger.info(f"成功解析近五年存量房数据 {len(df)} 条")
        else:
            logger.warning("未能解析到近五年存量房数据")
        return df

    except Exception as e:
        logger.error(f"解析近五年存量房数据失败：{e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()


def save_to_csv(df, csv_file, key_column, logger):
    """
    保存数据到CSV文件
    检查数据是否已存在，避免重复

    参数:
        df: 要保存的DataFrame
        csv_file: CSV文件路径
        key_column: 用于判断重复的关键列名（列表，如 ['年月', '经纪机构']）
        logger: 日志记录器

    返回:
        (新增条数, 跳过条数, 新增数据的DataFrame)
    """
    if df.empty:
        logger.warning(f"数据为空，跳过保存：{csv_file}")
        return 0, 0, pd.DataFrame()

    try:
        # 检查文件是否存在
        if os.path.exists(csv_file):
            # 读取现有数据
            existing_df = pd.read_csv(csv_file, encoding=config.CSV_ENCODING)
            logger.info(f"已存在数据文件 {csv_file}，共 {len(existing_df)} 条")

            # 检查重复
            merged = df.merge(existing_df, on=key_column, how='inner', indicator=True)
            duplicates = len(merged)
            new_data = df[~df.set_index(key_column).index.isin(
                existing_df.set_index(key_column).index
            )]

            if duplicates > 0:
                logger.info(f"发现 {duplicates} 条数据已存在，将跳过")

            if not new_data.empty:
                # 追加新数据
                new_data.to_csv(csv_file, mode='a', header=False,
                              index=False, encoding=config.CSV_ENCODING)
                logger.info(f"新增 {len(new_data)} 条数据到 {csv_file}")
            else:
                logger.info(f"没有新数据需要添加到 {csv_file}")

            return len(new_data), duplicates, new_data

        else:
            # 创建新文件
            df.to_csv(csv_file, index=False, encoding=config.CSV_ENCODING)
            logger.info(f"创建新文件 {csv_file}，写入 {len(df)} 条数据")
            return len(df), 0, df

    except Exception as e:
        logger.error(f"保存数据到 {csv_file} 失败：{e}")
        return 0, 0, pd.DataFrame()


def display_results(year_month, new_df_daily, new_df_month, new_df_agency, new_df_district, new_df_area, new_df_price=None, new_df_commercial=None, new_df_existing=None, new_df_commercial_daily=None):
    """
    在控制台展示需要更新（新增）到文件的数据
    """
    W = 70  # 总宽度
    has_new = not (new_df_daily.empty and new_df_month.empty
                   and new_df_agency.empty and new_df_district.empty and new_df_area.empty
                   and (new_df_price is None or new_df_price.empty)
                   and (new_df_commercial is None or new_df_commercial.empty)
                   and (new_df_existing is None or new_df_existing.empty)
                   and (new_df_commercial_daily is None or new_df_commercial_daily.empty))

    print()
    print("=" * W)
    if has_new:
        print("  存量房网上签约数据 - 新增数据".center(W - 4))
    else:
        print("  存量房网上签约数据 - 无新增".center(W - 4))
    print("=" * W)

    if not has_new:
        print()
        print(f"  目标数据月份：{year_month}")
        print("  所有数据均已存在，无需更新。")
        print()
        print("=" * W)
        print()
        return

    print()
    print(f"  目标数据月份：{year_month}")

    # --- 1. 每日数据 ---
    if not new_df_daily.empty:
        print()
        print("-" * W)
        print("  [ 新增 - 每日签约数据 ]")
        print("-" * W)
        for _, row in new_df_daily.iterrows():
            print(f"  日期：{row['日期']}")
            print(f"    签约套数：{int(row['签约套数'])} 套")
            print(f"    签约面积：{row['签约面积']:.2f} m2")
            print(f"    住宅签约套数：{int(row['住宅签约套数'])} 套")
            print(f"    住宅签约面积：{row['住宅签约面积']:.2f} m2")

    # --- 2. 月度汇总 ---
    if not new_df_month.empty:
        print()
        print("-" * W)
        print("  [ 新增 - 月度汇总 ]")
        print("-" * W)
        for _, row in new_df_month.iterrows():
            print(f"  月份：{row['月份']}")
            print(f"    网上签约套数：{int(row['网上签约套数'])} 套")
            print(f"    网上签约面积：{row['网上签约面积(m2)']:.2f} m2")
            print(f"    住宅签约套数：{int(row['住宅签约套数'])} 套")
            print(f"    住宅签约面积：{row['住宅签约面积(m2)']:.2f} m2")

    # --- 3. 经纪机构 ---
    if not new_df_agency.empty:
        print()
        print("-" * W)
        print(f"  [ 新增 - 经纪机构签约排行 ]（{len(new_df_agency)} 条）")
        print("-" * W)
        df_show = new_df_agency.sort_values('签约套数', ascending=False).head(10)
        print(f"  {'排名':<6}{'经纪机构':<26}{'发布套数':>10}{'签约套数':>10}{'退房套数':>10}")
        print(f"  {'-'*6}{'-'*26}{'-'*10}{'-'*10}{'-'*10}")
        for _, r in df_show.iterrows():
            name = r['经纪机构']
            if len(name) > 12:
                name = name[:11] + "…"
            print(f"  {int(r['序号']):<6}{name:<26}{int(r['发布套数']):>10}{int(r['签约套数']):>10}{int(r['退房套数']):>10}")

    # --- 4. 区县分布 ---
    if not new_df_district.empty:
        print()
        print("-" * W)
        print(f"  [ 新增 - 区县签约分布 ]（{len(new_df_district)} 条）")
        print("-" * W)
        df_show = new_df_district.sort_values('签约套数', ascending=False)
        print(f"  {'区县':<10}{'签约套数':>12}{'成交面积(m2)':>16}")
        print(f"  {'-'*10}{'-'*12}{'-'*16}")
        for _, r in df_show.iterrows():
            print(f"  {r['区县']:<10}{int(r['签约套数']):>12}{r['成交面积']:>16.2f}")

    # --- 5. 面积区间分布 ---
    if not new_df_area.empty:
        print()
        print("-" * W)
        print(f"  [ 新增 - 面积区间分布 ]（{len(new_df_area)} 条）")
        print("-" * W)
        print(f"  {'面积区间':<16}{'成交套数':>12}{'成交面积(m2)':>16}")
        print(f"  {'-'*16}{'-'*12}{'-'*16}")
        for _, r in new_df_area.iterrows():
            print(f"  {r['面积区间']:<16}{int(r['成交套数']):>12}{r['成交面积']:>16.2f}")

    # --- 6. 价格区间分布 ---
    if new_df_price is not None and not new_df_price.empty:
        print()
        print("-" * W)
        print(f"  [ 新增 - 价格区间分布 ]（{len(new_df_price)} 条）")
        print("-" * W)
        print(f"  {'价格区间':<16}{'发布套数':>10}{'成交套数':>10}{'成交面积(m2)':>16}")
        print(f"  {'-'*16}{'-'*10}{'-'*10}{'-'*16}")
        for _, r in new_df_price.iterrows():
            print(f"  {r['价格区间']:<16}{int(r['发布套数']):>10}{int(r['成交套数']):>10}{r['成交面积']:>16.2f}")

    # --- 7. 五年新建商品房统计 ---
    if new_df_commercial is not None and not new_df_commercial.empty:
        print()
        print("-" * W)
        print(f"  [ 新增 - 五年新建商品房网签 ]（{len(new_df_commercial)} 条）")
        print("-" * W)
        print(f"  {'年份':<10}{'住宅套数(万)':>14}{'住宅面积(万m2)':>16}{'非住宅面积(万m2)':>18}")
        print(f"  {'-'*10}{'-'*14}{'-'*16}{'-'*18}")
        for _, r in new_df_commercial.iterrows():
            print(f"  {r['年份']:<10}{r['住宅套数万']:>14.2f}{r['住宅面积万m2']:>16.2f}{r['非住宅面积万m2']:>18.2f}")

    # --- 8. 五年存量房统计 ---
    if new_df_existing is not None and not new_df_existing.empty:
        print()
        print("-" * W)
        print(f"  [ 新增 - 五年存量房交易 ]（{len(new_df_existing)} 条）")
        print("-" * W)
        print(f"  {'年份':<10}{'住宅套数(万)':>14}{'住宅面积(万m2)':>16}{'非住宅面积(万m2)':>18}")
        print(f"  {'-'*10}{'-'*14}{'-'*16}{'-'*18}")
        for _, r in new_df_existing.iterrows():
            print(f"  {r['年份']:<10}{r['住宅套数万']:>14.2f}{r['住宅面积万m2']:>16.2f}{r['非住宅面积万m2']:>18.2f}")

    # --- 9. 商品房每日数据 ---
    if new_df_commercial_daily is not None and not new_df_commercial_daily.empty:
        print()
        print("-" * W)
        print(f"  [ 新增 - 商品房每日数据 ]（{len(new_df_commercial_daily)} 条）")
        print("-" * W)
        for _, r in new_df_commercial_daily.iterrows():
            print(f"  日期：{r['日期']}")
            print(f"  可售期房：套数 {int(r['可售期房套数'])} | 面积 {r['可售期房面积']:.2f} | 住宅 {int(r['可售期房住宅套数'])}/{r['可售期房住宅面积']:.2f}")
            print(f"  未签约现房：套数 {int(r['未签约现房套数'])} | 面积 {r['未签约现房面积']:.2f} | 住宅 {int(r['未签约现房住宅套数'])}/{r['未签约现房住宅面积']:.2f}")
            print(f"  现房项目：个数 {int(r['现房项目个数'])} | 住宅 {int(r['现房住宅套数'])}/{r['现房住宅面积']:.2f}")
            print(f"  预售许可：许可证 {int(r['预售许可证'])} | 住宅 {int(r['预售住宅套数'])}/{r['预售住宅面积']:.2f}")
            print(f"  期房认购：套数 {int(r['期房认购套数'])} | 住宅 {int(r['期房认购住宅套数'])}/{r['期房认购住宅面积']:.2f}")
            print(f"  期房签约：套数 {int(r['期房签约套数'])} | 住宅 {int(r['期房签约住宅套数'])}/{r['期房签约住宅面积']:.2f}")
            print(f"  现房认购：套数 {int(r['现房认购套数'])} | 住宅 {int(r['现房认购住宅套数'])}/{r['现房认购住宅面积']:.2f}")
            print(f"  现房签约：套数 {int(r['现房签约套数'])} | 住宅 {int(r['现房签约住宅套数'])}/{r['现房签约住宅面积']:.2f}")

    print()
    print("=" * W)
    print()


def main():
    """主函数"""
    # 0. 确保目录存在（在初始化日志之前）
    ensure_directories()

    # 初始化日志
    logger = setup_logging()
    logger.info("=" * 50)
    logger.info("开始抓取存量房签约数据")
    logger.info("=" * 50)

    try:
        # 1. 抓取网页
        html = fetch_html(logger)
        soup = BeautifulSoup(html, 'html.parser')

        # 2. 提取数据年月
        year_month = extract_data_month(soup, logger)
        if not year_month:
            year_month = get_previous_month()
            logger.info(f"使用计算的上月年月：{year_month}")

        # 3. 解析数据
        logger.info("-" * 50)
        df_agency = parse_agency_data(soup, year_month, logger)
        df_district = parse_district_data(soup, year_month, logger)
        df_area = parse_area_data(soup, year_month, logger)
        df_price = parse_price_data(soup, year_month, logger)
        df_daily = parse_daily_data(soup, logger)
        df_commercial_daily = parse_commercial_data(soup, logger)
        df_month = parse_month_summary(soup, logger)
        df_commercial = parse_five_year_commercial(soup, logger)
        df_existing = parse_five_year_existing(soup, logger)

        # 3.5. 检查并扩展 CSV 文件的列数（向后兼容）
        # 3.5.1. 检查并扩展 resale_daily.csv 的列数（如果需要）
        if not df_daily.empty:
            required_columns = df_daily.columns.tolist()
            if os.path.exists(config.RESALE_DAILY_CSV):
                try:
                    existing_df = pd.read_csv(config.RESALE_DAILY_CSV, encoding=config.CSV_ENCODING)
                    existing_columns = existing_df.columns.tolist()

                    # 如果列数不匹配，需要添加新列
                    if set(required_columns) != set(existing_columns):
                        logger.info(f"检测到 resale_daily.csv 列数不匹配，正在添加新列...")

                        # 为每一行添加缺失的列，值为-1
                        for col in required_columns:
                            if col not in existing_columns:
                                existing_df[col] = -1

                        # 重新排列列顺序
                        existing_df = existing_df[required_columns]

                        # 备份原文件
                        backup_file = config.RESALE_DAILY_CSV + '.bak'
                        import shutil
                        shutil.copy2(config.RESALE_DAILY_CSV, backup_file)
                        logger.info(f"已备份原文件到 {backup_file}")

                        # 保存更新后的文件
                        existing_df.to_csv(config.RESALE_DAILY_CSV, index=False, encoding=config.CSV_ENCODING)
                        logger.info(f"已更新 resale_daily.csv 的列结构：{len(existing_columns)} -> {len(required_columns)} 列")
                except Exception as e:
                    logger.warning(f"扩展 daily.csv 列数时出错：{e}")

        # 3.5.2. 检查并扩展 month_agency.csv 的列数（添加"发布套数"）
        if not df_agency.empty:
            required_columns = df_agency.columns.tolist()
            if os.path.exists(config.AGENCY_CSV):
                try:
                    existing_df = pd.read_csv(config.AGENCY_CSV, encoding=config.CSV_ENCODING)
                    existing_columns = existing_df.columns.tolist()

                    # 检查是否缺少"发布套数"列
                    if '发布套数' in required_columns and '发布套数' not in existing_columns:
                        logger.info(f"检测到 month_agency.csv 缺少'发布套数'列，正在添加...")

                        # 在"签约套数"列之前插入"发布套数"列，值为-1
                        new_existing_df = existing_df.copy()
                        for i, col in enumerate(existing_columns):
                            if col == '签约套数':
                                # 在此位置插入"发布套数"列
                                new_existing_df.insert(i, '发布套数', -1)
                                break

                        # 备份原文件
                        backup_file = config.AGENCY_CSV + '.bak'
                        import shutil
                        shutil.copy2(config.AGENCY_CSV, backup_file)
                        logger.info(f"已备份原文件到 {backup_file}")

                        # 保存更新后的文件
                        new_existing_df.to_csv(config.AGENCY_CSV, index=False, encoding=config.CSV_ENCODING)
                        logger.info(f"已更新 month_agency.csv，添加'发布套数'列")
                except Exception as e:
                    logger.warning(f"扩展 month_agency.csv 列数时出错：{e}")

        # 4. 保存到CSV
        logger.info("-" * 50)
        logger.info("开始保存数据到CSV文件...")

        # 保存经纪机构数据
        new_agency, skip_agency, df_new_agency = save_to_csv(
            df_agency,
            config.AGENCY_CSV,
            ['年月', '经纪机构'],
            logger
        )

        # 保存区县数据
        new_district, skip_district, df_new_district = save_to_csv(
            df_district,
            config.DISTRICT_CSV,
            ['年月', '区县'],
            logger
        )

        # 保存面积数据
        new_area, skip_area, df_new_area = save_to_csv(
            df_area,
            config.AREA_CSV,
            ['年月', '面积区间'],
            logger
        )

        # 保存每日数据
        new_daily, skip_daily, df_new_daily = save_to_csv(
            df_daily,
            config.RESALE_DAILY_CSV,
            ['日期'],
            logger
        )

        # 保存商品房每日数据
        new_commercial_daily, skip_commercial_daily, df_new_commercial_daily = save_to_csv(
            df_commercial_daily,
            config.NEW_DAILY_CSV,
            ['日期'],
            logger
        )

        # 保存月度汇总数据
        new_month, skip_month, df_new_month = save_to_csv(
            df_month,
            config.RESALE_MONTHLY_CSV,
            ['月份'],
            logger
        )

        # 保存价格数据
        new_price, skip_price, df_new_price = save_to_csv(
            df_price,
            config.PRICE_CSV,
            ['年月', '价格区间'],
            logger
        )

        # 保存五年新建商品房数据
        new_commercial, skip_commercial, df_new_commercial = save_to_csv(
            df_commercial,
            config.NEW_5YEAR_CSV,
            ['年份'],
            logger
        )

        # 保存五年存量房数据
        new_existing, skip_existing, df_new_existing = save_to_csv(
            df_existing,
            config.RESALE_5YEAR_CSV,
            ['年份'],
            logger
        )

        # 5. 输出统计信息（日志）
        logger.info("-" * 50)
        logger.info("数据抓取统计：")
        logger.info(f"  目标年月：{year_month}")
        logger.info(f"  经纪机构：抓取 {len(df_agency)} 条，新增 {new_agency} 条，已存在 {skip_agency} 条")
        logger.info(f"  区县数据：抓取 {len(df_district)} 条，新增 {new_district} 条，已存在 {skip_district} 条")
        logger.info(f"  面积数据：抓取 {len(df_area)} 条，新增 {new_area} 条，已存在 {skip_area} 条")
        logger.info(f"  价格数据：抓取 {len(df_price)} 条，新增 {new_price} 条，已存在 {skip_price} 条")
        logger.info(f"  每日数据：抓取 {len(df_daily)} 条，新增 {new_daily} 条，已存在 {skip_daily} 条")
        logger.info(f"  商品房每日：抓取 {len(df_commercial_daily)} 条，新增 {new_commercial_daily} 条，已存在 {skip_commercial_daily} 条")
        logger.info(f"  月度汇总：抓取 {len(df_month)} 条，新增 {new_month} 条，已存在 {skip_month} 条")
        logger.info(f"  五年新建商品房：抓取 {len(df_commercial)} 条，新增 {new_commercial} 条，已存在 {skip_commercial} 条")
        logger.info(f"  五年存量房：抓取 {len(df_existing)} 条，新增 {new_existing} 条，已存在 {skip_existing} 条")
        logger.info("=" * 50)
        logger.info("数据抓取完成！")
        logger.info("=" * 50)

        # 6. 展示新增数据
        display_results(
            year_month, df_new_daily, df_new_month,
            df_new_agency, df_new_district, df_new_area,
            df_new_price, df_new_commercial, df_new_existing,
            df_new_commercial_daily
        )

    except Exception as e:
        logger.error(f"程序执行失败：{e}")
        raise


if __name__ == "__main__":
    main()
