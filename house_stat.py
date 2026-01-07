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
    """
    logger.info("开始解析经纪机构数据...")

    try:
        # 查找经纪机构表格
        table = soup.find('table', id='table_clf1')
        if not table:
            # 尝试查找包含"房地产经纪机构名称"的表格
            table = soup.find('td', string=re.compile('房地产经纪机构名称'))
            if table:
                table = table.find_parent('table')

        if not table:
            logger.error("未找到经纪机构数据表格")
            return pd.DataFrame()

        data = []
        rows = table.find_all('tr')

        for row in rows[1:]:  # 跳过表头
            cols = row.find_all('td')
            if len(cols) >= 4:
                try:
                    seq_num = cols[0].text.strip()
                    agency_name = cols[1].text.strip()
                    sign_count = cols[2].text.strip()
                    refund_count = cols[3].text.strip()

                    # 跳过空行或非数据行
                    if not seq_num or not seq_num.isdigit():
                        continue

                    data.append({
                        '年月': year_month,
                        '序号': int(seq_num),
                        '经纪机构': agency_name,
                        '签约套数': int(sign_count) if sign_count.isdigit() else 0,
                        '退房套数': int(refund_count) if refund_count.isdigit() else 0
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

                    count_val = float(count_str) if count_str else 0
                    area_val = float(area_str) if area_str else 0

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

                    count_val = float(count_str) if count_str else 0
                    area_val = float(area_str) if area_str else 0

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


def parse_daily_data(soup, logger):
    """
    解析每日存量房网上签约数据
    查找页面中最新日期的每日签约统计
    """
    logger.info("开始解析每日数据...")

    try:
        # 查找包含日期的表格，格式如：2026/1/5存量房网上签约
        date_pattern = re.compile(r'(\d{4})/(\d{1,2})/(\d{1,2})存量房网上签约')

        # 在页面中搜索所有匹配的日期
        daily_tables = []
        for table in soup.find_all('table'):
            table_text = table.get_text()
            match = date_pattern.search(table_text)
            if match:
                year = match.group(1)
                month = match.group(2).zfill(2)
                day = match.group(3).zfill(2)
                date_str = f"{year}-{month}-{day}"
                daily_tables.append((date_str, table))

        if not daily_tables:
            logger.warning("未找到每日数据表格")
            return pd.DataFrame()

        # 获取最新的日期数据（通常第一个就是最新的）
        latest_date, table = daily_tables[0]
        logger.info(f"解析每日数据，日期：{latest_date}")

        # 解析表格数据
        data = {}
        rows = table.find_all('tr')

        for row in rows:
            cols = row.find_all('td')
            for i in range(len(cols) - 1):
                label = cols[i].get_text(strip=True)
                value = cols[i + 1].get_text(strip=True)

                # 提取各项数据
                if '网上签约套数' in label and '住宅' not in label:
                    data['签约套数'] = int(value) if value.isdigit() else 0
                elif '网上签约面积' in label and '住宅' not in label:
                    # 移除单位并转换
                    area_value = value.replace('m²', '').replace(' ', '').strip()
                    data['签约面积'] = float(area_value) if area_value else 0
                elif '住宅签约套数' in label:
                    data['住宅签约套数'] = int(value) if value.isdigit() else 0
                elif '住宅签约面积' in label:
                    # 移除单位并转换
                    area_value = value.replace('m²', '').replace(' ', '').strip()
                    data['住宅签约面积'] = float(area_value) if area_value else 0

        if data:
            data['日期'] = latest_date
            df = pd.DataFrame([data])
            # 调整列顺序
            df = df[['日期', '签约套数', '签约面积', '住宅签约套数', '住宅签约面积']]
            logger.info(f"成功解析每日数据 1 条")
            return df
        else:
            logger.warning("未能从表格中提取到有效数据")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"解析每日数据失败：{e}")
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
                                temp_sign_count = int(value_text) if value_text.isdigit() else 0
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
                        data['网上签约套数'] = int(value_text) if value_text.isdigit() else 0
                    elif '网上签约面积' in label and '住宅' not in label:
                        # 移除可能的单位和空格
                        area_value = value_text.replace('m²', '').replace(' ', '').strip()
                        data['网上签约面积(m2)'] = float(area_value) if area_value else 0.0
                    elif '住宅签约套数' in label:
                        data['住宅签约套数'] = int(value_text) if value_text.isdigit() else 0
                    elif '住宅签约面积' in label:
                        area_value = value_text.replace('m²', '').replace(' ', '').strip()
                        data['住宅签约面积(m2)'] = float(area_value) if area_value else 0.0

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
        (新增条数, 跳过条数)
    """
    if df.empty:
        logger.warning(f"数据为空，跳过保存：{csv_file}")
        return 0, 0

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

            return len(new_data), duplicates

        else:
            # 创建新文件
            df.to_csv(csv_file, index=False, encoding=config.CSV_ENCODING)
            logger.info(f"创建新文件 {csv_file}，写入 {len(df)} 条数据")
            return len(df), 0

    except Exception as e:
        logger.error(f"保存数据到 {csv_file} 失败：{e}")
        return 0, 0


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

        # 3. 解析三类数据
        logger.info("-" * 50)
        df_agency = parse_agency_data(soup, year_month, logger)
        df_district = parse_district_data(soup, year_month, logger)
        df_area = parse_area_data(soup, year_month, logger)
        df_daily = parse_daily_data(soup, logger)
        df_month = parse_month_summary(soup, logger)

        # 4. 保存到CSV
        logger.info("-" * 50)
        logger.info("开始保存数据到CSV文件...")

        # 保存经纪机构数据
        new_agency, skip_agency = save_to_csv(
            df_agency,
            config.AGENCY_CSV,
            ['年月', '经纪机构'],
            logger
        )

        # 保存区县数据
        new_district, skip_district = save_to_csv(
            df_district,
            config.DISTRICT_CSV,
            ['年月', '区县'],
            logger
        )

        # 保存面积数据
        new_area, skip_area = save_to_csv(
            df_area,
            config.AREA_CSV,
            ['年月', '面积区间'],
            logger
        )

        # 保存每日数据
        new_daily, skip_daily = save_to_csv(
            df_daily,
            config.DAILY_CSV,
            ['日期'],
            logger
        )

        # 保存月度汇总数据
        new_month, skip_month = save_to_csv(
            df_month,
            config.MONTH_CSV,
            ['月份'],
            logger
        )

        # 5. 输出统计信息
        logger.info("-" * 50)
        logger.info("数据抓取统计：")
        logger.info(f"  目标年月：{year_month}")
        logger.info(f"  经纪机构：抓取 {len(df_agency)} 条，新增 {new_agency} 条，已存在 {skip_agency} 条")
        logger.info(f"  区县数据：抓取 {len(df_district)} 条，新增 {new_district} 条，已存在 {skip_district} 条")
        logger.info(f"  面积数据：抓取 {len(df_area)} 条，新增 {new_area} 条，已存在 {skip_area} 条")
        logger.info(f"  每日数据：抓取 {len(df_daily)} 条，新增 {new_daily} 条，已存在 {skip_daily} 条")
        logger.info(f"  月度汇总：抓取 {len(df_month)} 条，新增 {new_month} 条，已存在 {skip_month} 条")
        logger.info("=" * 50)
        logger.info("数据抓取完成！")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"程序执行失败：{e}")
        raise


if __name__ == "__main__":
    main()
