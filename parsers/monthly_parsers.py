"""月度数据解析器"""
import re
import pandas as pd
from .base_parser import safe_int, safe_float


def parse_agency_data(soup, year_month, logger):
    """
    解析按经纪机构统计的数据
    表格ID: table_clf1
    """
    logger.info("开始解析经纪机构数据...")

    try:
        outer_table = soup.find('table', id='table_clf1')
        if not outer_table:
            table = soup.find('td', string=re.compile('房地产经纪机构名称'))
            if table:
                outer_table = table.find_parent('table')

        if not outer_table:
            logger.error("未找到经纪机构数据表格")
            return pd.DataFrame()

        inner_tables = outer_table.find_all('table', border='1')
        if len(inner_tables) == 0:
            all_tables = outer_table.find_all('table')
            inner_tables = [t for t in all_tables if len(t.find_all('tr')) > 2]

        data = []
        for inner_table in inner_tables:
            rows = inner_table.find_all('tr')
            for row in rows[1:]:
                cols = row.find_all('td')
                if len(cols) >= 5:
                    try:
                        seq_num = cols[0].text.strip()
                        agency_name = cols[1].text.strip()
                        list_count = cols[2].text.strip()
                        sign_count = cols[3].text.strip()
                        refund_count = cols[4].text.strip()

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
    """
    logger.info("开始解析区县数据...")

    try:
        outer_table = soup.find('table', id='table_clf2')
        if not outer_table:
            logger.error("未找到区县数据表格")
            return pd.DataFrame()

        inner_table = outer_table.find('table', bordercolor=lambda x: x and '#4a9ee0' in x)
        if not inner_table:
            inner_table = outer_table.find('table')
            if not inner_table:
                logger.error("未找到内层区县数据表格")
                return pd.DataFrame()

        rows = inner_table.find_all('tr')

        if len(rows) < 6:
            logger.error(f"区县表格行数不足，期望至少6行，实际{len(rows)}行")
            return pd.DataFrame()

        data = []

        # 第一部分
        district_row1 = rows[0].find_all('td')
        districts1 = [td.get_text(strip=True) for td in district_row1[1:]]

        count_row1 = rows[1].find_all('td')
        counts1 = [td.get_text(strip=True) for td in count_row1[1:]]

        area_row1 = rows[2].find_all('td')
        areas1 = [td.get_text(strip=True) for td in area_row1[1:]]

        # 第二部分
        district_row2 = rows[3].find_all('td')
        districts2 = [td.get_text(strip=True) for td in district_row2[1:]]

        count_row2 = rows[4].find_all('td')
        counts2 = [td.get_text(strip=True) for td in count_row2[1:]]

        area_row2 = rows[5].find_all('td')
        areas2 = [td.get_text(strip=True) for td in area_row2[1:]]

        all_districts = districts1 + districts2
        all_counts = counts1 + counts2
        all_areas = areas1 + areas2

        logger.info(f"区县数据：找到{len(all_districts)}个区县")

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
                    logger.warning(f"跳过异常数据：区县={district}, 错误={e}")
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
    """
    logger.info("开始解析面积数据...")

    try:
        outer_table = soup.find('table', id='table_clf3')
        if not outer_table:
            logger.error("未找到面积数据表格")
            return pd.DataFrame()

        inner_table = outer_table.find('table', bordercolor=lambda x: x and '#4a9ee0' in x)
        if not inner_table:
            inner_table = outer_table.find('table')
            if not inner_table:
                logger.error("未找到内层面积数据表格")
                return pd.DataFrame()

        rows = inner_table.find_all('tr')

        if len(rows) < 3:
            logger.error(f"面积表格行数不足，期望至少3行，实际{len(rows)}行")
            return pd.DataFrame()

        area_header_row = rows[0].find_all('td')
        area_ranges = [td.get_text(strip=True) for td in area_header_row[1:]]

        count_row = rows[1].find_all('td')
        counts = [td.get_text(strip=True) for td in count_row[1:]]

        area_row = rows[2].find_all('td')
        areas = [td.get_text(strip=True) for td in area_row[1:]]

        logger.info(f"面积数据：找到{len(area_ranges)}个区间")

        data = []
        for i, area_range in enumerate(area_ranges):
            if i < len(counts) and i < len(areas):
                try:
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
                    logger.warning(f"跳过异常数据：面积区间={area_range}, 错误={e}")
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
    """
    logger.info("开始解析价格数据...")

    try:
        outer_table = soup.find('table', id='table_clf4')
        if not outer_table:
            logger.error("未找到价格数据表格")
            return pd.DataFrame()

        inner_table = outer_table.find('table', bordercolor=lambda x: x and '#4a9ee0' in x)
        if not inner_table:
            inner_table = outer_table.find('table')
            if not inner_table:
                logger.error("未找到内层价格数据表格")
                return pd.DataFrame()

        rows = inner_table.find_all('tr')

        if len(rows) < 5:
            logger.error(f"价格表格行数不足，期望至少5行，实际{len(rows)}行")
            return pd.DataFrame()

        price_header_row = rows[0].find_all('td')
        price_ranges = [td.get_text(strip=True) for td in price_header_row[1:]]

        list_count_row = rows[1].find_all('td')
        list_counts = [td.get_text(strip=True) for td in list_count_row[1:]]

        list_area_row = rows[2].find_all('td')
        list_areas = [td.get_text(strip=True) for td in list_area_row[1:]]

        deal_count_row = rows[3].find_all('td')
        deal_counts = [td.get_text(strip=True) for td in deal_count_row[1:]]

        deal_area_row = rows[4].find_all('td')
        deal_areas = [td.get_text(strip=True) for td in deal_area_row[1:]]

        logger.info(f"价格数据：找到{len(price_ranges)}个区间")

        data = []
        for i, price_range in enumerate(price_ranges):
            if i < len(list_counts) and i < len(list_areas) and i < len(deal_counts) and i < len(deal_areas):
                try:
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


def parse_month_summary(soup, logger):
    """
    解析月度存量房网上签约汇总数据
    """
    logger.info("开始解析月度汇总数据...")

    try:
        month_pattern = re.compile(r'(\d{4})年(\d{1,2})月存量房网上签约')
        daily_pattern = re.compile(r'\d{4}/\d{1,2}/\d{1,2}存量房网上签约')

        best_table = None
        best_month = None
        max_sign_count = 0

        for table in soup.find_all('table'):
            table_text = table.get_text()
            match = month_pattern.search(table_text)

            if match and not daily_pattern.search(table_text):
                year = match.group(1)
                month = match.group(2).zfill(2)
                month_str = f"{year}-{month}"

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

                if temp_sign_count > max_sign_count:
                    max_sign_count = temp_sign_count
                    best_table = table
                    best_month = month_str

        if best_table and best_month:
            logger.info(f"找到月度汇总数据：{best_month}，签约套数：{max_sign_count}")

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

                    if '网上签约套数' in label and '住宅' not in label:
                        data['网上签约套数'] = int(value_text) if value_text.isdigit() else -1
                    elif '网上签约面积' in label and '住宅' not in label:
                        area_value = value_text.replace('m²', '').replace(' ', '').strip()
                        data['网上签约面积(m2)'] = float(area_value) if area_value else -1
                    elif '住宅签约套数' in label:
                        data['住宅签约套数'] = int(value_text) if value_text.isdigit() else -1
                    elif '住宅签约面积' in label:
                        area_value = value_text.replace('m²', '').replace(' ', '').strip()
                        data['住宅签约面积(m2)'] = float(area_value) if area_value else -1

            if data['网上签约套数'] > 0 or data['住宅签约套数'] > 0:
                df = pd.DataFrame([data])
                logger.info(f"成功解析月度汇总数据：月份={best_month}")
                return df
            else:
                logger.warning(f"未能从表格中提取到有效的月度汇总数据")
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
    """
    logger.info("开始解析近五年新建商品房数据...")

    try:
        table = soup.find('table', id='table_001')
        if not table:
            logger.error("未找到近五年新建商品房数据表格")
            return pd.DataFrame()

        from datetime import datetime
        update_date = datetime.now().strftime("%Y-%m-%d")
        data = []

        rows = table.find_all('tr')
        for row in rows[5:]:
            cells = row.find_all('td')
            if len(cells) >= 4:
                first_cell_text = cells[0].get_text(strip=True)
                year_match = re.search(r'(\d{4})年', first_cell_text)
                if year_match:
                    year = int(year_match.group(1))
                    if 2020 <= year <= 2024:
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
    """
    logger.info("开始解析近五年存量房数据...")

    try:
        table = soup.find('table', id='table_002')
        if not table:
            logger.error("未找到近五年存量房数据表格")
            return pd.DataFrame()

        from datetime import datetime
        update_date = datetime.now().strftime("%Y-%m-%d")
        data = []

        rows = table.find_all('tr')
        for row in rows[5:]:
            cells = row.find_all('td')
            if len(cells) >= 4:
                first_cell_text = cells[0].get_text(strip=True)
                year_match = re.search(r'(\d{4})年', first_cell_text)
                if year_match:
                    year = int(year_match.group(1))
                    if 2020 <= year <= 2024:
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
