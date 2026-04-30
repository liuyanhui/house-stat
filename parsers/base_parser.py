"""基础解析器 - 共享辅助函数"""
import re
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta


def safe_int(value, default=-1):
    """安全转换为整数，失败返回默认值"""
    try:
        clean_val = str(value).replace(',', '').strip()
        return int(clean_val) if clean_val else default
    except (ValueError, AttributeError):
        return default


def safe_float(value, default=-1):
    """安全转换为浮点数，失败返回默认值"""
    try:
        clean_val = str(value).replace(',', '').strip()
        return float(clean_val) if clean_val else default
    except (ValueError, AttributeError):
        return default


def extract_data_month(soup, logger):
    """
    从页面标题中提取数据年月
    例如：2025年12月存量房网上签约 -> (2025, 12)
    """
    try:
        title_pattern = re.compile(r'(\d{4})年(\d{1,2})月存量房网上签约')

        for text in soup.stripped_strings:
            match = title_pattern.search(text)
            if match:
                year = match.group(1)
                month = match.group(2).zfill(2)
                year_month = f"{year}-{month}"
                logger.info(f"提取到数据年月：{year_month}")
                return year_month

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
    today = datetime.now()
    last_month = today - relativedelta(months=1)
    return last_month.strftime("%Y-%m")
