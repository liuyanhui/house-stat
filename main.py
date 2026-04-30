"""
房地产签约数据抓取主程序
从北京市住建委网站抓取存量房网上签约月统计数据
"""

from bs4 import BeautifulSoup
import pandas as pd
import config
from utils import (
    ensure_directories,
    setup_logging,
    fetch_html,
    save_to_csv,
    display_results,
    extend_csv_columns,
    extend_agency_csv
)
from parsers import (
    extract_data_month,
    get_previous_month,
    parse_agency_data,
    parse_district_data,
    parse_area_data,
    parse_price_data,
    parse_daily_data,
    parse_commercial_data,
    parse_month_summary,
    parse_five_year_commercial,
    parse_five_year_existing
)


def main():
    """主函数"""
    # 0. 确保目录存在
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
        if not df_daily.empty:
            extend_csv_columns(df_daily, config.RESALE_DAILY_CSV, logger)

        if not df_agency.empty:
            extend_agency_csv(df_agency, config.AGENCY_CSV, logger)

        # 4. 保存到CSV
        logger.info("-" * 50)
        logger.info("开始保存数据到CSV文件...")

        new_agency, skip_agency, df_new_agency = save_to_csv(
            df_agency, config.AGENCY_CSV, ['年月', '经纪机构'], logger
        )

        new_district, skip_district, df_new_district = save_to_csv(
            df_district, config.DISTRICT_CSV, ['年月', '区县'], logger
        )

        new_area, skip_area, df_new_area = save_to_csv(
            df_area, config.AREA_CSV, ['年月', '面积区间'], logger
        )

        new_daily, skip_daily, df_new_daily = save_to_csv(
            df_daily, config.RESALE_DAILY_CSV, ['日期'], logger
        )

        new_commercial_daily, skip_commercial_daily, df_new_commercial_daily = save_to_csv(
            df_commercial_daily, config.NEW_DAILY_CSV, ['日期'], logger
        )

        new_month, skip_month, df_new_month = save_to_csv(
            df_month, config.RESALE_MONTHLY_CSV, ['月份'], logger
        )

        new_price, skip_price, df_new_price = save_to_csv(
            df_price, config.PRICE_CSV, ['年月', '价格区间'], logger
        )

        new_commercial, skip_commercial, df_new_commercial = save_to_csv(
            df_commercial, config.NEW_5YEAR_CSV, ['年份'], logger
        )

        new_existing, skip_existing, df_new_existing = save_to_csv(
            df_existing, config.RESALE_5YEAR_CSV, ['年份'], logger
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
