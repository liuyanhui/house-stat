"""数据存储和展示"""
import os
import shutil
import pandas as pd
import config


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


def display_results(year_month, new_df_daily, new_df_month, new_df_agency,
                    new_df_district, new_df_area, new_df_price=None,
                    new_df_commercial=None, new_df_existing=None,
                    new_df_commercial_daily=None):
    """
    在控制台展示需要更新（新增）到文件的数据
    """
    W = 70
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

    # 每日数据
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

    # 月度汇总
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

    # 经纪机构
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

    # 区县分布
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

    # 面积区间分布
    if not new_df_area.empty:
        print()
        print("-" * W)
        print(f"  [ 新增 - 面积区间分布 ]（{len(new_df_area)} 条）")
        print("-" * W)
        print(f"  {'面积区间':<16}{'成交套数':>12}{'成交面积(m2)':>16}")
        print(f"  {'-'*16}{'-'*12}{'-'*16}")
        for _, r in new_df_area.iterrows():
            print(f"  {r['面积区间']:<16}{int(r['成交套数']):>12}{r['成交面积']:>16.2f}")

    # 价格区间分布
    if new_df_price is not None and not new_df_price.empty:
        print()
        print("-" * W)
        print(f"  [ 新增 - 价格区间分布 ]（{len(new_df_price)} 条）")
        print("-" * W)
        print(f"  {'价格区间':<16}{'发布套数':>10}{'成交套数':>10}{'成交面积(m2)':>16}")
        print(f"  {'-'*16}{'-'*10}{'-'*10}{'-'*16}")
        for _, r in new_df_price.iterrows():
            print(f"  {r['价格区间']:<16}{int(r['发布套数']):>10}{int(r['成交套数']):>10}{r['成交面积']:>16.2f}")

    # 五年新建商品房统计
    if new_df_commercial is not None and not new_df_commercial.empty:
        print()
        print("-" * W)
        print(f"  [ 新增 - 五年新建商品房网签 ]（{len(new_df_commercial)} 条）")
        print("-" * W)
        print(f"  {'年份':<10}{'住宅套数(万)':>14}{'住宅面积(万m2)':>16}{'非住宅面积(万m2)':>18}")
        print(f"  {'-'*10}{'-'*14}{'-'*16}{'-'*18}")
        for _, r in new_df_commercial.iterrows():
            print(f"  {r['年份']:<10}{r['住宅套数万']:>14.2f}{r['住宅面积万m2']:>16.2f}{r['非住宅面积万m2']:>18.2f}")

    # 五年存量房统计
    if new_df_existing is not None and not new_df_existing.empty:
        print()
        print("-" * W)
        print(f"  [ 新增 - 五年存量房交易 ]（{len(new_df_existing)} 条）")
        print("-" * W)
        print(f"  {'年份':<10}{'住宅套数(万)':>14}{'住宅面积(万m2)':>16}{'非住宅面积(万m2)':>18}")
        print(f"  {'-'*10}{'-'*14}{'-'*16}{'-'*18}")
        for _, r in new_df_existing.iterrows():
            print(f"  {r['年份']:<10}{r['住宅套数万']:>14.2f}{r['住宅面积万m2']:>16.2f}{r['非住宅面积万m2']:>18.2f}")

    # 商品房每日数据
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


def extend_csv_columns(df, csv_file, logger):
    """
    扩展CSV文件的列数（向后兼容）
    如果现有文件缺少新列，则添加这些列
    """
    if df.empty:
        return

    if not os.path.exists(csv_file):
        return

    try:
        required_columns = df.columns.tolist()
        existing_df = pd.read_csv(csv_file, encoding=config.CSV_ENCODING)
        existing_columns = existing_df.columns.tolist()

        if set(required_columns) != set(existing_columns):
            logger.info(f"检测到 {os.path.basename(csv_file)} 列数不匹配，正在添加新列...")

            for col in required_columns:
                if col not in existing_columns:
                    existing_df[col] = -1

            existing_df = existing_df[required_columns]

            backup_file = csv_file + '.bak'
            shutil.copy2(csv_file, backup_file)
            logger.info(f"已备份原文件到 {backup_file}")

            existing_df.to_csv(csv_file, index=False, encoding=config.CSV_ENCODING)
            logger.info(f"已更新 {os.path.basename(csv_file)} 的列结构：{len(existing_columns)} -> {len(required_columns)} 列")
    except Exception as e:
        logger.warning(f"扩展 {os.path.basename(csv_file)} 列数时出错：{e}")


def extend_agency_csv(df, csv_file, logger):
    """
    扩展经纪机构CSV文件，添加"发布套数"列
    """
    if df.empty:
        return

    if not os.path.exists(csv_file):
        return

    try:
        required_columns = df.columns.tolist()
        existing_df = pd.read_csv(csv_file, encoding=config.CSV_ENCODING)
        existing_columns = existing_df.columns.tolist()

        if '发布套数' in required_columns and '发布套数' not in existing_columns:
            logger.info(f"检测到 {os.path.basename(csv_file)} 缺少'发布套数'列，正在添加...")

            new_existing_df = existing_df.copy()
            for i, col in enumerate(existing_columns):
                if col == '签约套数':
                    new_existing_df.insert(i, '发布套数', -1)
                    break

            backup_file = csv_file + '.bak'
            shutil.copy2(csv_file, backup_file)
            logger.info(f"已备份原文件到 {backup_file}")

            new_existing_df.to_csv(csv_file, index=False, encoding=config.CSV_ENCODING)
            logger.info(f"已更新 {os.path.basename(csv_file)}，添加'发布套数'列")
    except Exception as e:
        logger.warning(f"扩展 {os.path.basename(csv_file)} 列数时出错：{e}")
