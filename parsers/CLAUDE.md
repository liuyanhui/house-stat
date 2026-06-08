# parsers/ — 数据解析模块

从 HTML 页面中提取结构化数据的解析器集合。所有解析函数接收 BeautifulSoup 对象和 logger，返回 pandas DataFrame。

## 文件说明

### `__init__.py`

模块导出文件，统一暴露所有解析函数，供 `main.py` 通过 `from parsers import ...` 调用。

导出的函数：
- `safe_int`, `safe_float`, `extract_data_month`, `get_previous_month`（来自 base_parser）
- `parse_agency_data`, `parse_district_data`, `parse_area_data`, `parse_price_data`, `parse_month_summary`, `parse_five_year_commercial`, `parse_five_year_existing`（来自 monthly_parsers）
- `parse_daily_data`, `parse_commercial_data`（来自 daily_parsers）

### `base_parser.py`

基础辅助函数，被其他解析器共享使用。

| 函数 | 说明 |
|------|------|
| `safe_int(value, default=-1)` | 安全转换为整数，失败时返回 -1 |
| `safe_float(value, default=-1)` | 安全转换为浮点数，失败时返回 -1 |
| `extract_data_month(soup, logger)` | 从页面标题中提取数据年月（如 "2025年12月存量房网上签约" → "2025-12"） |
| `get_previous_month()` | 获取当前日期的上一个月（当页面提取失败时的回退方案） |

### `monthly_parsers.py`

月度数据解析器，解析按月统计的各类表格。

| 函数 | 目标表格 | 输出文件 |
|------|----------|----------|
| `parse_agency_data(soup, year_month, logger)` | `table_clf1` — 经纪机构排行 | `agency_monthly.csv` |
| `parse_district_data(soup, year_month, logger)` | `table_clf2` — 区县签约分布 | `district_monthly.csv` |
| `parse_area_data(soup, year_month, logger)` | `table_clf3` — 面积区间分布 | `area_monthly.csv` |
| `parse_price_data(soup, year_month, logger)` | `table_clf4` — 价格区间分布 | `price_monthly.csv` |
| `parse_month_summary(soup, logger)` | 月度存量房网上签约汇总表 | `resale_monthly.csv` |
| `parse_five_year_commercial(soup, logger)` | `table_001` — 近五年新建商品房 | `new_5year.csv` |
| `parse_five_year_existing(soup, logger)` | `table_002` — 近五年存量房 | `resale_5year.csv` |

### `daily_parsers.py`

每日数据解析器，解析按日统计的表格。

| 函数 | 解析内容 | 输出文件 |
|------|----------|----------|
| `parse_daily_data(soup, logger)` | 存量房每日签约 + 可售房源 + 新发布房源（13 列） | `resale_daily.csv` |
| `parse_commercial_data(soup, logger)` | 商品房每日统计：可售期房、未签约现房、现房项目、预售许可、期房/现房认购与签约（43 列） | `new_daily.csv` |
