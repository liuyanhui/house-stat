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
| `parse_agency_data(soup, year_month, logger)` | `table_clf1` — 经纪机构排行（按表头文本定位列 `_column_map`，页面删"发布套数"列后存 -1） | `agency_monthly.csv` |
| `parse_district_data(soup, year_month, logger)` | `table_clf2` — 区县签约分布 | `district_monthly.csv` |
| `parse_area_data(soup, year_month, logger)` | `table_clf3` — 面积区间分布（按表头文本定位行，勿改回位置索引；含发布侧两列，早期 -1 占位） | `area_monthly.csv` |
| `parse_price_data(soup, year_month, logger)` | `table_clf4` — 价格区间分布 | `price_monthly.csv` |
| `parse_month_summary(soup, logger)` | 月度存量房网上签约汇总表 | `resale_monthly.csv` |
| `parse_five_year_commercial(soup, logger)` | `table_001` — 近五年新建商品房 | `new_5year.csv` |
| `parse_five_year_existing(soup, logger)` | `table_002` — 近五年存量房 | `resale_5year.csv` |

### `daily_parsers.py`

每日数据解析器，解析按日统计的表格。

| 函数 | 解析内容 | 输出文件 |
|------|----------|----------|
| `parse_daily_data(soup, logger)` | 存量房每日签约（5 列；日期从"存量房网上签约"表标题提取，历史上的可售/新发布房源 8 列已随页面下线清理） | `resale_daily.csv` |
| `parse_commercial_data(soup, logger)` | 商品房每日统计 8 张表（43 列）：按表格首行标题（含日期关键字）定位，行内纯按清洗后 label 匹配，独立"面积"行归属上一计数行（`_COMMERCIAL_TABLES` 映射表驱动） | `new_daily.csv` |

## 重要约定

1. **解析器定位一律按表头/表标题文本**，不做位置索引、不做行内关键字二次确认（`'期房' in str(row)` 类条件在标题独占一行后永假；位置索引在页面增删列后错位）。历史教训见根 CLAUDE.md 约定 9。
2. **`soup.find_all(text=True)` 会命中 `<script>` 内文本**（如 `popup_statistic_everyday_onclick` 含表格标题字样），定位表格必须用表格自身首行标题，不能靠文本节点找 parent。
3. **`parse_area_data` 按表头文本定位行**（`_find_row_by_label`），不依赖行位置。历史上页面把面积表从 3 行（成交）改版为 5 行（发布+成交）后，旧的位置索引逻辑把"发布套数"误存为"成交套数"。改动此处后务必跑 `python script/validate.py` 校验加总=全市。

