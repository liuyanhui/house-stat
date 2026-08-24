# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.1.0] - 2026-08-24

### Added
- 原始 HTML 按日存档到 `data/raw/`（页面只显示最新一天/一月，错过无法回补；不进 git）
- 断流门 `check_monthly_feeds`：月度表解析为空且页面年月新于库内 → 非零退出，防页面改版后静默断流
- 滞后告警 `check_daily_freshness`：日度数据最新日期早于昨天时告警
- 校验新规则：住宅签约套数/面积 ≤ 总计；日度加总 vs 月度对账（仅日度覆盖完整的月份，阈值 0.5%）
- 已知历史异常白名单 `KNOWN_ISSUES`（当前 1 条：2025-01 总面积 < 住宅面积，首爬即如此无从修正）
- 面积表发布侧两列（发布套数/发布面积，2026-07 起有真实数据，更早 -1 占位）

### Fixed
- 经纪机构表解析断流：页面 2026-05 起删除"发布套数"列，旧位置索引+固定列数判断导致全部行被跳过，数据断流 3 个月（2026-05/06 永久丢失，2026-07 已回补）。改为按表头文本定位列（`_column_map`）
- 商品房日表大面积失效（43 列中 23 列恒 -1）：页面改版后标题独占一行导致 `'期房' in str(row)` 类行内条件永假，且 `find_all(text=True)` 命中 `<script>` 文本定位到容器表。改为按表格首行标题定位 + 纯 label 匹配（`_COMMERCIAL_TABLES` 映射驱动），认购/签约 4 表字段全部复活
- 图表图例：双轴图此前只显示左轴图例，全市图同比柱（绿涨/橙跌）与周度图面积线无说明，已补全

### Changed
- 解析器定位统一约定：一律按表格首行标题/表头文本，不做位置索引、不做行内关键字二次确认；价格表与日表日期提取同步改造；`_find_row_by_label` 增加精确匹配模式（表头"面积"是"发布面积"的子串）
- `resale_daily.csv` 清理 8 个死列（页面已删除"可售房源统计""新发布房源"栏目，恒 -1），重构为 5 列
- 共享助手收敛：`normalize_label`（base_parser）、`_read_csv`（validate）、`_fmt_ymd`/`_find_table_by_title`（daily_parsers）；删除不可达的 `extend_agency_csv` 特例迁移

## [3.0.0] - 2026-07-02

### Added
- 趋势分析包 `analysis/`（load/metrics/plots/report/html_render）：全市/周度/各区/市场结构趋势，8 张 matplotlib 图（中文字体）
- `script/analyze.py --report`：生成趋势报告（Markdown + HTML + PNG，写入 `report/`）
- `script/gen_html.py`：把 `trend_report.md` 转成自包含 HTML（图片 base64 内嵌）
- 数据完整性校验门 `utils/validate.py` + `script/validate.py`：面积段/价格段加总 vs 全市（阈值 5%），接入 main.py，不一致非零退出
- 节假日判定改用 `chinese_calendar` 法定日历（替代旧的"低于工作日均值 15%"循环逻辑）

### Fixed
- 面积段解析 bug：北京住建委 2026-04~05 把面积表从 3 行（成交）改版成 5 行（发布+成交），旧 `parse_area_data` 写死 `rows[1]` 误把"发布套数"当"成交套数"；改为按表头文本定位行（`_find_row_by_label`）

## [2.0.0] - 2026-04-30

### Added
- Price range monthly statistics (60万以下, 60～90万, 90～120万, etc.)
- New home (commercial property) daily statistics with 43 data fields:
  - 可售期房统计
  - 未签约现房统计
  - 现房项目情况
  - 预售许可
  - 期房网上认购
  - 期房网上签约
  - 现房网上认购
  - 现房网上签约
- Five-year historical data for new homes (新建商品房网签情况)
- Five-year historical data for resale homes (存量房交易情况)
- Automatic CSV column expansion with `.bak` backup files
- Default value handling: failed scrapes return `-1`

### Changed
- **Breaking**: Renamed all CSV files to use `new_*` and `resale_*` prefixes
  - `daily.csv` → `resale_daily.csv`
  - `month.csv` → `resale_monthly.csv`
  - `commercial_daily.csv` → `new_daily.csv`
  - `five_year_commercial.csv` → `new_5year.csv`
  - `five_year_existing.csv` → `resale_5year.csv`
  - `month_agency.csv` → `agency_monthly.csv`
  - `month_district.csv` → `district_monthly.csv`
  - `month_area.csv` → `area_monthly.csv`
  - `month_price.csv` → `price_monthly.csv`
- Enhanced agency data to include "发布套数" (listing count) column

### Fixed
- Duplicate 2024 records in five-year data parsing
- Agency data only showing 5 records instead of 10 (nested table issue)

## [1.0.0] - 2026-01-06

### Added
- Initial release
- Monthly agency statistics (经纪机构月度统计)
- Monthly district statistics (区县月度统计)
- Monthly area statistics (面积区间月度统计)
- Daily resale home signing data (每日存量房网上签约)
- Monthly summary data (月度汇总数据)
- Automatic deduplication based on date/year-month
- Logging system for tracking operations
- Automatic retry on network failures (3 attempts)
- UTF-8-BOM encoding for Excel compatibility

---

## File Naming Convention (v2.0.0+)

| Prefix | Description |
|--------|-------------|
| `resale_*` | Resale homes (存量房/二手房) |
| `new_*` | New homes (新建商品房/新房) |
| `*_daily` | Daily frequency data |
| `*_monthly` | Monthly frequency data |
| `*_5year` | Five-year historical data |
