# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
