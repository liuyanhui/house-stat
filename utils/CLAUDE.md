# utils/ — 工具模块

提供数据抓取、存储、目录管理和日志配置等基础功能。

## 文件说明

### `__init__.py`

模块导出文件，统一暴露所有工具函数，供 `main.py` 通过 `from utils import ...` 调用。

导出的函数：
- `ensure_directories`（来自 directory）
- `setup_logging`（来自 logging_setup）
- `fetch_html`（来自 fetcher）
- `save_to_csv`, `display_results`, `extend_csv_columns`, `extend_agency_csv`（来自 storage）
- `validate_integrity`, `check_monthly_feeds`, `check_daily_freshness`, `check_known_issues`（来自 validate）

### `fetcher.py`

网页抓取模块。

| 函数 | 说明 |
|------|------|
| `fetch_html(logger)` | 抓取北京市住建委网页 HTML 内容。失败时自动重试（最多 3 次，间隔 5 秒）。使用 config.py 中配置的 User-Agent 和超时时间。 |
| `_archive_html(html, logger)` | 抓取成功后把原始 HTML 存到 `data/raw/YYYY-MM-DD.html`（同日覆盖）。页面只显示最新一天/一月，无存档则解析器故障后无法回补。存档失败仅告警不阻断。 |

### `storage.py`

数据存储与展示模块。

| 函数 | 说明 |
|------|------|
| `save_to_csv(df, csv_file, key_column, logger)` | 将 DataFrame 保存到 CSV 文件。通过主键列自动去重，已存在数据跳过，仅追加新数据。返回 `(新增条数, 跳过条数, 新增数据DataFrame)`。 |
| `display_results(...)` | 在控制台展示本次新增的所有数据（每日签约、月度汇总、经纪机构排行、区县分布、面积区间、价格区间、五年历史等）。 |
| `extend_csv_columns(df, csv_file, logger)` | 扩展 CSV 列数以实现向后兼容。当程序新增了数据列而旧文件缺少时，自动补 `-1` 并生成 `.bak` 备份。 |
| `extend_agency_csv(df, csv_file, logger)` | 扩展经纪机构 CSV 文件，为旧文件补充"发布套数"列。 |

### `directory.py`

目录管理模块。

| 函数 | 说明 |
|------|------|
| `ensure_directories()` | 确保 `data/` 和 `log/` 目录存在，不存在时自动创建。 |

### `validate.py`

数据完整性校验，是防止静默脏数据的防线。

| 函数 | 说明 |
|------|------|
| `validate_integrity(data_dir=None, logger=None)` | 三类规则：① `area_monthly`/`price_monthly` 各段成交加总 ≈ `district_monthly` 全市（阈值 5%）；② `resale_monthly` 住宅 ≤ 总计；③ 日度加总 vs 月度对账（仅日度完整覆盖月份，阈值 0.5%）。`KNOWN_ISSUES` 白名单内的存量异常不算失败。返回 `(ok, issues)`。`main.py` 抓取后调用，不一致 `sys.exit(1)`。 |
| `check_monthly_feeds(year_month, feeds, logger=None)` | 断流门：月度表本次解析为空且页面年月新于库内最新 → 返回失败（`feeds: {数据名: (df, csv路径)}`）。`main.py` 解析后调用，失败非零退出。 |
| `check_daily_freshness(data_dir=None, logger=None, today=None)` | 滞后告警：`resale_daily`/`new_daily` 最新日期早于昨天 → 返回告警列表，仅告警不阻断。`today` 可注入便于测试。 |
| `check_known_issues(data_dir=None)` | 返回当前数据中仍存在的白名单历史异常（如 2025-01 总面积<住宅面积），供校验脚本提示。 |

### `logging_setup.py`

日志配置模块。

| 函数 | 说明 |
|------|------|
| `setup_logging()` | 初始化日志系统，同时输出到文件（`log/house_stat.log`）和控制台。日志格式：`[时间] 级别 - 消息`。返回 logger 实例。 |
