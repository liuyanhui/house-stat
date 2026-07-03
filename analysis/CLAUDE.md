# analysis/ — 趋势分析包

把 `data/` 下的官方自爬 CSV 转成趋势报告（Markdown + HTML + PNG）。不含预测，仅历史趋势。

## 文件说明

### `load.py`
加载各 CSV、解析日期、清洗 -1 占位、规范区县名（去全角空格）。
- `load_monthly()` 全市月度；`load_daily()` 每日（周度聚合来源）；`load_district()`/`load_area()`/`load_price()` 各分类；`load_annual()` 五年年度。

### `metrics.py`
趋势指标，全部确定性、可复现。
- `add_yoy_mom` 同比/环比；`add_moving_avg` N月移动均值；`avg_unit_area` 套均面积；`weekly_aggregate` 按自然周聚合（套数/面积/套均/天数 + 4周均线）；`district_share`/`district_rank_change` 区域份额与排名变化；`segment_share` 分段占比。

### `plots.py`
10 张图（matplotlib，`Microsoft YaHei` 中文字体，Agg 后端）。
- 全市：`city_trend`（柱+12月均线+同比）、`seasonality`（年×月热力）、`avg_unit_area_trend`。
- 周度：`weekly_trend`（柱+4周均线+面积双轴）、`weekly_avg_area`（套均面积连续趋势）。
- 区域：`district_stacked`/`district_share`（Top 8 + 其他）、`district_small_multiples`。
- 结构：`area_segment_count`/`area_segment_share`（面积段成交量/占比热力）。

**关键约定**：宽表 index 必须先用 `_str_index` 转字符串再 `plot.area`/`plot.bar`，否则 pandas 用 Period 序数当 x 坐标，与 `set_xticks(range(n))` 错位。

### `report.py`
组装 `report/trend_report.md`：摘要 / 全市 / 周度 / 区域 / 市场结构 / 数据说明（含媒体参考锚点）。用 `_table()` 生成 GFM 表格。**仅生成 md，不耦合 html**——HTML 由 `html_render` 从成品 md 转换。

### `html_render.py`
Markdown→自包含 HTML 转换器（零依赖，移植自 refined-stock `gen-html.mjs`）。
- `md_to_html(md_text, image_dir)`：转换；`render_file(md_path, out_html_path)`：读 md 写 html。
- 扩展了 GFM 表格（带样式、横向滚动）与图片 base64 内嵌（单文件自包含）。
- 微信胶囊主题集中在 `THEME` 常量（accent `#009874`、中文字体栈），用 `<style>` 块。

### `ai_digest.py`
AI 分析输入导出（客观 digest + 可粘贴 prompt）。
- `build_prompt()` / `render_file(out_path)`：从 load/metrics 算客观事实（全市月度同比/环比/MA12、套均面积、周度、区域份额/排名、面积段占比、年度+H1 锚点），**排除价格**，拼成单文件 `report/ai_digest.md`，整篇粘贴给 LLM。
- 原则：代码拥有数字、AI 只拥有叙述（+预判）。硬规则：事实只引 digest 数字、薄数据 hedge、区分季节性；**预判可外推但按资深分析师纪律**（区分事实/判断、方向+幅度区间+置信度+证伪条件、短 horizon 1–3 月、情景化）；不谈价格、不给买卖建议。客观+预判双段，手动喂、不进流程。

## 约定

- 数据均来自官方自爬，口径一致。第三方历史未纳入主序列（见 `HANDOFF.md`）。
- 面积段缺 2026-04（永久丢失，解析器时间窗口 bug）；2026-05 起每月正确累积。
- 报告产物 `report/` 是代码生成的衍生物，**已 gitignore**，每次 `python script/analyze.py --report` 重新生成。
