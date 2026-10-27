# analysis/ — 趋势分析包

把 `data/` 下的官方自爬 CSV 转成趋势报告（Markdown + 交互 HTML + PNG + charts JSON）。不含预测，仅历史趋势。

## 文件说明

### `load.py`
加载各 CSV、解析日期、清洗 -1 占位、规范区县名（去全角空格）。
- `load_monthly(data_dir=None)` 全市月度；`load_daily()` 每日（周度聚合来源）；`load_district()`/`load_area()`/`load_price()` 各分类；`load_annual()` 五年年度。
- `load_new_daily()`（M1）：**列级** -1→NaN 清洗（库存列 4-29 起有效、认购/签约列 2026-08-23 起；禁行级过滤，会丢 4 个月库存历史）。
- `load_agency()`（M1）：弃"发布套数"列（全 -1）。
- 质量常量（ESTIMATED_DAYS 等）单一来源在根 `quality.py`。

### `metrics.py`
趋势指标，全部确定性、可复现。
- 口径防护（M1）：`add_yoy_mom`（断档环比 NaN；同比需连续段 ≥13 月）、`sanitize_bad_values`（2025-01 面积坏值→NaN，供整窗规则）、`weekly_aggregate`（`_week_bucket` 共用周口径；`est_days`/`MA4含估算` 估算标记随聚合传播）、`expand_estimated_days`。
- 累计与结构（M2）：`add_ttm`（12 月连续+值全有效才出值）、`add_ytd`（等窗，年内缺月/NaN 前缀污染整段无效）、`nonresidential_share`（'总>住宅'守卫）。
- 新房与机构（M3）：`new_home_weekly`（认购/签约=期房+现房住宅）、`new_inventory`（库存快照）、`district_mom`（区县月度环比，首月无值+断档防护）、`agency_refund`（当月+3月滚动均 Σ/Σ，断档整窗空）、`agency_top`（上榜内份额）、`annual_anchor`（官方年度+满 12 月年份月度和）。
- 其余：`add_moving_avg`、`avg_unit_area`、`district_share`/`district_rank_change`、`segment_share`、`coverage_counts`。

### `charts.py`（M1 起，替代已退役的 plots.py）
图表引擎：**ChartSpec 单一来源 → 双 sink**。spec 为纯 dict（kind/series/axes/annotations），拟合/均线/份额一律 prep 算好，sink 只画不算。
- kind 家族：`bars_lines`（双轴/正负着色/逐柱着色）、`lines`、`stacked_area`（100% 份额 prep 预计算，禁 percent 栈）、`heatmap`（连续色阶或 piecewise 七档分色——环比类用分档，极值饱和、格内数值为准）、`smallmul`。纯线轴 `_axis_min` 自适应下限（柱轴保持 0 基线）；图表数值全局圆整 ≤2 位小数（`_round_floats`）。
- `write_chart(spec)`：产 `<stem>.png` + `charts/<stem>.json`（同 stem 契约；JSON={name,kind,height,option}，**option 为 Python 算好的完整 ECharts 配置**，NaN→None、allow_nan=False、禁函数）。
- `prep_*` 19 个（与 `verify.CHART_STEMS` 一一对应，`test_chart_stems_registry` 强制同步）：city_trend / seasonality / avg_unit_area / weekly_trend / weekly_avg_area / district_stacked / district_share / district_smallmul / area_segment_count / area_segment_share / cum_view / structure_share / new_weekly / resale_vs_new / new_inventory / district_mom / annual_anchor / agency_top / refund_rate。
- 调色板单一来源：根 `palette.py`（PALETTE + DISTRICT_COLORS，matplotlib/ECharts 共用）。
- 迁移期像素比对工具：`tests/pixel_diff.py`（超容差→人工复核记录）。

### `report.py`
组装 `report/trend_report.md`：摘要 / 全市 / 周度 / 区域 / 市场结构 / 数据说明。图表经 `charts.prep_*` + `write_chart` 产出；`<!-- toc -->` 占位（html_render 渲染目录）。媒体 H1 参考表已移除（根 CLAUDE.md 约定 12）。**仅生成 md，不耦合 html**——HTML 由 html_render 从成品 md 转换。

### `html_render.py`
Markdown→自包含 HTML 转换器（零第三方依赖，stdlib only）。
- 基础：GFM 表格（带样式、横向滚动）、图片 base64 内嵌、微信胶囊主题（`THEME`）。
- **图表注入（M1）**：与 md 图片同 stem 的 `charts/<stem>.json` 存在且 vendor 可用 → `<img>` 换成 `<div class="chart">`，文末内联 vendor + 数据块（`</` 转义防提前闭合）+ bootstrap（IntersectionObserver 懒 init 回退全量、debounce resize）。HTML 注释（`<!-- toc -->`/`<!-- kpi -->`）直通。
- `<!-- toc -->` → 计数器 id 目录（避中文 slug）。
- **退化**：`DASHBOARD_NO_JS=1` 或 vendor 缺失 → 纯 PNG 模式（输出与旧版一致）。
- **KPI 卡**（M2）：`<!-- kpi -->` + kpi.json 构建期静态渲染（无 JS，两模式通用）；**表格排序**（M2）：`_cell_key` Python 推断 data-v + 原生 JS 事件委托（缺失置尾，纯文字表不启用）。

### `verify.py`（M1–M3）
生成后验收门（全部从 CSV **独立重算**，不经 load/metrics）：charts↔PNG 双向成对（CHART_STEMS 19 个，与 prep_* 注册表测试强制同步）、JSON 无 NaN 字面量、锚点（尾月套数==city_trend 柱末值==md 千分位）、kpi 直比（latest/MTD 日度当月重算）、TTM/YTD 独立重算、新房周签约重算、非住宅占比∈(0,100)、HTML 体积门 2MB、交互/PNG 标记、vendor 组件自检。analyze.py 渲染后、开浏览器前调用，失败隔离 `*.bad` 非零退出。

### `ai_digest.py`
AI 分析输入导出（客观 digest + 可粘贴 prompt）。H1 媒体锚点已移除（2026-10-26，根 CLAUDE.md 约定 12）。原则不变：代码拥有数字、AI 只拥有叙述（+预判）；不谈价格、不给买卖建议。

## 约定

- 数据均来自官方自爬，口径一致；不引用任何第三方/媒体数据（约定 12）。
- 面积段缺 2026-04（永久丢失）；2026-05 起每月正确累积。
- 报告产物 `report/` 是代码生成的衍生物，**已 gitignore**，每次 `python script/analyze.py --report` 重新生成。
- 根目录 `quality.py`（质量元数据单一来源，零依赖）与 `palette.py`（调色板）供本包与 utils/html_render 共用。
