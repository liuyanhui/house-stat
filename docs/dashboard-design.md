# 趋势报告升级 — 设计方案 v1.1（评审定稿版）

> v1.0 → v1.1：吸收五角色设计评审（架构/前端/QA/产品/数据分析师）全部意见，各角色均结论"无架构性阻塞"。定稿依据见 §0。

## 0. 评审定稿记录

| 角色 | 关键意见 | 落点 |
|---|---|---|
| 架构 | quality.py 须零依赖纯函数；KNOWN_ISSUES 并入；ESTIMATED_DAYS 迁移漏 ai_digest 消费方；kpi.json 归属错位；契约缺口（同 stem/双向校验/PNG 退化/plots 退役时点）；verify 独立重算+失败路径 | §2.2/§2.4/§2.8/§1 |
| 前端 | 注释直通规则；注入点在 _img_tag 处（两条路径）；NaN 序列化（None+allow_nan=False）；option 禁函数；调色板三处单源；体积门改总门；IO 回退/单 resize | §2.4/§2.6/§2.8 |
| QA | 同源比对无效→独立重算；先渲染后校验自相矛盾→暂存改名+先校验后开浏览器；三分支真实数据打不到→合成 fixture；修复清单须逐点白名单+基线快照；补三项断言与 file:// 清单 | §2.8/§5/§4 |
| 产品 | M1 边界自相矛盾→注入机制与内容同批，TOC 提进 M1；O7 是纪律违例应立即修（→M0）；生成时间入 DATASET_META；回退须零操作可发现 | §4/§2.2/§2.6 |
| 数据分析师 | 去化分子分母同口径+完整月操作定义+上线即"—"预期；退房率跨断档"—"+Σ/Σ合并比+上榜名单逐月变动；YTD 同坏值整窗规则+仅住宅口径；周估算传播 est_days 列 | §2.3/§3 |

## 1. 总体数据流（一条管线，入口不变）

```
main.py（不变）→ data/*.csv
→ script/analyze.py --report（唯一生成入口）
    load.py(+2) → metrics.py（防护+新指标）→ quality.py（质量元数据）
    metrics/report 层 → kpi.json（M2 引入）
    charts.py（spec 单一来源）→ matplotlib sink→PNG ＋ echarts sink→report/charts/*.json
    report.py（md，![](png) 引用不变）→ html_render.py（渲染期注入）
    → 产物先写暂存名 *.tmp → verify 门（从 CSV 独立重算）
        通过 → 改名定稿 + 打开浏览器
        失败 → 改名 *.bad 隔离、不开浏览器、非零退出（退出码穿透 run_report）
```

## 2. 模块设计

### 2.1 `analysis/load.py`（改）
- `load_new_daily()`：**列级清洗**（-1→NaN 按列；库存列 2026-04-29 起有效、签约/认购列 2026-08-23 起有效；禁行级过滤）；登记 2026-09-16 缺失。
- `load_agency()`：弃"发布套数"列（全 -1），保留 年月/机构/签约/退房。
- 质量常量迁至 `quality.py`。

### 2.2 `quality.py`（根目录，新）
- 实施修正：不放 analysis/ 内——`analysis/__init__` 连带导入 plots（matplotlib），utils 侧引用会拖入重依赖。
- **零项目内 import**（纯常量 + 纯函数）；coverage 从数据派生，勿手维护。
- `DATASET_META` **六字段**：`{coverage, estimated_days, missing_months, known_issues, notes, generated_at}`。
- 并入 `utils/validate` 的 `KNOWN_ISSUES`（单一来源不被打穿；注意 validate.py 本身不消费 ESTIMATED_DAYS）。
- `ESTIMATED_DAYS` 迁入，**真实消费方为 report.py 与 ai_digest.py**（v1.1 更正：v1.0 误写为 validate.py），两处同批切换（M1）。

### 2.3 `analysis/metrics.py`（改）
- 防护：`safe_pct_change`（断档→NaN）；同比仅连续≥13 月；MA 按长度降级；2025-01 总面积坏值→NaN；new_daily 2026-04 剔除。
- 新指标（含评审口径补丁）：
  - `destock_months`：分子=最新日（可售期房住宅+未签约现房住宅）套数；分母=**近 3 个完整月（期房签约住宅+现房签约住宅）均值**（分子分母同口径）；**完整月操作定义=该月签约字段全量且无缺日**（9 月缺 16 日不算）；不足 3 个完整月→"—"；**预期注明：上线即"—"，约 2026-12 起有值**；9-16 剔除致 9 月分母约低 3%，数据说明注明。
  - `refund_rate`：上榜口径；当月值+3 月滚动并列；**滚动=Σ退房/Σ签约（合并比，禁比率均值）；跨 2026-05/06 断档→"—"**；上榜名单逐月变动在数据说明注明。
  - `ttm`：面积含 2025-01 坏值的窗口**整窗 NaN**（禁 skipna 剔月）。
  - `ytd`：**同坏值整窗 NaN 规则；仅住宅口径（套数/住宅面积）出 YTD，总面积恒 NaN 明示**。
  - `nonresidential_share`、`presale_vs_ready`。
  - `weekly_aggregate` **新增 est_days 列**（该周含估算天数）；MA4 跨估算周同标（估算标记全层级传播，需求 §5.4）。

### 2.4 `analysis/charts.py`（新，spec 单一来源）
- `prep_xxx(df) → ChartSpec{series, axes, annotations, meta}`；sink×2：`to_matplotlib`（PNG）、`to_echarts`（完整 option）。
- **调色板单源**：`PALETTE` 常量供 html_render.THEME、matplotlib rcParams、ECharts base_option 三处共用，防样式漂移。
- **JSON 序列化规则**：NaN→None、`json.dumps(..., allow_nan=False)`、option 内禁函数（formatter 用字符串模板）。
- annotations：日级估算/缺月/断档断线/不完整周置灰统一在此层定义，双 sink 同渲染；周级"含估算 n 天"由 est_days 聚合。
- **契约**：charts/*.json 与 PNG 同 stem 成对；verify 双向存在校验；缺 json 自动退化 PNG。
- **plots.py 退役时点**：M1 迁移完成即并入 charts.py 的 matplotlib sink，plots.py 删除（防双实现并存）。

### 2.5 `analysis/report.py`（改）
- md 图表引用不变；统一格式化函数（千分位/单位/NaN→"—"/MTD 标注）。
- O7（删媒体表）移至 **M0 立即执行**（纪律违例不排队）。

### 2.6 `analysis/html_render.py`（改）
- **注释直通规则**：`_convert` 新增 HTML 注释块直通（`<!-- toc -->`/`<!-- kpi -->` 占位符不再被转义）。
- **图注入点在 `_img_tag` 生成处**按文件名匹配（base64 之前；独占行/行内两条路径都覆盖）；默认模式注入 div+JSON，NO_JS 模式才走 base64 PNG。
- bootstrap：UMD echarts 内联、经典 script、IntersectionObserver 懒 init（**IO 缺失回退立即全量 init**）、**单 resize 监听**遍历已 init 实例。
- TOC：h2/h3 计数器 id + `<!-- toc -->` 注入（约 20 行，**提进 M1**）。
- KPI 卡（M2）：`<!-- kpi -->` + kpi.json 渲染。
- 表格（M2）：data-v 排序键 + 原生 JS 事件委托，**NaN（"—"）排序置尾**；sticky 表头须把 overflow 从 `.table-wrap` 移入内层 max-height 容器。

### 2.7 `assets/vendor/echarts.custom.js`（新，入 git）
- 官方在线按需构建，**M3 全集一次构建**：bar/line/heatmap/tooltip/legend/dataZoom/toolbox/markLine/title/grid；版本+sha256 记文件头注释与 CLAUDE.md；构建期内联。

### 2.8 `script/analyze.py`（改）
- 顺序：**render → verify → open**（现"渲染完即开浏览器"须改）。
- **verify 断言集（全部从 CSV 独立重算，禁调 metrics 同一函数）**：TTM=近 12 月整数和；YTD 等窗重算（仅住宅）；去化周期独立公式重算（容差 0.1%）；退房率 Top3 逐值重算；非住宅占比∈[0,1]；Top10 份额和≤100%+ε；面积段占比和≈100%±5%；新指标口径注释存在性；**总 HTML 体积门 2MB**；vendor bundle 组件自检；charts/*.json↔PNG 双向存在。
- 失败路径：产物改名 `*.bad` 隔离、不开浏览器、非零退出穿透。

## 3. 口径决策表（v1.1 更新）

| # | 决策 | 取值 |
|---|---|---|
| 1 | new_daily 清洗 | 列级 -1→NaN，禁行级过滤 |
| 2 | charts/*.json | 完整 option；NaN→None+allow_nan=False；禁函数 |
| 3 | vendor | M3 全集一次构建，版本+sha256 锁定 |
| 4 | 去化周期 | 分子分母同口径（住宅）；完整月=字段全量且无缺日；不足 3 月→"—"；上线即"—"（约 2026-12 起有值） |
| 5 | 退房率 | 当月+3 月滚动并列；**Σ/Σ 合并比**；**跨断档→"—"**；上榜名单逐月变动注明 |
| 6 | TTM/YTD | 含 2025-01 坏值窗口整窗 NaN；YTD 仅住宅口径 |
| 7 | 排名变化 | 跨断档禁用（与 5 统一） |
| 8 | M1 验收 | **逐点白名单**（2025-12 TTM 行、跨 05/06 环比格等）+ git 内基线 md 快照 diff |
| 9 | verify | 独立重算派生期望值；失败隔离 *.bad+先校验后开浏览器 |
| 10 | KPI 断言 | kpi.json 直比 metrics（M2 起） |
| 11 | 粒度 | 月为主、周最细；新房认购/签约月度；日度仅聚合输入 |
| 12 | 估算传播 | 日级 est → 周 est_days → MA4 同标，全层级携带 |

## 4. 分期与回退

- **M0 纪律热修（立即）**：删报告 H1 媒体表 + ai_digest `_H1_ANCHORS` 及相关三处文本 + CLAUDE.md 数据覆盖状态同步。
- **M1 基建+迁移**：§2.1/2.2/2.3/2.4 + §2.6 的注入/bootstrap/退化开关/TOC + §2.7 + §2.8 + §5 测试。交付可见改进=图表交互+目录（不止"图能动"）。
- **M2 优化**：O1–O4 + kpi.json + 表格排序（O6 可选；R5 可提前至 M2 末）。
- **M3 丰富**：R1（含去化/现房期房）→ R2 → R4 → R3。
- **回退（零操作可发现）**：HTML 异常 → 直接读 `report/trend_report.md`（PNG 全在，md 永远生成）；`DASHBOARD_NO_JS=1` 为**生成侧**开关，写入 CLAUDE.md；档位 C（独立 dashboard）后备。

## 5. 测试策略（新）

- `tests/` + **合成 fixture CSV**：覆盖真实数据打不到的分支——TTM 坏值窗、去化"不足 3 完整月"、跨断档环比/退房滚动、估算周聚合传播。
- **基线回归**：`tests/baseline/` 入 git 存基线 md 快照；改版 diff 只允许白名单内差异（修复清单逐点）。
- **file:// 手工清单**（M1/M2 验收）：双击打开、首屏 <2s、hover/dataZoom/保存图片、中文渲染、（M2 起）表格排序。

## 6. 变更记录

- v1.0：初稿（需求 v0.3 + 需求评审 4 路意见）。
- v1.1：吸收设计评审五路全部意见；新增 M0、测试策略、口径补丁（去化/退房率/YTD/估算传播）、契约与失败路径、调色板单源、TOC 提前。
