# 交接说明（HANDOFF）— 交互式趋势报告升级

> 更新：2026-10-26 ｜ 作者会话：Claude Code（GLM-5.3）＋ Yiyue ｜ 本文件是换机/冷启动的唯一入口。

## 一句话现状

报告管线已升级为**交互式**（ECharts 内嵌单文件 HTML，双击即开、无服务器、可退化纯图）。**M0/M1/M2 完成并打 tag（m0/m1/m2，m1 已重指 rebase 后提交）**，下一步从 M3 的 R1（新房板块）继续。

## 文档索引

| 文档 | 内容 |
|---|---|
| `docs/dashboard-requirements.md` | 需求 v0.3 定稿（两轮五角色评审后） |
| `docs/dashboard-design.md` | 设计 v1.1 定稿（五角色评审后，含实施修正记录） |
| `docs/dashboard-dev-plan.md` | 开发计划 v1.1（三角色评审后，进度戳见文首） |
| 根/各目录 `CLAUDE.md` | 模块级说明（随代码同步更新） |

## 冷启动（新机器）

1. `git clone` 本仓库；安装 **Python 3.14**（`py -3.14`）＋ `pip install -r requirements.txt`。干净机器无旧机器的 PYTHONPATH 污染问题，直接 `py -3.14` 即可（污染问题见根 CLAUDE.md 顶部说明）。
2. 一键出报告：`report.bat`（或 `PYTHONPATH= py -3.14 script/analyze.py --report`）→ 生成 `report/trend_report.html` 并自动打开。**交互版开箱即用**（echarts vendor 已入 git）；`DASHBOARD_NO_JS=1` 环境变量可退回纯图模式。
3. 测试：`py -3.14 tests/run_tests.py`（当前 **23 用例，全绿为准**）。
4. 数据校验：`py -3.14 script/validate.py`。
5. 抓取新数据：`run.bat`（原功能未动）。
6. `report/`、`log/` 不入 git（衍生物，随时重生）；`tests/baseline/` 与 `assets/vendor/` **在 git 里，勿删**。

## 工作流约定（用户定，务必遵守）

- **每个关键步骤（需求/设计/计划/里程碑）完成 → 并行多角色评审**（产品/数据分析师/前端/数据管道/QA 等智能体角色），评审通过才进入下一步；评审意见逐条给采纳/驳回结论。
- 智能体**分批放（3+2）**，5 个同时放会触发 API 限流（429）；限额重置后再补。
- 聊天先讲大白话结论，编号/术语放文档里。
- 测试不全绿不提交。

## 进度明细（截至 2026-10-27）

- **M0 纪律热修 ✅（tag m0）**：删媒体引用表与 ai_digest H1 锚点（根 CLAUDE.md 约定 12：只用自爬数据）。
- **M1 基建+迁移 ✅（tag m1，架构/QA/前端三路评审通过）**：`quality.py`/`palette.py`（根，零依赖）、`analysis/charts.py`（ChartSpec 双 sink，plots.py 已退役）、html_render 注入（懒 init/NO_JS 退化/TOC）、`analysis/verify.py`（独立重算验收门）、analyze.py 渲染→校验→开浏览器、tests/ 全套。
- **M2 ✅（tag m2，2026-10-27；QA/前端/架构三路评审通过）**：T2.7 注入健壮性｜T2.1 进行中周禁总量环比改日均｜T2.3 KPI 卡+kpi.json｜T2.2 质量标注上图（估算周红框、缺失月"缺"列、quality.json）｜T2.4 新鲜度头部｜T2.5 表格排序（data-v+置尾+吸顶）｜T2.6 R5 三指标（TTM 整窗 180,962 / YTD 等窗 +5.1% / 非住宅占比）+两新图+验收门独立重算。评审战果：架构抓到 AREA_SEGMENTS 误删回归（已修+守卫）；m1 tag 重指 rebase 后提交。
- **M3 待做（下一步从 R1 开始）**：R1 新房（认购/签约×期房/现房、去化周期——注意"上线即—、约 2026-12 起有值"的预期说明、现房/期房结构；load_new_daily 已备）→ R2 二手 vs 新房 → R4 五年图（自爬年度+2025 月度和、均值线、口径缝标注）→ R3 机构（上榜内份额、退房率当月+Σ/Σ滚动、跨断档"—"、排名断档禁用；load_agency 已备）→ M3 评审+复盘。
- **非阻塞备忘（前端评审，M3 顺手清）**：NO_JS 下 sortable 类残留、_cell_key 无 isfinite 守卫（'nan' 字面量理论洞）、表头 textContent 重写抹行内标记、ECharts 热力缺失列无"缺"字（仅留空白）。

## 关键决策速查（详见设计文档 §3）

只用已爬官方数据（禁第三方/媒体）｜粒度月为主、周最细、新房用月度｜JS 只渲染不算数（派生指标全 Python 预计算）｜断档禁插值、禁假环比（同比需连续 13 月）｜不完整周期与完整周期禁直比｜估算标记全层级传播（est_days/MA4含估算）｜verify 门从 CSV 独立重算、失败隔离 `*.bad`｜`DASHBOARD_NO_JS=1` 或 vendor 缺失自动退化纯图。

## 已知遗留（M2 前端评审记录在案）

- verify 对 charts JSON 的 option 结构仅抽查 city_trend（全量键校验待加）。
- analyze `*.bad` 隔离路径与 NO_JS→analyze 全链为手工验证（QA 已实测），自动化测试待补。
