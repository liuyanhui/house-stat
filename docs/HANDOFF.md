# 交接说明（HANDOFF）— 交互式趋势报告升级

> 更新：2026-10-30 ｜ 作者会话：Claude Code（GLM-5.3）＋ Yiyue ｜ 本文件是换机/冷启动的唯一入口。

## 一句话现状

升级项目 **M0–M3 全部完成并打 tag（m0/m1/m2/m3）**：报告为交互式单文件 HTML（ECharts 内嵌、双击即开、可退化纯图），含 19 张图、KPI 卡、可排序表格、质量标注、独立重算验收门。后续只剩 M4 清理清单（见下）。

**新任务进行中：周度成交趋势文字分析**（报告第三节新增面向普通读者的趋势叙述段）——需求 v0.3 已定稿（两轮四角色评审，19 条意见全采纳），**下一步：设计文档**。入口：`docs/weekly-narrative-requirements.md`。

## 文档索引

| 文档 | 内容 |
|---|---|
| `docs/dashboard-requirements.md` | 需求 v0.3 定稿（两轮五角色评审后） |
| `docs/dashboard-design.md` | 设计 v1.1 定稿（五角色评审后，含实施修正记录） |
| `docs/dashboard-dev-plan.md` | 开发计划 v1.1（三角色评审后，进度戳见文首） |
| `docs/weekly-narrative-requirements.md` | 周度叙述需求 v0.3 定稿（两轮四角色评审后，**进行中任务**） |
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

## 进度明细（截至 2026-10-30）

- **周度叙述（进行中）— 需求 ✅（2026-10-30，两轮四角色评审：产品/数据分析师/QA + 架构）**：报告第三节新增 3~4 句普通读者口径的趋势叙述（方向=连涨连跌/水位=vs 半年周均/最新=进行中周同期对齐日均）。评审战果：抓出"进行中周=days<7"定义错误（历史 33 个不满周是节假日/缺口）、日均对比构成不对等（实测工作日≈周末 3 倍，改同期对齐）、节假日周机械制造连涨连跌（改工作日净额≤3 天判定+中断计数）、verify 锚点被近 10 周表天然命中（改段界定符+wk_facts.json 直比）、双实现漂移（加交叉验证用例）。**下一步：设计文档**（事实函数接口、wk_facts.json schema、模板状态机、verify 独立重算结构），完成后照例多角色评审。

- **M0 纪律热修 ✅（tag m0）**：删媒体引用表与 ai_digest H1 锚点（根 CLAUDE.md 约定 12：只用自爬数据）。
- **M1 基建+迁移 ✅（tag m1，架构/QA/前端三路评审通过）**：`quality.py`/`palette.py`（根，零依赖）、`analysis/charts.py`（ChartSpec 双 sink，plots.py 已退役）、html_render 注入（懒 init/NO_JS 退化/TOC）、`analysis/verify.py`（独立重算验收门）、analyze.py 渲染→校验→开浏览器、tests/ 全套。
- **M2 ✅（tag m2，2026-10-27；QA/前端/架构三路评审通过）**：T2.7 注入健壮性｜T2.1 进行中周禁总量环比改日均｜T2.3 KPI 卡+kpi.json｜T2.2 质量标注上图（估算周红框、缺失月"缺"列、quality.json）｜T2.4 新鲜度头部｜T2.5 表格排序（data-v+置尾+吸顶）｜T2.6 R5 三指标（TTM 整窗 180,962 / YTD 等窗 +5.1% / 非住宅占比）+两新图+验收门独立重算。评审战果：架构抓到 AREA_SEGMENTS 误删回归（已修+守卫）；m1 tag 重指 rebase 后提交。
- **M3 ✅（tag m3，2026-10-27；QA/架构/数据分析师三路评审通过）**：R1/R2 新房板块（周度认购签约、二手对照、库存 6.48→5.63 万套）+ 区县月度环比热力 + R4 五年锚点（官方年度+2025 月度和、口径缝明示）+ R3 机构板块（上榜内份额 Top10、退房率当月+Σ/Σ 滚动、断档整窗空）。评审战果：数据分析师互相关实测**证伪**"认购领先签约一周"（实为零滞后同周联动），图注已撤改；另历 /simplify（8 项清理）与 /code-review（7 项，含干净房间生成、空转断言）两轮质量关。
- **用户验收 UI 三轮（M3 后）**：①KPI 卡/进行中周日均口径/表格去滚动条等 5 项；②图表数值全局 ≤2 位小数；③纯线轴自适应下限（柱轴保 0 基线）、区县环比热力七档分色（±5/±20/±50%，极值饱和）、红框柱（估算周）图注置首+表内"估N天"标记。
- **M4 清理清单（非阻塞，按需做）**：
  1. `_first_positive`（返 None）与 `_first_non_null`（返 0）契约不一，统一之
  2. `_with_missing_months` 内联复刻 `_period_labels`，改调用
  3. `quality.MISSING_MONTHS['new_daily']` 存的是日期非月份（语义错位）
  4. 新房字段全量起点 2026-08-23 仅在 NOTES 文本，宜机器可读
  5. report 面积段表未复用 `_area_pivots`；`AREA_SEGMENTS` 住 charts 被 report 反向引用（启用价格段前宜迁 quality.py）
  6. 成功重跑不自动清旧 `*.bad` 残件
  7. 前端备忘：NO_JS 下 sortable 类残留、`_cell_key` 无 isfinite 守卫、表头 textContent 重写抹行内标记、ECharts 热力缺失列无"缺"字
  8. **去化周期**：满 3 个完整签约月后启用（预计 2027-01 前后；规则已定，届时加 metric+prep，verify 注册表测试会强制同步）
  9. 8-17 首周仅含 8-23 一天数据，呈现轻微失真（分析师提示，可考虑丢弃不足 3 天的首周）

## 关键决策速查（详见设计文档 §3）

只用已爬官方数据（禁第三方/媒体）｜粒度月为主、周最细、新房用月度｜JS 只渲染不算数（派生指标全 Python 预计算）｜断档禁插值、禁假环比（同比需连续 13 月）｜不完整周期与完整周期禁直比｜估算标记全层级传播（est_days/MA4含估算）｜verify 门从 CSV 独立重算、失败隔离 `*.bad`｜`DASHBOARD_NO_JS=1` 或 vendor 缺失自动退化纯图。

## 已知遗留（M2 前端评审记录在案）

- verify 对 charts JSON 的 option 结构仅抽查 city_trend（全量键校验待加）。
- analyze `*.bad` 隔离路径与 NO_JS→analyze 全链为手工验证（QA 已实测），自动化测试待补。
