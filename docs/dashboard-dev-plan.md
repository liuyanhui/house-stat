# 开发计划 v1.1（依据设计 v1.1 + 三角色计划评审修订）

> **进度戳（2026-10-27）**：M0 ✅（tag m0）｜M1 ✅（tag m1）｜M2 ✅（tag m2）｜**M3 ✅（tag m3，三路评审过，全部里程碑完成）**。剩余为 M4 非阻塞清理清单，见 `docs/HANDOFF.md`。

> v1.0 → v1.1：QA（测试机制前移、基线先行、评审入口条件化、逐类注错）+ 产品（工期与可见交付、拆大任务、T2.6 决策点）+ 架构（T1.5/T1.6 依赖修正、T1.1 文件清单更正、T1.4 三拆、渲染层测试）。
> 通用验收（每任务）：`python script/validate.py` 通过；不破坏 `report.bat`。测试框架用零依赖 `tests/run_tests.py`（不引 pytest）。

## M0 纪律热修（约 0.5 天｜完成后你能看到：报告和 AI 导出不再引用媒体数据，其余不动）

| ID | 任务 | 文件 | 内容 | 验收 | 依赖 |
|---|---|---|---|---|---|
| T0.0 | 基线先行 | tests/baseline/ | 把当前 `trend_report.md` 复制存档为 `tests/baseline/trend_report.pre.md` 并入 git；附白名单 diff 脚本（后续 diff 只允许白名单内差异） | 存档提交；diff 脚本空跑通过 | — |
| T0.1 | 删报告媒体表 | analysis/report.py | 删"参考：上半年网签量长周期（媒体引用）"节 | 重生成报告；diff 基线仅该节消失 | T0.0 |
| T0.2 | ai_digest 锚点清除 | analysis/ai_digest.py | 删 `_H1_ANCHORS`、"数据范围"末行、"历史坐标"节、PROMPT_TEMPLATE 第五节 | gen_ai_digest 跑通；输出无"上半年锚点/媒体" | T0.1 |
| T0.3 | 文档同步 | 根 CLAUDE.md、analysis/CLAUDE.md | 数据覆盖状态更新（实测至 2026-10-24）；新增硬规则"只用自爬数据"；媒体表移除记录 | 文档与代码一致 | T0.2 |
| M0 评审 | 1 路 QA 抽查 diff | — | 入口：validate 过+diff 恰为白名单 | 通过标准：无白名单外差异 | T0.3 |

## M1 基建+迁移（约 3–4 天｜完成后你能看到：图能悬停看数/缩放/图例开关，报告带目录，其余内容不变）

| ID | 任务 | 文件 | 内容 | 验收 | 依赖 |
|---|---|---|---|---|---|
| T1.a | 测试骨架前移 | tests/、tests/fixtures/ | `run_tests.py` 骨架+合成 CSV fixture 生成器（断档环比/去化不足月/退房跨断档/估算周聚合四分支） | fixture 空跑通过；四分支样本可生成 | T0.3 |
| T1.1 | quality.py | quality.py（根目录）（新）、load.py、**report.py、ai_digest.py**（ESTIMATED_DAYS 真实消费方）、utils/validate.py（仅 KNOWN_ISSUES 迁入） | 零 import；DATASET_META 六字段；常量并入；消费方切换 | validate 过；grep 无旧常量定义 | T1.a |
| T1.2 | 新 loader ×2 | analysis/load.py | load_new_daily（列级清洗/8-23 边界/9-16 登记）、load_agency | fixture 单测过（库存 4-29 起、签约 8-23 起 NaN） | T1.1 |
| T1.3 | metrics 防护 | analysis/metrics.py | safe_pct_change/同比门控/MA 降级/坏值 NaN/weekly est_days/MA4 传播 | fixture 单测过（四分支） | T1.2 |
| T1.4a | charts 基座 | analysis/charts.py（新） | ChartSpec/PALETTE 单源/双 sink/序列化规则（NaN→None、禁函数）/同 stem 契约 | 单测：spec→PNG 与 spec→json 均可产、json 无 NaN 字面量 | T1.3 |
| T1.4b | 十图迁移（分批） | analysis/charts.py、report.py | 先迁 2–3 图试点评审，再迁其余 7–8 图 | **像素 diff 操作化**：PIL 尺寸相等+逐像素均值差<容差（工具脚本入库），超差图人工复核并记录 | T1.4a |
| T1.4c | plots.py 退役 | analysis/plots.py（删） | 删除并在 charts.py 收尾 | grep 无 plots 引用；全部图正常产出 | T1.4b |
| T1.5 | vendor（提前并行） | assets/vendor/ | 按需构建（M3 全集）+sha256 记录 | bundle 组件自检过 | 可与 T1.1 起并行 |
| T1.6a | 注入核心 | analysis/html_render.py | 注释直通+_img_tag 处两路径注入+bootstrap（IO 回退/单 resize）+vendor 内联+**THEME 切换到 PALETTE** | 默认模式图交互；**渲染层单测**（直通/两路径） | T1.4a、T1.5 |
| T1.6b | NO_JS+TOC | analysis/html_render.py | NO_JS 开关（base64 PNG 路径）+TOC（计数器 id） | NO_JS 输出与旧版 diff 白名单内；TOC 可跳转 | T1.6a |
| T1.7 | verify 门 | script/analyze.py | render→verify→open；独立重算断言集；失败 *.bad+退出码穿透 | **逐类断言各注错一次**均触发非零退出与隔离 | T1.4c、T1.6a |
| T1.8 | 测试补全 | tests/ | 渲染层测试（注释直通/两路径/NO_JS 回退）并入 run_tests；白名单机制完善 | 全套测试绿 | T1.7 |
| T1.9 | M1 实测 | — | file:// 手工清单（双击/首屏<2s/hover/缩放/保存图片/中文） | 清单逐项记录 | T1.8 |
| 中期抽查 | T1.4b 试点后 | — | 2–3 图试点即请 1 路前端抽查 spec/json/像素 diff 报告（视觉回归最高危段） | 抽查通过再迁其余 | T1.4b(试点) |
| M1 评审 | 多角色 | — | **入口条件：T1.9 记录齐+verify 全绿+像素 diff 全过+全套测试绿**；角色：架构/前端/QA/产品；通过标准：无阻塞项 | 评审纪要留档 | T1.9 |

## M2 抛光（约 2 天｜完成后你能看到：最新月数字变 KPI 卡、坑标注直接画在图上、表格能点表头排序、报告头标数据截止时间）

| ID | 任务 | 内容 | 验收 | 依赖 |
|---|---|---|---|---|
| T2.0 | M2 入口决策 | 定 T2.6 去留（默认做） | 决策记录 | M1 评审 |
| T2.1 | O1 进行中周期 | 不完整周置灰+天数+日均；MTD 卡位+“非全月”；YTD 等窗 | 视觉规则落地；verify 加断言 | M1 |
| T2.2 | O3 质量标注上图 | annotations 渲染（估算日/缺月/断线/含估算周） | 每处标注可追溯到 quality.json | M1 |
| T2.3 | O4 KPI 卡 | kpi.json（metrics 层产出）+`<!-- kpi -->`+统一格式化 | verify 直比 | M1 |
| T2.4 | O2 新鲜度头部 | DATASET_META 渲染报告头 | 与 quality.json 一致 | T2.3 |
| T2.5 | O6 表格排序 | data-v+事件委托+NaN 置尾+sticky 容器 | 手工清单加排序项 | M1 |
| T2.6 | （默认做）R5 提前 | TTM/YTD/非住宅占比+图表 | verify 独立重算断言 | T1.3 |
| M2 评审 | 多角色 | 入口：verify 全绿+白名单 diff 内+手工清单过 | 同 M1 标准 | 全部 |

## M3 加菜（约 2 天｜完成后你能看到：多出新房（含去化周期）、二手 vs 新房对比、五年图、机构退房率四个板块）

| ID | 任务 | 内容 | 验收 | 依赖 |
|---|---|---|---|---|
| T3.1 | R1 新房 section | 认购/签约×期房/现房（月粒度）、去化周期（“上线即—”预期说明）、现房/期房结构 | 口径注记齐全；verify 重算断言 | M2 |
| T3.2 | R2 二手 vs 新房 | 两线对比+序列长度悬殊断线标注 | 标注可见 | T3.1 |
| T3.3 | R4 五年图示 | 自爬年度+2025 月度和；五年均值线；口径切换缝标注 | 万套↔套单位验证 | M2 |
| T3.4 | R3 机构 | 上榜内份额、退房率（当月+Σ/Σ 滚动、断档"—"）、排名（断档禁用） | 三处断档处理正确 | M2 |
| M3 评审 | 多角色+总结 | 入口同 M2；产出复盘（估算 vs 实耗、返工点） | 同 M1 标准 | 全部 |

## 里程碑与提交

每任务单独 commit；M0–M3 各打 tag。全程不碰 main.py 抓取侧。工期合计约 7.5–8.5 天（含评审缓冲）。
