# 周度叙述开发计划 v1.1（依据需求 v0.3 + 设计 v1.2 + 三角色计划评审修订）

> v1.0 → v1.1：产品（W2 真实样句预览前移、两级拦截）+ 架构（W1 拆 a/b、W2 依赖松绑至 schema、W4 拆 a/b 交叉验证提前、tag 时点后移、白名单机制化、工期 2–2.5 天）+ QA（遗留 json 验证归属、删"扩展位"后门、通读记录格式钉死、W3 命令化）。
> 通用验收（每任务）：`PYTHONPATH= py -3.14 tests/run_tests.py` 全绿；`py -3.14 script/validate.py` 通过；不破坏 `report.bat`。测试框架沿用零依赖 `tests/run_tests.py`（不引 pytest）。每任务单独 commit，测试不全绿不提交。
> 工期估计：2–2.5 天（W4a 可与 W2 并行，压回约 2 天）。tag `wn1`（wn=weekly-narrative，避让任务号 W1–W5），**W 评审通过后打**。

## W 周度叙述（单里程碑｜完成后你能看到：报告第三节图与表之间多一段 3~4 句的趋势白话叙述，取代原"最新周"注）

| ID | 任务 | 文件 | 内容 | 验收 | 依赖 |
|---|---|---|---|---|---|
| W1a | 事实函数·周侧 | analysis/metrics.py、tests/run_tests.py | `weekly_trend_facts` 周侧：节假日周原语（chinese_calendar 工作日净额 ≤3、回退告警同 classify_day_type 模式）+ 方向链（N=链长−1、节假日/持平/缺口中断、估算不中断、null 三成因、last_pair_reversal、post_holiday）+ 峰谷（13 周窗、tie-break 非节假日优先、est/holiday 标记） | test_weekly_facts_direction 过（真实节假日日期 2025-09-29 周；est 经 weekly_aggregate(estimated_dates=…) 注入、序列避开 2026-07）；既有 32 用例仍绿 | — |
| W1b | 事实函数·末周侧 | analysis/metrics.py、tests/run_tests.py | `weekly_trend_facts` 末周侧：水位（基准=剔近 4 周的 ≤26 周、±5% 死区、<13 null）+ 末周三态（complete/in_progress/lagged）+ 同期对齐（P 位置/contiguous/prev_same_ok、ip_* 源自 daily 末周子集、ip_daily_avg 恒 int）+ narrative/字段最小化 | test_weekly_facts_level / last_week / degrade 过；`weekly_trend_facts` 整函数契约与设计 §3 一致 | W1a |
| W2 | 渲染模块 | analysis/weekly_narrative.py（新）、tests/run_tests.py | `render(facts) -> str` 纯函数：S1 五变体；S2 时间线故事（节日名口语映射、（含估算）、est×节假日叠加顺序）；S3 链值首尾 + 锚句（avg4 基准、三重排除、高/低位回退）+ 日期码唯一锚定末周值；S4 三变体；S5 段末注；水位短语方向中立 + 退化档换头；预算 ≤6 裁剪序；`<!-- wk-narrative -->` 包裹 | test_weekly_narrative_render 过（五变体/叠加/负例/日期码唯一/满配预算+裁剪/界定符成对/退化档）；模块无 DataFrame 反查。**可见交付：用真实 80 周数据过 W1 facts 渲染样段，贴给 Yiyue 预览措辞（结论前置/口语锚点/预算体感），通过后进 W3** | 设计 §3 schema 冻结（合成 facts 即可开发，可与 W1b 交错） |
| W4a | 独立重算+交叉验证 | analysis/verify.py、tests/run_tests.py | `_wk_facts_from_csv(data_dir, estimated_dates=None)`：csv 直读（跳过 ≤0 占位行、自建周一分桶、chinese_calendar 净额、est 缺省 import quality） | test_verify_weekly_gate 的交叉验证部分：同一合成 CSV 双实现逐字段相等（float 未 round 对 round1、abs>0.1） | W1b |
| W3 | report 集成 | analysis/report.py、analysis/CLAUDE.md、tests/baseline/whitelist.txt | 三节插入叙述段（图注后、近 10 周表前）；**删 wk_note 分支**；wk_facts.json 同次落盘（原生 cast、NaN→None、allow_nan=False、narrative=false 字段最小化）；daily 空 → 跳过且删遗留 wk_facts.json；whitelist 登记：**界定符精确行 + S1–S5 句首 stem 正则约 10 条**（baseline_diff.py 逐行正则无上下文，弃宽通配） | `PYTHONPATH= py -3.14 tests/baseline_diff.py tests/baseline/trend_report.pre.md report/trend_report.md tests/baseline/whitelist.txt` 通过（白名单内）；report.bat 重生成功；md 含界定符对；json 可解析且无 NaN 字面量；CLAUDE.md 登记 | W2 |
| W4b | verify 门接线 | analysis/verify.py、tests/run_tests.py | `WK_BANNED_WORDS` 常量；第 10 项检查组五条：存在性 fail-closed / 降级三方一致性（**含"daily 空而 json 存在→issue"**）+界定符 count==2 / 逐字段直比 / 条件化段内锚点 / 措辞门 | test_verify_weekly_gate 全过（交叉验证 + 篡改/不一致/缺 json/daily 空遗留 json/禁用词/预算超限各注错一次均触发 issue）；既有 test_verify_gate* fixture 补 wk_facts.json 后仍全绿 | W3、W4a |
| W5 | 收尾实测 | docs/HANDOFF.md、根 CLAUDE.md（如涉） | 全量回归；真实 80 周数据报告重生 + 人工通读（第二道拦截）；**HANDOFF 固定小节留档：叙述段原文 + 四项逐条勾选（结论前置/预算/禁用词/口语锚点）**，commit 只留指针 | run_tests 全绿（**32 → 38**：W1×4 + W2×1 + W4×1）；通读小节齐 | W4b |
| W 评审 | 3 路：QA/架构/数据分析师 | — | **入口条件：verify 全绿 + baseline diff 白名单内 + 全套测试 38 绿 + HANDOFF 通读小节齐**；重点：双实现交叉验证覆盖度、真实叙述段措辞、降级路径实测 | 评审纪要留档（采纳/驳回逐条），**通过后打 tag `wn1`** | W5 |

## 风险与预案

- **真实数据措辞意外组合**：两级拦截——W2 真实样句预览（第一道）+ W5 全量通读（第二道），返本限 W2 模板内；**回 W2 补句式变体须同任务补 render 用例与 whitelist stem 正则，并重跑 W5 通读**（无"扩展位"免检通道）。
- **baseline 白名单误伤**：逐句式登记（界定符行 + S1–S5 句首 stem 正则），W3 验收逐条核对新增项，无宽通配。
- **chinese_calendar 未覆盖未来日期**（跨年生成）：回退纯 weekday + 一次告警（W1a 按 classify_day_type 模式实现），不阻塞。

## 与主链的边界

不碰 main.py 抓取侧；不动 charts/CHART_STEMS（无新图）；html_render 零改动（注释直通既有测试覆盖）；ai_digest 增强（需求 P2）**不在本里程碑**，留 HANDOFF 清单。
