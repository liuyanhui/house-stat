# 图表数据叙述设计 v1.0（依据需求 v0.2）

> 2026-11-03 起草。分层与纪律全文沿用周度叙述已验证模式（`weekly-narrative-design.md` v1.6）；
> 本文档只写差异与新增，冲突处以周度设计为准。待三角色评审。

## §1 架构与模块归属

```
metrics.chart_facts(stem, …DataFrame)  →  纯 dict（A 类引用值 + B 类派生，无文案）
chart_narrative.render(stem, facts)    →  纯函数（只读 facts，文案全在此层）
report.py 三处插入（图注后）+ cn_facts.json 同次落盘
verify 检查 11（图级 CSV 聚合锚点）+ 检查 12（叙述组：直比/锚点/措辞/预算/界定符/干净房间）
```

- **facts 归 metrics.py**（与 weekly_trend_facts 并列，职责=纯指标）；**render 归新模块 `analysis/chart_narrative.py`**；`CN_BANNED_WORDS` 封闭表在 verify.py（与 WK_BANNED_WORDS 并列，单一来源 tests 从此导入）。
- **CN_STEMS 注册表**（verify.py 常量）：`('district_mom', 'area_segment_share', 'refund_rate')`；须为 `CHART_STEMS` 子集；tests 注册表断言三方同步（CN_STEMS ↔ 界定符 ↔ 白名单段）。
- **数据流关键决策**：metrics 从 **DataFrame**（load 后的区县/面积段/机构表）算 facts；verify 从**图 JSON** 重算 B 类直比 facts——两侧不同源（DataFrame vs 图 JSON），对质才有锚定意义；图 JSON 对不对由检查 11（vs CSV）兜底。

## §2 每图 facts schema 与判定规则

字段类型约定同周度（int / float(round1) / str / bool / None）；`narrative=false` 时除 `narrative, last_month` 外全 None（字段最小化，防直比歧义）。

### district_mom（区县环比热力）

| 字段 | 类型 | 规则 |
|---|---|---|
| narrative | bool | 末列非全 NaN 且 ≥2 个月 |
| last_month | str 'YY-MM' | x 轴末月 |
| n_districts | int | 恒 17（城六区+近远郊，全市行已滤） |
| leader_name / leader_pct / leader_units | str / float(round1) / int | 参评门槛=**上月 ≥500 套** 且 pct>0，取 pct 最大；无合格候选 → name=None |
| laggard_name / laggard_pct / laggard_units | 同上 | 同门槛且 pct<0，取 pct 最小；无 → None |
| n_up / n_down | int | 末列全部 17 区计数（NaN 不计） |
| broad_up / broad_down | bool | ≥15/17 同向才 true（措辞门槛） |

门槛数据源：`district_monthly` 前一月套数（leader_units 取**当月**套数，入句防小基数误读）。

### area_segment_share（面积段份额）

| 字段 | 类型 | 规则 |
|---|---|---|
| narrative | bool | ≥2 个可得月（跳过 2026-04 断档后） |
| last_month / prev_month | str | prev=**上一可得月**（4 月自动跳过，不内插） |
| top_seg / top_share / prev_top_share | str / float(round1)×2 | 末月最大份额段 |
| top_delta_pp | float(round1) | 末月−上一可得月，**从原始值算、渲染才圆整**（防 1dp 差分误差） |
| top_changed | bool | 份额易主（top_seg ≠ prev 的最大段） |
| max_abs_delta_pp | float(round1) | 全部 6 段 \|Δ\| 最大值（"均在 ±X pp 内"摆事实句用） |

变化句触发：`top_changed or abs(top_delta_pp) >= 0.5`，否则出摆事实句（`均在 ±{max_abs_delta_pp 圆整上限}pp 内`）。

### refund_rate（机构退房率）

| 字段 | 类型 | 规则 |
|---|---|---|
| narrative | bool | 末月率非空 |
| last_month | str | 机构表末月（**可能落后全市月**，句内必带月份码） |
| cur_rate / prev_rate / delta_pp | float(round1)×3 | series[0] 末值与前值；prev 断档（None）→ delta=None 跳环比句 |
| roll_last / roll_prev | float(round1) / None | series[1] 末两个**有效**滚动点 |
| roll_months_adjacent | bool | 末两有效点月份相邻才 true（否则滚动句整句跳过——当前数据即此态） |

## §3 cn_facts.json 与界定符

- 单文件 `report/cn_facts.json`：`{stem: {字段…}}`，仅含存在的图；NaN→None、`allow_nan=False`、原生 cast（同 wk_facts.json 约定）。
- 界定符：`<!-- cn-<stem> -->` 起止对（HTML 注释直通，NO_JS/交互两模式零渲染差异）。
- **干净房间**（fail-closed，双向）：任一 CN_STEM 图存在而 json 缺 → issue；json 存在而对应图全缺 → issue；`narrative=false` ↔ 该 stem 界定符 0 个，三方一致。

## §4 verify：检查 11 + 检查 12

**检查 11（图级 CSV 聚合锚点——补"3/4 图零重算"地基）**，从 `data/*.csv` 直算 vs 图 JSON：

| stem | 直算内容 | 比对 |
|---|---|---|
| district_mom | 末月各区 pct=(末−前)/前×100，从 district_monthly.csv | 与 JSON 末列 cells 逐格相等（1dp 容差 0.05）；n_up/n_down 重算相等 |
| area_segment_share | 末月各段 share=段/全市×100，从 area_monthly.csv | 末列 cells 逐格相等（1dp 容差 0.05）；各段加总 ≈100（±0.5） |
| refund_rate | 末柱 Σ退房/Σ签约×100，从 agency_monthly.csv | series[0] 末值相等（0.05） |

**检查 12（叙述组，仿第 10 项）**，对每个 CN_STEM：

1. B 类直比：从图 JSON 重算（家数=末列 cells 计数；top_seg/Δpp=share cells 派生；delta_pp=refund series 差分）vs cn_facts.json 逐字段（float round1 对 raw、容差 0.1；**同型直比**——type(a) is type(b)，继承 wn 评审 F3 加固）。
2. A 类锚点串：段内必含——有 leader 时 `f"{leader_name}"` 与 `f"{leader_pct:+.1f}%"`（去掉符号渲染规则见 §5）；top_seg 与 `f"{top_share:.1f}%"`；`f"{cur_rate:.1f}%"` 等按句式声明逐条查（**不做**值域成员检查）。
3. `CN_BANNED_WORDS` 段内扫描（P1：领先/滞后/领跑/改善/恶化/回暖/走强/走弱）。
4. 预算：正则 `\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?%(?:pp)?|(?<=±)\d+(?:\.\d+)?|(?<![\d.])\d{1,2}(?= 区)`，≤5/图（家数与 pp 计入、月份码不计）。
5. 界定符 count==2 ↔ narrative 开关 ↔ 图存在性三方一致；干净房间。

## §5 模板句式族（render）

箭头规则与月度明细表一致：涨跌幅度百分数渲染**取绝对值**、方向由词表达（继承 W2.3 裁决）。

### district_mom（≤3 句正文）

- 领涨句（有 leader）：`**{name}领涨，环比 {abs(pct)}%（当月 {units:,} 套）。**`
- 无 leader 且 broad_down：`**17 区全部下跌。**`（防"领涨区跌 37%"病句）
- 领跌句（有 laggard）：`{name}领跌，环比 {abs(pct)}%（{units:,} 套）。`
- 家数句：broad → `16/17 区上涨`；否则 → `17 区中 16 区上涨、1 区下跌`（不出现"普涨/普跌"字样除非 broad）。
- 门槛括注：`(500 套以上区中)`——不独立成句。

### area_segment_share（≤2 句）

- 主句：`**{seg}占 {share}%（{month} 月）。**`
- 变化句（触发时）：易主 → `{seg}份额升至第一（{share}%）`；否则 → `比{prev_month} 月 {±abs(Δpp)} 个百分点`（词表：升至/降至/持平禁用——持平由"不触发"表达）。
- 摆事实句（未触发）：`各段份额变化均在 ±{x} 个百分点内（{prev_month}→{last_month} 月）。`

### refund_rate（≤2 句）

- 主句：`**{month} 月上榜机构合计退房率 {rate}%。**`（"上榜机构合计"定语必带）
- 环比句（delta 非 None）：`比上月 {abs(delta)} 个百分点`（只报差值，禁"改善/恶化"）。
- 滚动句（roll_months_adjacent）：`近 3 个月滚动退房率 {roll}%，比前一窗口 {abs(Δ)} 个百分点`。

## §6 白名单登记（预算 12~16 条）

每图整段一行（界定符包裹）；登记：`^[+-]<!-- cn-`（界定符，1 条覆盖六方向）、三组段首 stem（`^[+-]\*\*(\d+ 区|.{2,6}占|.{2,3} 月上榜)` 类，逐句首登记约 6~9 条）、`^[+-]\*各段份额变化均在`（摆事实句单独 1 条）。禁宽通配，逐句式核对。

## §7 测试矩阵（40 → 预计 52±2）

| 测试 | 内容 |
|---|---|
| test_chart_facts_district / _area / _refund（+3） | 判定正确、门槛筛选、无候选、断档月、narrative 门、字段最小化 |
| test_chart_narrative_render_district / _area / _refund（+3） | 句式变体全覆盖（含无领涨/普跌/易主/摆事实/滚动跳句）、句数 ≤3、整段一行、界定符成对、数字 ≤5、禁用词零命中 |
| test_verify_chart_gate（+2） | 检查 11 三图注错各一次；检查 12 注错矩阵（B 类篡改/锚点缺失/界定符不符/预算超限/禁用词/json 残留） |
| test_cn_stems_registry（+1） | CN_STEMS ⊆ CHART_STEMS、三图界定符与白名单条目存在、facts 字段集 == 设计 §2 |
| 检查 11/12 对既有 test_verify_gate* fixture | 补 cn_facts.json 后仍全绿（+0~1） |

## §8 退化矩阵（实现级，需求 §六的代码落点）

| 图 | 条件 | facts | render | verify |
|---|---|---|---|---|
| 三图共通 | 图 prep 失败/数据空 | 该 stem 不入 json | 不渲染 | 界定符 0 ↔ json 无键 ↔ 图缺失三方一致 |
| district_mom | <2 月或末列全 NaN | narrative=false（最小化） | 不渲染 | 同上，json 键存在 |
| district_mom | 无 ≥500 正候选 | leader_name=None | 跳领涨句 | 锚点串条件化不查 |
| area_segment | 仅 1 个可得月 | narrative=false | 不渲染 | 同上 |
| refund | 末月断档 | narrative=false | 不渲染 | 同上 |
| refund | 末两滚动点不相邻 | roll_months_adjacent=False | 跳滚动句 | 滚动锚点不查 |

## §9 风险与预案

- **月份码漂移**：三图 x 轴随数据推进，叙述句含月份文本 → 白名单 stem 设计避开月份（"YY 月上榜"用 `\d{1,2} 月` 通配登记，属句式非数据）。
- **leader 与 laggard 同月门槛筛选后空集**：已定义（name=None 跳句），facts 字段最小化不做——保留计数句。
- **区县名与 AREA_SEGMENTS 改名**：锚点串含名称，名称变更会触发锚点失败（fail-closed 正确行为，人工核后更新 facts 来源）。
- **cn_facts.json 与 wk_facts.json 的存在性检查相互独立**：互不代位（daily 空删 wk、月度图缺删 cn，各管各的干净房间）。

## §10 实施任务切分（工期估 1.5~2 天，每任务单独 commit，测试不全绿不提交）

| ID | 任务 | 文件 | 依赖 |
|---|---|---|---|
| C1 | facts 三函数 + CN_STEMS | metrics.py、tests | — |
| C2 | render 三图句式族 | chart_narrative.py（新）、tests | C1（schema 冻结后可并行） |
| C3 | report 集成 + cn_facts.json 落盘 + 白名单登记 | report.py、analysis/CLAUDE.md、whitelist.txt | C2 |
| C4 | 检查 11（图级 CSV 锚点） | verify.py、tests | C1 |
| C5 | 检查 12（叙述组）+ CN_BANNED_WORDS | verify.py、tests | C3、C4 |
| C6 | 收尾实测（真实数据通读 + HANDOFF 留档）+ 三角色评审 → tag `cn1` | docs | C5 |
