# 图表数据叙述设计 v1.1（依据需求 v0.2；三路设计评审后修订）

> 版本史：v1.0（2026-11-03 起草）→ **v1.1（2026-11-03）**：吸收三路设计评审（QA"不通过修订后再审"/架构"有条件通过"/数据分析师"有条件通过"，合并 23 条阻断级全采纳，含三路撞车的预算体系/锚点格式/检查 11 缺口/白名单枚举；数据分析师 170 格逐格对账零不一致、实算 9 个月门槛与满配预算）→ **v1.1a（2026-11-03，QA 复核后）**：复核结论**进 C1**（11 阻断 10 条已兑现），三个 C2 前置条件落档——N-1 refund 例句上月值 2.00%→1.96%（实值更正，原句自相矛盾）、N-2 全跌句条件收紧 n_down==17 + broad_down 家数句式补冻结 + 全跌月家数句省略、N-3 area 满配 N 3→2（易主句不叠加幅度）+ 段名冻结实名无空格；实现冻结项：容差算符一律 ≤、broad 字段 ×2。分层与纪律沿用 `weekly-narrative-design.md` v1.6；本文只写差异，冲突处以周度设计为准。

## §1 架构与模块归属

```
metrics.chart_facts(stem, …DataFrame)  →  纯 dict（A 类引用值 + B 类派生，无文案）
chart_narrative.render(stem, facts)    →  纯函数（只读 facts，文案全在此层）
report.py 三处插入（图注后）+ cn_facts.json 同次落盘
verify 检查 11（图级 CSV 聚合锚点）+ 检查 12（叙述组）
```

- facts 归 metrics.py、render 归新模块 `analysis/chart_narrative.py`、`CN_BANNED_WORDS` 与 `CN_STEMS` 归 verify.py（与 WK 先例一致，单一来源 tests 从此导入）。
- **CN_STEMS**：`('district_mom', 'area_segment_share', 'refund_rate')`，须为 CHART_STEMS 子集；tests 断言 ↔ 界定符 ↔ 白名单三方同步。
- **双源对质暗道封死**（架构 F2）：`chart_facts` 自算 pct/share/ΣΣ，**不复用** `metrics.district_mom`/`agency_refund` 等 prep 中间层——否则检查 12 沦为序列化自检。
- **守卫=数据 CSV 存在性**（架构 F5/QA F-10 合并裁决）：`district_monthly/area_monthly/agency_monthly.csv` 存在 → cn_facts.json 必含对应键；CSV 缺 → 该图全部检查跳过。既有 test_verify_gate* 夹具只含 resale CSV，零改动。
- **月份码统一图轴同款 'YY-MM'**（架构 F3④/分析师 S1）：模板删"月"字（`26-09 上榜机构…`），跨年无歧义，白名单 stem 可落笔。

## §2 每图 facts schema 与判定规则

类型约定同周度；`narrative=false` 时除 `narrative, last_month` 外全 None。

### district_mom（区县环比热力）

| 字段 | 类型 | 规则（v1.1 修订处标注 ★） |
|---|---|---|
| narrative | bool | 末列非全 NaN 且 ≥2 个月 |
| last_month | str 'YY-MM' | x 轴末月 |
| n_districts | int | ★ 从数据计长并断言 ==17（fail-closed 于增区/改名）；区名单与 charts y 轴同源——**滤 '全　市'（全角空格行）、宣武/崇文（-1 占位行）**（分析师 S8/架构 F1） |
| leader_name / leader_pct / leader_units | str / float(round1) / int | 参评=上月 ≥500 套 且 pct>0，取 pct 最大；无 → None。units=当月套数（B 类，检查 11 直算锚定，分析师 F7） |
| laggard_name / laggard_pct / laggard_units | 同上 | 同门槛且 pct<0 取最小 |
| n_up / n_down / n_flat | int ×3 | ★ **pct==0.0 计持平**（与 visualMap 白=平稳一致），不计入 up/down（分析师 F2——26-08 实列 5+11+1=17） |
| broad_up / broad_down | bool | ≥15/17 同向 |

★ 门槛实算（26-07/08/09）：排除 **5** 个小区（怀柔/延庆/平谷/密云/开发区），门槛区 12 个，不筛空；需求文档"4 个"已同步更正。

### area_segment_share（面积段份额）

| 字段 | 类型 | 规则 |
|---|---|---|
| narrative | bool | ≥2 个可得月（跳过 2026-04） |
| last_month / prev_month | str | prev=上一可得月（图 JSON 侧="从末列向前扫第一个有 cells 的列"，26-04 列在轴上但无格，架构 F8） |
| top_seg / top_share | str / float(round1) | 末月最大份额段 |
| prev_top_share | float(round1) | ★ 保留但**仅供检查 12 配对直比**，无模板消费（分析师 S5 改性） |
| top_delta_pp | float(round1) | **渲染值**；★ 触发判定（top_changed / ≥0.5）用**未圆整原始差**（分析师 S4） |
| top_changed | bool | 份额易主 |
| max_abs_delta_pp | float(round1) | ★ **ceil 进位到 1dp**（1.42→1.5；round1 会向下圆整出被自身证伪的假上限，分析师 F3 实例） |

### refund_rate（机构退房率）

| 字段 | 类型 | 规则 |
|---|---|---|
| narrative | bool | 末月率非空 |
| last_month | str | 机构表末月（可落后全市月） |
| cur_rate / prev_rate / delta_pp | float(round2) ×3 | ★ **round2、渲染 2dp**（图 series 经 `_round_floats` 2dp，round1 会图文不一致 1.7 vs 1.74，架构 F7）；★ **prev=x 轴紧邻月**（标签前一格），None → delta=None 跳环比句，禁"上一有值点"读法（分析师 F1/架构 F6——断流复发时句面才不撒谎） |
| roll_last / roll_prev | float(round2) / None | series[1] 末两个**有效**滚动点（None 容忍仅 1 点） |
| roll_months_adjacent | bool | 末两有效点月份相邻 |

## §3 cn_facts.json 与界定符

- 单文件 `{stem: {字段…}}`，NaN→None、allow_nan=False、原生 cast；仅含 CSV 存在的图。
- 界定符 `<!-- cn-<stem> -->` 起止对。
- 干净房间（fail-closed 双向）：CSV 存在而 json 缺键 → issue；json 键在而 CSV 缺 → issue；`narrative=false` ↔ 界定符 0 ↔ 三方一致；★ **三图全缺 → report 删遗留 cn_facts.json**（同 wk 显式清场先例，架构 F10）。

## §4 verify：检查 11 + 检查 12

**检查 11（图级 CSV 锚点）**——★ CSV 侧清洗规则三行写死（架构 F1，约定 9 教训的 verify 版）：①区县名全角空格规范化（'东　城'→'东城'）；②滤 '全　市' 行与 ≤0 占位行（宣武/崇文）；③断档月 NaN ↔ 图 JSON 格缺席**双向配对**（cell 缺席=NaN，非 null）。y 轴按**名称映射**非位置（行序=总量降序）。分母：area 的 share=段/**area 表当月各段成交套数合计**（当前恰=总签约口径；**禁取住宅口径**——偏差 3.68pp，QA F-2）。

| stem | 直算内容 | 比对 |
|---|---|---|
| district_mom | 末月 17 区 pct、n_up/n_down/n_flat、★ **leader/laggard 全量复算（含 ≥500 门槛与并列 tie-break）、leader/laggard_units（当月套数）**（QA F-1） | cells 逐格（**容差 0.05+1e-9**，QA F-3 浮点裕量）；计数与门槛结果直比 facts |
| area_segment_share | 末月各段 share（分母=段和） | 末列 cells 逐格（0.05+1e-9）；加总 ≈100（±0.5） |
| refund_rate | 末柱 Σ退房/Σ签约 | series[0] 末值（**2dp 容差 0.005+1e-9**，QA F-14） |

**检查 12（叙述组）**，对每个 CN_STEM（守卫=CSV 存在；**narrative=false → 只做三方一致，跳过直比/锚点/预算/禁用词**，QA F-9）：

1. **A 类配对直比**（架构 F2 新增）：`leader_pct`↔(leader 行,末列) cell、`top_share/prev_top_share`↔share 矩阵末列/prev 列对应格、`cur_rate/prev_rate`↔series[0] 末值/紧邻值、`roll_last/roll_prev`↔series[1] 末两有效点——防张冠李戴（facts 抓错区/错段时其余门全拦不住）。
2. **B 类直比**（从图 JSON 重算）：n_up/n_down/n_flat、top_seg/top_changed/max_abs_delta_pp、delta_pp、roll_months_adjacent。**比较协议**：两侧 `round(x,10)` 后比、**算符一律 ≤（含等号，QA 复核冻结项——0.15 边界的 double 表示 0.15000000000000002 用 < 会误杀）**；★ 容差分型：share Δpp 类 **0.15**（1dp 格差分误差预算 0.05×3，QA F-4/分析师 F4 实算 8 对中 4 对压线）、refund 2dp 类 0.1、其余同型严格相等（继承 wn 加固：type(a) is type(b)）。
3. **锚点串**（abs 无符号、与 render 逐字一致，QA F-8/分析师 F8）：见 §5 每句式冻结列；**不做值域成员检查**。
4. `CN_BANNED_WORDS`（领先/滞后/领跑/改善/恶化/回暖/走强/走弱）段内扫描。
5. 预算正则：`\d{1,3}(?:,\d{3})*(?= 套)|\d+(?:\.\d+)?(?= 个百分点)|\d+(?:\.\d+)?%|(?<![\d.])\d{1,2}(?= 区)`（★ 补套数与裸 pp 两分支，QA F-7/分析师 F5——"900 套"“0.8 个百分点"原漏计）；上限见 §5 逐图；月份码 'YY-MM' 天然不命中。
6. 界定符 count==2 ↔ narrative ↔ CSV 存在性三方一致。

## §5 模板句式族（最终文案冻结；每图整段一行、界定符包裹；首句粗体=结论前置）

### district_mom（正文 ≤2 句；**数字上限 9，满配恰 9 不变量**）

- 有领涨领跌（合一粗体句）：`**东城领涨，环比 32.8%（当月 725 套）；房山领跌，环比 11.5%（当月 900 套）。**（均按上月签约 500 套以上筛选）`——仅半句时留半；括注挂粗体句、不独立成句、领涨领跌共享（分析师 S2）。
- ★ 全跌月（**n_down==17**，QA 复核 N-2 收紧——n_up==0 不排持平，0 涨+16 跌+1 持平时"全部下跌"与家数句"1 区持平"自相矛盾）：`**17 区全部下跌。**`（全跌句自身已承载家数信息，**家数句省略**；领涨句必然缺席，无需括注）
- 家数句（全跌月省略，见上条）：broad_up → `16/17 区上涨（普涨）`；★ broad_down → `16/17 区下跌（普跌）`（QA 复核 N-2 补冻结）；非 broad → `17 区中 5 区上涨、11 区下跌、1 区持平`（n_flat=0 省略末段）。
- 锚点串：leader/laggard 的 `name`、`f"{abs(pct):.1f}%"`、`f"{units:,} 套"`（两句统一"当月"字样）；全跌月 `"17 区全部下跌"`；家数句 `n_up/n_down/n_flat` 数字。

### area_segment_share（正文 ≤2 句；**数字上限 2，满配恰 2**——QA 复核 N-3 修正：主句 1 + 变化句/摆事实句 1 恒为 2，v1.1 的"3"构造不出；★ 段名落笔冻结为图轴/CSV 同款实名**无空格**）

- `**60m2以下占 31.0%（26-09）。**`
- 变化句（易主）：`60～80m2份额升至第一（31.2%）`（不叠加幅度半句，N 值按此冻结）；（非易主 |Δ|≥0.5）：`比 26-08 升 0.8 个百分点`（方向词表：升/降；★ 补方向，分析师 F6）。
- 摆事实句（未触发）：`各段份额变化均在 ±1.5 个百分点内（26-08→26-09）。`
- 锚点串：`top_seg`、`f"{top_share:.1f}%"`、`f"{abs(top_delta_pp):.1f} 个百分点"` 或 `f"±{max_abs_delta_pp:.1f} 个百分点"`。

### refund_rate（正文 ≤2 句；**数字上限 5，满配恰 5**）

- `**26-09 上榜机构合计退房率 1.74%，比上月 1.96% 回落 0.22 个百分点。**`（★ 带上月值可回推验算，分析师 S6 采纳；方向词：回落/上升；prev 断档→半句 `**26-07 上榜机构合计退房率 2.53%。**`；例句 1.96% 为 QA 复核 N-1 更正——26-08 实值 1.96%，v1.1 误写 2.00% 与 0.22 个百分点自相矛盾）
- 滚动句（roll_months_adjacent）：`近 3 个月滚动退房率 2.06%，比前一窗口回落 0.19 个百分点。`
- 锚点串：`f"{cur_rate:.2f}%"`、`f"{prev_rate:.2f}%"`、`f"{abs(delta_pp):.2f} 个百分点"`、`f"{roll_last:.2f}%"`。

## §6 白名单（render 冻结后按实际首句全变体枚举，共约 12 条）

- 界定符逐 stem 双向带 `$`：`^[+-]<!-- cn-district_mom -->$` 等 6 条。
- 段首：`^[+-]\*\*(.{2,5}(领涨|领跌)|17 区全部下跌)`（2 条，覆盖首句三态）；`^[+-]\*\*.{3,9}占 `（2 条）；`^[+-]\*\*\d{2}-\d{2} 上榜机构合计退房率`（2 条）。
- 禁宽通配；登记时逐句式核对（v1.0 的 `.{2,6}占`/`.{2,3} 月上榜`/行首摆事实句三条死/漏条目全部废弃重写）。

## §7 测试矩阵（40 → 52~55）

| 测试 | 内容 |
|---|---|
| test_chart_facts_district / _area / _refund（+3） | 判定/门槛/无候选/断档/narrative 门；★ 含 26-08 持平列、全角空格+宣武占位行清洗回归（架构 F1） |
| test_chart_narrative_render 三图（+3） | 全句式变体逐字冻结、句数 ≤2 正文、整段一行、界定符成对、★ **满配恰 N 不变量**（9/2/5）、禁用词零命中 |
| test_verify_chart_gate（+3） | 检查 11 三图注错（含门槛筛选错/units 错）；检查 12 注错矩阵（A 类配对张冠李戴/B 类篡改/锚点缺失/界定符不符/预算超限/禁用词/json 残留/narrative=false 三方一致） |
| test_cn_stems_registry（+1） | CN_STEMS ⊆ CHART_STEMS、界定符与白名单条目存在、facts 字段集 == §2 |
| 既有 test_verify_gate* / weekly | ★ 零改动（守卫=CSV 存在性；夹具无三图 CSV 即自然跳过）——替代 v1.0 的"+0~1 补 fixture" |

## §8 退化矩阵（verify 列逐行独立）

| 图 | 条件 | facts | render | verify |
|---|---|---|---|---|
| 三图 | 数据 CSV 缺/prep 失败 | 不入 json | 不渲染 | 跳过该图全部检查；json 残留键 → issue |
| district | <2 月或末列全 NaN | narrative=false（最小化） | 不渲染 | 仅三方一致（键在/界定符 0/CSV 在） |
| district | 无 ≥500 正候选 | leader_name=None | 跳领涨半句 | 领涨锚点不查 |
| area | 仅 1 可得月 | narrative=false | 不渲染 | 仅三方一致 |
| refund | 末月断档 | narrative=false | 不渲染 | 仅三方一致 |
| refund | 紧邻月 None（断流复发） | delta_pp=None | 跳环比半句 | 环比锚点不查 |
| refund | 末两滚动点不相邻或仅 1 点 | roll_months_adjacent=False / roll_prev=None | 跳滚动句 | 滚动锚点不查 |

## §9 风险与预案

- **图文视觉极值矛盾**（登记，分析师 S3）：门槛外小区可为图上最深色格（26-09 视觉极值=开发区 +46.5%、叙述领涨=东城 32.8%）——括注"按上月签约 500 套以上筛选"已示口径，属已知差异非错误。
- 区县名/段名变更 → 锚点失败 fail-closed，人工核后更新。
- cn/wk 两 json 存在性检查相互独立，互不代位。

## §10 实施任务切分（每任务单独 commit，测试不全绿不提交；工期 1.5~2 天）

| ID | 任务 | 文件 | 依赖 |
|---|---|---|---|
| C1 | facts 三函数 + CN_STEMS | metrics.py、tests | — |
| C2 | render 三图句式族（§5 逐字冻结） | chart_narrative.py（新）、tests | C1 schema 冻结（可并行） |
| C3 | report 集成 + json 落盘 + 白名单 | report.py、analysis/CLAUDE.md、whitelist.txt | C2 |
| C4 | 检查 11（含 CSV 清洗复刻） | verify.py、tests | 与 C1/C2 均无依赖（架构确认可最先做） |
| C5 | 检查 12 + CN_BANNED_WORDS | verify.py、tests | C3、C4 |
| C6 | 真实数据通读 + HANDOFF 留档 + 三角色评审 → tag `cn1` | docs | C5（回归样本：26-08 持平列、26-05vs03 跨断档 1.42pp 对） |

## §11 评审纪要（2026-11-03，设计 v1.0 三路）

QA：不通过（修订后再审）11 阻断+5 建议；架构：有条件通过 4 阻断+8 建议；数据分析师：有条件通过 8 阻断+10 建议（170 格对账零不一致、门槛/预算/容差全部实算）。合并去重 23 阻断全采纳；三路撞车确认四主题：预算体系（≤5 与满配 7 冲突+pp/套数漏计）、锚点格式（+号 vs abs 渲染）、检查 11 缺口（极值/门槛/units/CSV 清洗/分母/容差）、白名单枚举（段名长度/段首变体失配）。修订后按 QA 出口：**QA 复核 §4/§5/§6 三节即可进 C1**，不全量重审。
