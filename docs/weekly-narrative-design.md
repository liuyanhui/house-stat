# 周度成交趋势文字分析 — 设计方案 v1.2

> 版本史：v1.0（2026-10-30 起草）→ v1.1（2026-10-30）：吸收第一轮三角色评审（架构/数据分析师/QA，11 阻断+12 建议）——streak 差一修正（链长−1）、direction=null 三成因、末周三态枚举（asof 机制删除，回纯数据锚）、水位基准剔除近 4 周、锚句回 avg4+三重排除、锚点条件化、narrative=false 字段最小化、预算裁剪规则、测试补 4 用例 → **v1.2（2026-10-30）**：吸收第二轮产品/技术写作评审（4 阻断+5 建议全采纳）——null 三句式去预期语气、水位短语方向中立化、退化档语病修正、N=1 带值、lagged 口语化、日期码锚定末周值、holiday_last×谷值同周去重、裁剪顺序细化。

## 0. 需求映射（v0.3 → 本设计）

| 需求条 | 设计落点 |
|---|---|
| §2.1 三问 / §2.3 统计口径 | §2.1 事实函数（方向链 / 水位 / 峰谷 / 末周四态） |
| §2.2 措辞纪律 | §2.2 渲染器（句骨架 + 子句矩阵 + 锚点规则 + 预算） |
| §2.4 降级矩阵 | §2.1 判定输出 + §2.2 组装规则（降级由字段缺失/null 表达） |
| §2.5 集成 / wk_note 取代 | §2.3 |
| §3.1 verify 门 | §2.4 |
| §3.2 测试 / 交叉验证 | §5 |

## 1. 数据流增量（一条管线加一个旁支，不动主链）

```
report.generate()：
  weekly = weekly_aggregate(daily)                                  # 既有，图用
  facts = metrics.weekly_trend_facts(weekly, daily)                 # 新：纯 dict，纯数据派生
  md 三节 ← weekly_narrative.render(facts)                          # 新：纯函数，<!-- wk-narrative --> 包裹
  report/wk_facts.json ← facts（原生类型，NaN→None，allow_nan=False）
  verify.check()：第 10 项检查组（独立重算直比 + 段内锚点 + 措辞门）
```

## 2. 模块设计

### 2.1 `analysis/metrics.py`：`weekly_trend_facts(weekly, daily) -> dict`

输入：`weekly`（`weekly_aggregate` 输出——周口径与 est_days 单一来源）+ `daily`（仅末周：日期位置、数据末日、ip 数值单一来源）。**无时间参数：全部从数据派生**（评审裁决：需求 §2.1.3 本就是数据末日锚，引入 asof 属设计漂移，且制造翻周竞态与 quality.json 回读依赖——整体删除）。输出纯 dict（§3），不含任何文案。

**完整周**：`days ≥ 7`，按 week_start 升序记 `fw`；`n_full = len(fw)`。`n_full < 8 → narrative=false`，**json 仅保留 `narrative/n_full_weeks/last_week_state` 三字段，其余全 null**（降级字段最小化，防 verify 直比歧义）。

**判定一：方向链**（从 `fw` 末位向前走）：

```
chain = [fw[-1]]
for i in range(len(fw)-1, 0, -1):
    a, b = fw[i-1], fw[i]                  # a 早 b 晚
    if (b.ws - a.ws).days > 7: break       # 缺口中断
    if holiday(a) or holiday(b): break     # 节假日周中断（任一侧）
    if a.units == b.units: break           # 持平中断
    d = sign(b.units - a.units)
    if chain 方向已定 and d != chain_dir: break
    chain_dir = d; chain.insert(0, a)

N = len(chain) - 1                          # ★ 上涨/下跌次数（链 [100,110] → N=1，修差一）
direction = up/down（N ≥ 1）；len(chain) == 1 → direction=null
null_reason（direction=null 四成因；gap 为 W1a 实施补充——原设计三成因漏了"末对缺口"）：
  holiday_last  末完整周本身是节假日周 → "假期周与其前后周不可直接比较"
  holiday_prev  末完整周的前一周是节假日周（节后首周，无可比第二对）
  tie           末两完整周持平
  gap           末对周序缺口 >7 天（数据断流，无可比第二对）
last_pair_reversal：N==1 且再前一对（若可比）方向相反 → 渲染层选互现句
streak_contains_est = chain 内任一周 est_days>0
post_holiday = chain[0] 前一周（fw 中相邻）是节假日周 → "节后"前缀（仅 up/down 有意义）
```

节假日周：周内 7 天 `chinese_calendar.is_workday` 计净额 **≤3**；库未覆盖回退纯 weekday 并告警一次（同 `script/analyze.py:classify_day_type` 模式）。

**判定二：水位**：`avg4 = mean(fw[-4:])`；**基准 = fw 剔除最近 4 周后的最近 26 个完整周**（`fw[-30:-4]`；不足 26 个取全部更早周；**不足 13 个 → level=null**）——基准含近 4 周自身会自比稀释（评审：前段序列几乎恒 parity）。`gap = (avg4−基准)/基准×100`；`|gap| ≤ 5 → parity`，`> 5 → above`，`< −5 → below`。基准周数 <26 → 措辞"有数据以来"。

**判定三：峰谷**：窗口 `fw[-13:]`（`n_full < 13` → 峰谷字段全 null）。peak/trough = 窗口 max/min，**tie-break 优先非节假日周**（防首现为节假日周时把次周同值也归因假期）；trough **保留**节假日周为候选（评审部分采纳：节假日周是事实低点、其解释句是普通读者核心价值；复读随 13 周窗口滑动自然消退）；各带 est/holiday 标记，节日名不进 json。

**判定四：末周状态**（三态枚举，纯数据锚）：

```
complete    ：末周 days == 7
in_progress ：days < 7 且非 lagged（本周自然未完）
lagged      ：days < 7 且 前一周也 days<7 且前一周非节假日周（数据滞后，§2.4 滞后降级；
              持续滞后的告警是 check_daily_freshness 既有职责）
```

进行中周的同期对齐：位置集合 P = 末周实际出现日期的周内偏移；`ip_contiguous = (max(P)−min(P)+1 == |P|)`，**非连续 → S4 整句跳过**（周中缺天时"刚过 D 天"失实，不如不说）；对比基期 = 上一自然周同偏移日期，P 内日期在 daily 全存在 → `ip_prev_same_ok=true`；`ip_days < 3` → S4 跳过（字段保留）。**ip_* 数值一律取自 daily 末周子集**（与位置同源；weekly 末行仅用于状态判定）。

### 2.2 `analysis/weekly_narrative.py`（新模块）：`render(facts) -> str` 纯函数

实施即抽独立模块（句骨架+子句矩阵+降级超 150 行判据，避免二次搬迁）。只读 facts dict，不反查 DataFrame；输出 md 段落，界定符 `<!-- wk-narrative -->` 起止包裹。

**句骨架**（S1 必出，S2–S5 按事实存在性组装；S2/S3 合并为一条时间线故事，任一周值只出现一次）：

| 句 | 内容 | 出现条件 |
|---|---|---|
| S1 加粗 | N≥2：`**[节后]成交连续 N 周[回升/回落][，水位短语]。**`；N=1：`**最近一周[回升/回落]，成交 X 套[，水位短语]。**`（last_pair_reversal → `**近几周有涨有跌，最近一周回升，成交 X 套。**`）；null 四句式——holiday_last：`**最近一周恰逢[节日名]，签约受假期影响，本周不作涨跌比较。**`（该周同时是 trough13 时简化为 `**最近一周恰逢假期，涨跌暂不比较。**`，节日名与解释句由 S2 唯一承载，防双句复读）；holiday_prev：`**节后首周，涨跌走势还看不出来。**`；tie：`**最近两周基本持平。**`；gap：`**此前一周数据缺失，暂无可比走势。**` | narrative=true |
| S2 走势 | 峰谷按时间序讲故事；谷/峰为节假日周 → 节日名表述 + `——假期签约少是每年惯例，不是市场转冷`；含估算 → 值后 `（含估算）`；N=1 时锚句（若有）附于本句末 | 峰谷非 null |
| S3 细节 | 链值：N≥2 列首尾 2 值（中间省略）+ 锚句；**末周值后挂全段唯一日期码**：`3,760 套（10 月 19–25 日当周）` | N ≥ 2 |
| S4 最新 | in_progress：`本周刚过 D 天，日均 A 套，[仍]高于/低于上周同期（B 套）。`（ip_prev_same_ok=false → 仅报本周套数）；lagged：`本周只统计到 D 天，共 X 套（官方数据更新延迟）。`；complete：不出现 | days<7 且 ip_days≥3 且 ip_contiguous |
| S5 段末注 | `（文中"连续 N 周"含 1 个估算周；估算口径见数据说明。）` | streak_contains_est |

- **水位短语（方向中立）**：above=`高于最近半年的周均水平`；below=`仍低于最近半年的周均水平`；parity=`与最近半年的周均水平大体相当`；基准 <26 周退化档同构换头：`最近半年的周均水平`→`有数据以来的周均水平`。
- **时间锚**：`X 月上/中/下旬`（1–10/11–20/21–月末）；节日名 `cc.get_holiday_detail` → 口语映射（国庆节→国庆假期那周……未知名回退旬表述+`假期集中的一周`）。
- **锚句规则**：基准=**avg4**（评审回正：需求定 4 周均，v1.0 漂移成末周值）；候选=13 周窗口内**排除链上各周、节假日周、近 4 周内各周**；与 avg4 差 ≤10% 的最近周 → `大约回到 X 月 X 旬的水平`；无候选且 avg4 为窗口极值 → `处于近三个月高/低位`；再否则无锚句。
- **数字预算 ≤6 与裁剪**：计数=千分位套数 + `日均 N`（**日均恒整数、恒无千分位**）；S1（含 N=1 带值）/S5/界定符/日期码不计；时间线去重后仍超 6 时按序裁剪：**峰值套数（保"X 月中旬高点"旬表述）→ 锚句 → S3 首值**；谷值与节假日解释句绑定、不可裁。

### 2.3 `analysis/report.py`（改）

- 三节：叙述段插在周度图注之后、近 10 周表之前；**wk_note 分支删除**（S4 吸收其信息）。
- `wk_facts.json` 与叙述段同次落盘（**daily 非空必写**；daily 空 → 整节跳过**且删除遗留 wk_facts.json**——干净房间补漏，防旧文件冒充新产物）。
- 同步更新：`tests/baseline/whitelist.txt`（wk_note 两行 + **叙述段新增行通配正则**：S1–S5 与界定符行）；`analysis/CLAUDE.md`。

### 2.4 `analysis/verify.py`（改）

- **常量**：`WK_BANNED_WORDS = ['预计','有望','或将','拐点','动能','窗口均值','口径','回暖','走强','走弱']`（单一来源，tests 从此导入）。
- **独立重算** `_wk_facts_from_csv(data_dir, estimated_dates=None)`：csv 直读 `resale_daily.csv`（**跳过 住宅签约套数≤0 的 -1 占位行；"daily 非空" = 有效行 ≥1**，仿 kpi mtd 先例）→ datetime 自建周一分桶 → 逐字段重算（方向链全套、chinese_calendar 净额、est 缺省 import quality.ESTIMATED_DAYS 展开，**禁复制登记**；测试经 estimated_dates 注入）。**无 asof 读取**（asof 机制已删）。
- **第 10 项检查组**（md 相关项以 md 文件存在为守卫，与现有 check 一致）：
  1. 存在性：daily 非空而 `wk_facts.json` 缺失 → issue（fail-closed）。
  2. 降级一致性：独立数 n_full<8 ↔ json narrative=false（且其余字段全 null）↔ md 界定符 0 个；narrative=true ↔ 界定符恰 2 个（`md.count('<!-- wk-narrative -->') == 2`，取首对之间为段；1/3/4 个均 issue）。
  3. 事实直比：逐字段（int/str/bool/枚举精确等；**float：未 round 重算值对 json round1 值，`abs>0.1` 才 issue**，含等号）。
  4. 段内锚点（条件化，防 mixed 假失败）：`direction ∈ {up,down}` → `f'{last_full_units:,}'` 须在段内；peak13/trough13_units 非 null → 其千分位须在段内。
  5. 措辞门：段内 WK_BANNED_WORDS 扫描 + 预算计数（`\d{1,3}(?:,\d{3})+` 与 `日均\s\d+`，≤6）。

## 3. `report/wk_facts.json` schema（原生类型，NaN→None，allow_nan=False；文案不进）

| 字段 | 类型 | 说明 |
|---|---|---|
| narrative | bool | n_full ≥ 8；false 时除前三字段外全 null |
| n_full_weeks | int | 完整周数 |
| last_week_state | complete/in_progress/lagged | 末周三态 |
| last_full_week / last_full_units | str / int | 末完整周 |
| direction | up/down/null | null=链长 1 |
| null_reason | holiday_last/holiday_prev/tie/gap/null | direction=null 成因 |
| streak_weeks | int | **N=上涨/下跌次数**（≥1 才有方向；0=null） |
| last_pair_reversal | bool | N=1 时互现句依据 |
| streak_contains_est / post_holiday | bool | 段末注 / "节后"前缀 |
| avg4 / avg26 / avg26_weeks | float(round1) / float(round1) / int | avg26=剔除近 4 周的最近 ≤26 完整周均；avg26_weeks=基准窗周数（W1b 实施补充，渲染层"半年/有数据以来"措辞依据），<26 触发退化措辞 |
| level | above/below/parity/null | ±5% 死区；基准 <13 周 null |
| level_gap_pct | float(round1) / null | |
| peak13_week/units/est/holiday | str/int/bool/bool | <13 周全 null；tie-break 非节假日优先 |
| trough13_* | 同上 | 节假日周保留为候选 |
| ip_week / ip_units / ip_days | str/int/int | daily 末周子集单一来源 |
| ip_contiguous | bool / null | 非连续 → S4 跳过 |
| ip_daily_avg / ip_prev_same_avg | int / int或null | **恒整数**（无千分位无小数） |
| ip_prev_same_ok | bool / null | |

## 4. 时间锚（v1.1 取消 asof）

v1.0 曾设计 gen_at→facts→quality.json→verify 回读链以判"进行中周"。评审裁决：需求 §2.1.3 本就是**数据末日锚**，asof 属漂移且引入翻周竞态与 quality.json 回读失败路径。v1.1 全部判定纯数据派生（末周三态见 §2.1 判定四），无任何时间参数；quality.json 保持既有用途不变。

## 5. 测试设计（tests/run_tests.py 体系；不 mock chinese_calendar）

fixture 构造器：`_mk_daily(start, weeks, units_pattern, ...)` → 日度 DataFrame + 临时 CSV；节假日场景用真实日期（2025-09-29 周含 10-01~03）；**est 场景经 weekly_aggregate(estimated_dates=…) 与 _wk_facts_from_csv(estimated_dates=…) 注入同一集合**（不硬绑 2026-09-16，序列 start 避开 2026-07 防混入第二个 est 周）。

| 用例 | 断言 |
|---|---|
| test_weekly_facts_direction | 连涨/连跌（含 N 差一：链 [100,110] → N=1）；节假日周中断（国庆周前后）；持平中断；缺口>7 天中断；估算周不中断+streak_contains_est；post_holiday；null 三成因各自 null_reason；last_pair_reversal |
| test_weekly_facts_level | ±5% 三档；基准剔除近 4 周（构造重叠场景验证不恒 parity）；<26 措辞退化；基准 <13 → null |
| test_weekly_facts_last_week | complete/in_progress/lagged 三态（滞后=前一周也缺天且非节假日）；同期对齐数值（手工构造周二三位置）；上周同位置缺天→ok=false；P 非连续→contiguous=false；days<3；ip_* 等于 daily 末周子集 |
| test_weekly_facts_degrade | 7 完整周 narrative=false 且其余字段 null；恰好 8/13/26 等值边界 |
| test_weekly_narrative_render | S1 五变体（N≥2/N=1 带值/互现/null 三句式含 trough 同周简化）；节假日谷→节日名+惯例句（**含 est×节假日叠加**：两句式并存顺序）；est 峰→（含估算）+段末注；**节假日周不触发负例**（无解释句）；**lagged 变体**；日期码唯一次（末周值后）；预算 ≤6（正则计数，含满配场景+裁剪顺序）；界定符成对；水位三档短语与退化档 |
| test_verify_weekly_gate | 交叉验证：同一合成 CSV → metrics 链 vs `_wk_facts_from_csv` 逐字段相等；篡改 json→fail；narrative=false 与界定符不一致→fail；daily 非空缺 json→fail；段内禁用词→fail；预算超 6→fail；既有 test_verify_gate* fixture 补 wk_facts.json 后全绿 |

## 6. 实施顺序（每步全绿再进）

1. **T1** `metrics.weekly_trend_facts` + test_weekly_facts_*（4 组）
2. **T2** `weekly_narrative.render` + report 集成（wk_note 删除、json 落盘+daily 空清遗留、whitelist/CLAUDE.md 同步）+ render 测试
3. **T3** verify 第 10 项检查组 + test_verify_weekly_gate（含交叉验证）+ 既有 verify fixture 补文件
4. **T4** 全量回归 + 真实数据人工通读（§2.2 措辞纪律）+ 报告重生

## 7. 评审处置记录

**第一轮（v1.0 → v1.1，2026-10-30）**

| 角色 | 意见 | 处置 |
|---|---|---|
| 架构 | ①判定四 asof 偏离需求（数据末日锚才是对的，asof 制造竞态+滞后变体死分支）；②锚点无条件必查假失败；③锚句基准漂移（末周值≠需求 4 周均）；④quality.json 回读键路径/失败路径；⑤daily 空遗留旧 json、md 缺失守卫；⑥预算可超 6/重复计数/日均千分位漏计；⑦narrative=false 字段照算还是 null、原生 cast；⑧ip_* 单一来源 | ①②③采纳（三态枚举+asof 整体删除、锚点条件化、基准回 avg4）；④随①失效（不再读 quality.json）；⑤⑥⑦⑧采纳（§2.3/§2.2/§3/§2.1） |
| 数据分析师 | ①streak=len(chain) 差一（[100,110]→"连续 2 周"失实）；②链长 1 三成因全落 mixed 失实、post_holiday×mixed 未定义；③锚句候选未排节假日周+恒现废话+基准漂移；④mixed 锚点误杀；⑤基准含近 4 周自比稀释→前段恒 parity；⑥峰谷首现 tie-break、谷被节假日周锁死复读；⑦P 非连续时天数/日均矛盾 | ①②③④⑤采纳（N=链长−1、direction=null+三成因句式、avg4+三重排除、锚点条件化、基准剔除近 4 周）；⑥部分采纳（tie-break 采纳；**谷值排除节假日周驳回**——节假日周是事实低点，其解释句是普通读者核心价值，复读随窗口滑动自然消退）；⑦采纳（非连续整句跳过） |
| QA | ①asof 源失败路径（quality.json 嵌套键+fixture 缺文件）；②锚点组合洞；③daily 非空与 -1 清洗未定义；④容差两侧 round 掩盖漂移；⑤界定符"恰 1 对"匹配规则；⑥测试缺 3 条（est×节假日叠加、滞后变体、节假日负例）；⑦fixture 年份耦合+est 注入点；⑧whitelist 路径与行数 | ①随架构①失效（不读 quality.json；fixture 补文件保留于 §5/§6-T3）；②③④⑤⑥⑦⑧全部采纳（§2.4/§5） |

**第二轮（v1.1 → v1.2，2026-10-30，产品/技术写作）**

| 角色 | 意见 | 处置 |
|---|---|---|
| 产品/技术写作 | ①holiday_prev 句"待更多周确认"属预期语气、"连续走势"抽象；②holiday_last 句"假期"两现+公文体；③below"但还没回到"仅 up 通、连词矛盾+软预期；④parity 退化档"的量"悬空语病；⑤N=1 结论句零数字过薄；⑥lagged"数据滞后"读者分不清、"已录得"财经腔；⑦缺时间定位（"最近一周"指哪几天）；⑧holiday_last×谷值同周 S1/S2 双复读"国庆"；⑨满配密度可接受、再裁先裁峰值套数（谷值与解释句绑定不可裁） | 全部采纳（①`节后首周，涨跌走势还看不出来`；②`最近一周恰逢[节日名]，签约受假期影响，本周不作涨跌比较`；③方向中立`仍低于…`；④`…周均水平大体相当`+退化档同构换头；⑤N=1 S1 带值 `成交 X 套`；⑥`本周只统计到 D 天，共 X 套（官方数据更新延迟）`；⑦日期码唯一锚定 S3 末周值 `（M 月 D–D 日当周）`；⑧同周简化句+节日名 S2 唯一承载；⑨裁剪序：峰值套数→锚句→S3 首值） |

**实施修正记录（W1a/W1b，2026-10-30）**

- null_reason 增补第 4 成因 `gap`（末对周序缺口 >7 天）：原设计三成因漏了链长 1 的缺口路径；句式 `**此前一周数据缺失，暂无可比走势。**`（§2.1/§2.2/§3 已同步）。
- 峰谷 tie-break 实现曾写反（优先了节假日周），test_weekly_facts_direction ⑩ 抓获后修正——交叉验证价值的即时体现。
- schema 增补 `avg26_weeks`（基准窗周数）：渲染层需据此选"最近半年/有数据以来"措辞，原 schema 缺该字段（W1b）。
