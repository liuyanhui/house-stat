# house-stat — 北京房地产网签数据

从北京市住建委抓取网签数据，并做**二手住宅成交趋势分析**（全市 + 各区，数字 + 图表）。**确定性代码报告不含预测**（只摆事实）；**AI 分析层（ai_digest）可做预判**，按资深分析师纪律。

> 各目录另有 `CLAUDE.md` 说明本目录模块。

## 常用命令

```bash
pip install -r requirements.txt
python main.py                      # 抓取最新数据（含完整性校验门，失败非零退出）
python script/analyze.py --report   # 生成趋势报告（report/ 下 md + 交互 html + png + charts/*.json + kpi.json），渲染→verify门→自动开浏览器；失败隔离 *.bad 非零退出；一键入口 report.bat
python script/validate.py           # 独立数据完整性校验
py -3.14 tests/run_tests.py         # 全套测试（换机冷启动先跑这个，全绿为准）
python script/gen_html.py           # 单独把 trend_report.md 转成自包含 html
python script/gen_ai_digest.py      # 导出 AI 客观分析 digest+prompt（report/ai_digest.md，手动喂 LLM）
```

> **Python 版本**：默认用 **Python 3.14**（`py -3.14`），它带原生 cp314 wheel（numpy/pandas/matplotlib/PIL/chinese_calendar 等）。关键陷阱：本机全局 `PYTHONPATH` 指向 `D:\liuyh\software\Python\Python313\Lib\site-packages`，会排在 3.14 `sys.path` 最前，用 cp313 的 numpy 遮蔽 3.14 原生包导致 import 崩。**故 `run.bat`/`report.bat` 在调用前都 `set "PYTHONPATH="` 清空**。直接跑脚本同理：`PYTHONPATH= py -3.14 script/analyze.py --report`。换干净机器（无该 PYTHONPATH）直接 `py -3.14` 即可；matplotlib/numpy 报错先检查 PYTHONPATH 是否被污染，再考虑建 venv。

## 数据流

```
main.py → fetcher(存档 data/raw/) → parsers → storage(去重写 data/*.csv)
        → check_monthly_feeds(断流门) → validate_integrity(校验门) → check_daily_freshness(滞后告警)
script/analyze.py --report → analysis.load → metrics(口径防护) → charts(PNG + charts/*.json)
        → report(md) → html_render(html，图表交互注入/目录/NO_JS退化)
        → verify 门(锚点独立重算/成对契约/体积门；失败隔离 *.bad 非零退出) → 开浏览器
```

## 目录

| 目录 | 作用 | 入 git |
|------|------|--------|
| `data/` | 官方自爬 CSV（可靠基准，逐月累积）；`raw/` 存原始 HTML（不进 git） | ✓（raw/ 除外） |
| `parsers/` | HTML→DataFrame 解析器 | ✓ |
| `utils/` | 抓取/存储/校验/日志/目录 | ✓ |
| `analysis/` | 趋势分析包（load/metrics/charts/report/html_render/verify/ai_digest） | ✓ |
| `docs/` | 升级项目文档：需求 v0.3 / 设计 v1.1 / 开发计划 v1.1 / **HANDOFF.md（换机冷启动入口）** | ✓ |
| `script/` | 入口脚本（analyze/gen_html/gen_ai_digest/validate） | ✓ |
| `tests/` | 测试骨架 run_tests.py + fixtures 合成样本 + 基线/像素 diff 工具（`PYTHONPATH= py -3.14 tests/run_tests.py`） | ✓ |
| `assets/vendor/` | 第三方前端依赖原样入库（echarts.min.js 5.6.0，版本+sha256 见其 README.txt，永不手改） | ✓ |
| `report/` | 报告产物（md+html+png，代码生成） | ✗ 已忽略，每次重生 |
| `log/` | 运行日志 | ✗ 已忽略 |

## 数据覆盖状态（截至 2026-10-27，官方自爬）

| 文件 | 覆盖 | 说明 |
|---|---|---|
| resale_monthly | 2025-01 ~ 2026-09（21 月） | 全市月度，主序列 |
| resale_daily | 2025-04-21 ~ 2026-10-25 | 周度聚合来源；2026-07-25/26 为估算（见约定 8）；2026-09-16 为残差回推估算（见约定 11） |
| district_monthly | 2025-12 ~ 2026-09 | 各区月度 |
| area_monthly | 2025-12 ~ 2026-09，缺 2026-04 | 2026-04 永久丢失（见约定 7）；发布侧列 2026-07 起有数据 |
| price_monthly | 2026-03 ~ 2026-09 | 发布数据全占位（官方行为），已从报告移除 |
| agency_monthly | 2025-12 ~ 2026-09，缺 2026-05/06 | 经纪机构排行；05/06 因页面删"发布套数"列致解析断流而丢失（2026-08 修复，见约定 9） |
| new_daily | 2026-04-29 ~ 2026-10-25，缺 2026-09-16 | 商品房每日；2026-08-23 起认购/签约 4 表字段全量解析；9/16 无月度表可对账、永久缺失（见约定 11） |
| resale_5year / new_5year | 2020 ~ 2024 年度 | 长周期 |

## 关键约定（勿踩）

1. **数据可靠性优先**：自爬/官方是基准；第三方历史未纳入主序列（口径/可靠性，媒体历史多锁图片/微信门）。
2. **校验门是防线**：改解析器或数据后必跑 `script/validate.py`，面积/价格段加总必须 ≈ 全市（5%）。
3. **`parse_area_data` 按表头文本定位行**，勿改回位置索引（曾因页面改版把"发布套数"误存为"成交套数"）——此条已推广为所有解析器的总纲，见约定 9。
4. **plots 宽表 index 先转字符串**（`_str_index`），否则 x 轴 Period 序数错位。
5. **节假日用 `chinese_calendar`**，勿用旧的"低于工作日均值 15%"循环逻辑。
6. **预测分层**：确定性代码报告（trend_report）不含预测，仅历史事实；**AI 分析层（ai_digest→AI）可做预判**，但按资深分析师纪律——区分事实/判断、短 horizon（1–3 月）、情景化、给置信度+证伪条件，预判非事实非定论、不含价格、不作买卖建议。
7. 面积段 2026-04 永久丢失（解析器时间窗口 bug，已记入报告数据说明）。
8. **2026-07-25/26 日数据为月度回推估算**（官方故障缺失，非真实观测）：按 `(7月月度总量 − 7月已有日度之和) ÷ 2` 补全——签约 154/154、住宅 147/146（293 奇数，147 给周六 25、146 给周日 26）、签约面积 13715.98×2、住宅面积 13429.49×2。补全后 7 月日度和与月度精确相等（15910 / 14037）。序列可连续使用，但引用单日值须注明为估算。
9. **解析器定位一律按表头/表标题文本，不做位置索引、不做行内关键字二次确认**（页面改版多次踩坑：面积表"发布套数"误存为"成交套数"、经纪机构表删列后断流 3 个月、商品房日表标题独占一行导致 `'期房' in str(row)` 永假、`soup.find_all(text=True)` 会命中 `<script>` 文本定位到容器表）。
10. **防断流三件套**（2026-08 加入）：`data/raw/` 原始 HTML 按日存档（页面只显示最新一天/一月，错过无法回补）；`check_monthly_feeds` 断流门（月度表解析为空且页面月份更新 → 非零退出）；`check_daily_freshness` 滞后告警。`validate` 另含住宅≤总计、日月对账规则；历史存量异常走 `KNOWN_ISSUES` 白名单（当前 1 条：2025-01 总面积 < 住宅面积，首爬即如此无从修正）。
11. **2026-09-16 日数据为残差回推估算**（非真实观测）：当日 13:00 定时抓取拿到的是 9-15 数据、9-17 未运行（git 无提交），页面只显示最新一天故无法回补。按 9 月月度值减其余 29 日之和回补——总量 779 套 / 68,450.30 m²、住宅 704 套 / 64,185.35 m²；补全后 9 月日度和与月度精确相等（17,633 / 15,756），日月对账由"覆盖不整月跳过"变为正常校验。序列可连续使用，但引用单日值须注明为估算。**new_daily 同日缺失且无月度表可对账，不回补、永久缺失**。
12. **只用自爬数据（2026-10-26 定，硬规则）**：分析与报告只允许引用官方自爬 CSV，不引入、不引用任何第三方/媒体数据。据此已移除报告"上半年网签长周期（媒体引用）"参考表与 ai_digest 的 H1 媒体锚点（含 prompt 模板相关表述）。长周期只用自爬年度表（2020–2024）。

## 路线图

- **交互式报告升级（✅ 完成，tag m0–m3）**：19 张交互图（ECharts 单文件内嵌、`DASHBOARD_NO_JS=1` 退化纯图）、KPI 卡、可排序表格、质量标注（估算周红框/缺失月空列）、TTM/累计同比/非住宅占比、新房板块（周度认购签约/库存/二手对照）、区县环比热力（七档分色）、机构退房率、五年锚点；32 用例测试 + 独立重算验收门（失败隔离 `*.bad`）。续作入口与 M4 清理清单见 `docs/HANDOFF.md`。
- **AI 叙述层**：已落地 `analysis/ai_digest.py` + `script/gen_ai_digest.py`，导出 `report/ai_digest.md` 手动喂 LLM；AI 不进 cron（"代码拥有数字，AI 只拥有叙述"）。
- **新房去化周期**：规则已定（近 3 完整签约月均值），满 3 个完整月（预计 2027 年初）后启用。
- **面积段/区县长历史**：随 main.py 每月自动累积变厚。
- **预测**：确定性报告（trend_report）明确不做；预判只在 ai_digest 层。
