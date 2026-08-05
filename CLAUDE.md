# house-stat — 北京房地产网签数据

从北京市住建委抓取网签数据，并做**二手住宅成交趋势分析**（全市 + 各区，数字 + 图表）。**确定性代码报告不含预测**（只摆事实）；**AI 分析层（ai_digest）可做预判**，按资深分析师纪律。

> 各目录另有 `CLAUDE.md` 说明本目录模块。

## 常用命令

```bash
pip install -r requirements.txt
python main.py                      # 抓取最新数据（含完整性校验门，失败非零退出）
python script/analyze.py --report   # 生成趋势报告（report/ 下 md + html + png）
python script/validate.py           # 独立数据完整性校验
python script/gen_html.py           # 单独把 trend_report.md 转成自包含 html
python script/gen_ai_digest.py      # 导出 AI 客观分析 digest+prompt（report/ai_digest.md，手动喂 LLM）
```

> **Python 版本**：默认用 **Python 3.14**（`py -3.14`），它带原生 cp314 wheel（numpy/pandas/matplotlib/PIL/chinese_calendar 等）。关键陷阱：本机全局 `PYTHONPATH` 指向 `D:\liuyh\software\Python\Python313\Lib\site-packages`，会排在 3.14 `sys.path` 最前，用 cp313 的 numpy 遮蔽 3.14 原生包导致 import 崩。**故 `run.bat`/`report.bat` 在调用前都 `set "PYTHONPATH="` 清空**。直接跑脚本同理：`PYTHONPATH= py -3.14 script/analyze.py --report`。换干净机器（无该 PYTHONPATH）直接 `py -3.14` 即可；matplotlib/numpy 报错先检查 PYTHONPATH 是否被污染，再考虑建 venv。

## 数据流

```
main.py → fetcher → parsers → storage(去重写 data/*.csv) → validate_integrity(校验门)
script/analyze.py --report → analysis.load → metrics → plots(PNG) → report(md) → html_render(html)
```

## 目录

| 目录 | 作用 | 入 git |
|------|------|--------|
| `data/` | 官方自爬 CSV（可靠基准，逐月累积） | ✓ |
| `parsers/` | HTML→DataFrame 解析器 | ✓ |
| `utils/` | 抓取/存储/校验/日志/目录 | ✓ |
| `analysis/` | 趋势分析包（load/metrics/plots/report/html_render/ai_digest） | ✓ |
| `script/` | 入口脚本（analyze/gen_html/gen_ai_digest/validate） | ✓ |
| `report/` | 报告产物（md+html+png，代码生成） | ✗ 已忽略，每次重生 |
| `log/` | 运行日志 | ✗ 已忽略 |

## 数据覆盖状态（截至 2026-08-05，官方自爬）

| 文件 | 覆盖 | 说明 |
|---|---|---|
| resale_monthly | 2025-01 ~ 2026-07（19 月） | 全市月度，主序列 |
| resale_daily | 2025-04-21 ~ 2026-08-04 | 周度聚合来源；2026-07-25/26 为估算（见约定 8） |
| district_monthly | 2025-12 ~ 2026-07 | 各区月度 |
| area_monthly | 2025-12 ~ 2026-07，缺 2026-04 | 2026-04 永久丢失（见约定 7） |
| price_monthly | 2026-03 ~ 2026-07 | 发布数据全占位，已从报告移除 |
| agency_monthly | 2025-12 ~ 2026-04 | 经纪机构排行 |
| new_daily | 2026-04-29 ~ 2026-08-04 | 商品房每日 |
| resale_5year / new_5year | 2020 ~ 2024 年度 | 长周期 |

## 关键约定（勿踩）

1. **数据可靠性优先**：自爬/官方是基准；第三方历史未纳入主序列（口径/可靠性，媒体历史多锁图片/微信门）。
2. **校验门是防线**：改解析器或数据后必跑 `script/validate.py`，面积/价格段加总必须 ≈ 全市（5%）。
3. **`parse_area_data` 按表头文本定位行**，勿改回位置索引（曾因页面改版把"发布套数"误存为"成交套数"）。
4. **plots 宽表 index 先转字符串**（`_str_index`），否则 x 轴 Period 序数错位。
5. **节假日用 `chinese_calendar`**，勿用旧的"低于工作日均值 15%"循环逻辑。
6. **预测分层**：确定性代码报告（trend_report）不含预测，仅历史事实；**AI 分析层（ai_digest→AI）可做预判**，但按资深分析师纪律——区分事实/判断、短 horizon（1–3 月）、情景化、给置信度+证伪条件，预判非事实非定论、不含价格、不作买卖建议。
7. 面积段 2026-04 永久丢失（解析器时间窗口 bug，已记入报告数据说明）。
8. **2026-07-25/26 日数据为月度回推估算**（官方故障缺失，非真实观测）：按 `(7月月度总量 − 7月已有日度之和) ÷ 2` 补全——签约 154/154、住宅 147/146（293 奇数，147 给周六 25、146 给周日 26）、签约面积 13715.98×2、住宅面积 13429.49×2。补全后 7 月日度和与月度精确相等（15910 / 14037）。序列可连续使用，但引用单日值须注明为估算。

## 路线图

- **AI 叙述层**：已落地 `analysis/ai_digest.py` + `script/gen_ai_digest.py`，导出 `report/ai_digest.md` 手动喂 LLM；AI 不进 cron（"代码拥有数字，AI 只拥有叙述"）。
- **新房（new_*）联动分析**：new_daily 仅 ~3 个月，待累积。
- **面积段/区县长历史**：随 main.py 每月自动累积变厚。
- **预测**：确定性报告（trend_report）明确不做；预判只在 ai_digest 层。
- **自动化测试**：目前是功能级实测 + 数字对账，无 pytest，可补 `tests/`。
