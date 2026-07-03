# house-stat — 北京房地产网签数据

从北京市住建委抓取网签数据，并做**二手住宅成交趋势分析**（全市 + 各区，数字 + 图表，**不含预测**）。

> 续任务/交接先读 `HANDOFF.md`（目标、进度、陷阱、如何继续）。各目录另有 `CLAUDE.md` 说明本目录模块。

## 常用命令

```bash
pip install -r requirements.txt
python main.py                      # 抓取最新数据（含完整性校验门，失败非零退出）
python script/analyze.py --report   # 生成趋势报告（report/ 下 md + html + png）
python script/validate.py           # 独立数据完整性校验
python script/gen_html.py           # 单独把 trend_report.md 转成自包含 html
```

> **Python 版本**：依赖在 Python 3.13 下完整可用（matplotlib 等）。原开发机因全局 PYTHONPATH 把 3.13 包装进 3.14 导致 matplotlib 崩，故 `run.bat`/`report.bat` 默认 `py -3.13`。换机器装好 requirements 即可，matplotlib 报错就建 venv。

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
| `analysis/` | 趋势分析包（load/metrics/plots/report/html_render） | ✓ |
| `script/` | 入口脚本（analyze/gen_html/validate） | ✓ |
| `report/` | 报告产物（md+html+png，代码生成） | ✗ 已忽略，每次重生 |
| `log/` | 运行日志 | ✗ 已忽略 |

## 关键约定（勿踩）

1. **数据可靠性优先**：自爬/官方是基准；第三方历史未纳入主序列（口径/可靠性，见 HANDOFF.md）。
2. **校验门是防线**：改解析器或数据后必跑 `script/validate.py`，面积/价格段加总必须 ≈ 全市（5%）。
3. **`parse_area_data` 按表头文本定位行**，勿改回位置索引（曾因页面改版把"发布套数"误存为"成交套数"）。
4. **plots 宽表 index 先转字符串**（`_str_index`），否则 x 轴 Period 序数错位。
5. **节假日用 `chinese_calendar`**，勿用旧的"低于工作日均值 15%"循环逻辑。
6. **报告不含预测**，仅历史趋势。
7. 面积段 2026-04 永久丢失（解析器时间窗口 bug，已记入报告数据说明）。
