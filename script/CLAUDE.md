# script/ — 分析与校验脚本

入口脚本，编排数据校验、趋势报告生成、Markdown→HTML 转换。分析逻辑本身在 `analysis/` 包内。

## 文件说明

### `analyze.py`

趋势分析与控制台分析入口。

**用法**：
```bash
python script/analyze.py --report              # 生成趋势报告（md + html + png，写 report/）
python script/analyze.py                       # 控制台文本分析（月度/周度/工作日假期/区县/面积/同比环比）
python script/analyze.py --data-dir /path      # 指定数据目录
```

`--report` 调用 `analysis.report.generate()` 产 `report/trend_report.md` + 10 张 PNG，再调 `analysis.html_render` 产自包含 `report/trend_report.html`（图片 base64 内嵌）。无参数走原控制台逻辑（`classify_day_type` 用 `chinese_calendar` 法定日历）。

**依赖**：`pandas`、`numpy`、`matplotlib`、`chinese_calendar`（见 requirements.txt）。

### `gen_html.py`

把 `report/trend_report.md` 转成自包含 `report/trend_report.html`（图片 base64 内嵌、微信胶囊主题）。可单独运行，无需重跑全量报告。

```bash
python script/gen_html.py
python script/gen_html.py --md in.md --out out.html
```

### `validate.py`

独立数据完整性校验：逐月校验 `area_monthly`/`price_monthly` 各段成交加总 ≈ `district_monthly` 全市（阈值 5%），不一致非零退出。`main.py` 抓取后也会自动调用同一 `utils.validate_integrity`。
