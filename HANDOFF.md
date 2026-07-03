# HANDOFF — 北京二手住宅成交趋势分析

> **清空上下文后读此文件即可继续任务。** 最后更新：2026-07-02。

## 一、项目目标

把一个原本"太简单、没深度"的北京房地产网签数据抓取项目，重构为**有深度的成交趋势分析**。

- **核心需求**：站在看北京楼市的人的角度，分析**全市和各区的成交趋势**，用**数字 + 图表 + 趋势图**呈现。
- **明确不做预测**——只呈现历史趋势，预测交给人/AI/将来。
- **可靠性第一**：自爬/官方数据才是基准；第三方历史因口径/可靠性未纳入主序列。
- 起因原话："分析感觉太简单，并且没有深度"。

## 二、如何继续（第一步）

```bash
cd house-stat
# 装依赖（见下方"环境"）
pip install -r requirements.txt
# 生成趋势报告（Markdown + HTML + PNG，输出到 report/）
python script/analyze.py --report
# 抓取最新数据（含完整性校验门，失败会非零退出）
python main.py
# 独立数据校验
python script/validate.py
```

报告产物：`report/trend_report.md`（文本）+ `report/trend_report.html`（自包含，图片 base64 内嵌，浏览器直接开）+ `report/*.png`（10 张图）。md→html 转换器是 `analysis/html_render.py`，独立转换用 `script/gen_html.py`。

## 三、环境（重要）

本项目在 **Python 3.13** 下完整可用（`requests/bs4/pandas/chinese_calendar/matplotlib`）。

**历史机器的坑（供参考，换机器可能不复现）**：原开发机 `python`=3.14，但全局 `PYTHONPATH` 指向 Python313 的 site-packages，导致 3.14 优先加载 3.13 的包；其中 matplotlib 的编译扩展是 cp313、3.14 加载不了 → 崩溃。**解决：统一用 `py -3.13` 运行**（3.13 的包原生齐全）。`run.bat` / `report.bat` 已默认 `py -3.13`。

**换新机器**：直接 `pip install -r requirements.txt` 装到任意 Python 3.10+，跑 `python script/analyze.py --report` 验证。若 matplotlib 报错，多半是版本不匹配，建 venv 隔离最稳。

## 四、架构与关键文件

```
main.py                      抓取主流程：fetch→parse→save→validate_integrity（校验门，失败非零退出）
config.py                    路径常量：DATA_DIR / LOG_DIR / REPORT_DIR
parsers/monthly_parsers.py   月度解析；parse_area_data 已修复（按表头文本定位行，勿改回位置索引）
utils/validate.py            validate_integrity：面积/价格段加总 vs 全市（阈值5%）
analysis/                    趋势分析包（本次新增的核心）
  load.py                    加载各 CSV、清洗 -1、规范区县名、load_daily()
  metrics.py                 同比/环比、移动均值、套均面积、weekly_aggregate()、区域份额/排名
  plots.py                   全部图表 + 中文字体(Microsoft YaHei)
  report.py                  组装 trend_report.md（md 生成链路，勿与 html_render 耦合）
  html_render.py             Markdown→自包含 HTML 转换器（微信胶囊主题、GFM 表格、图片 base64 内嵌）
script/analyze.py            --report 生成趋势报告（md+html+png）；无参数=原控制台文本分析
script/gen_html.py           独立把 trend_report.md 转成 trend_report.html
script/validate.py           独立校验
report/                      趋势报告输出（trend_report.md + trend_report.html + PNG，会进 git）
```

**复用约定**：`pct_change`/`normalize_district` 在 script/analyze.py；`safe_int/safe_float` 在 parsers/base_parser.py。

## 五、当前 10 张图

1. `city_trend` 全市月度（柱+12月均线+同比双轴）
2. `seasonality` 年×月热力图
3. `avg_unit_area` 月度套均面积
4. `weekly_trend` 周度成交（柱+4周均线+面积双轴；天数<7 的周标橙）
5. `weekly_avg_area` 周度套均面积连续趋势（8周均线+线性趋势）
6. `district_stacked` 各区堆叠（Top 8 + 其他）
7. `district_share` 各区份额 100%堆叠（Top 8 + 其他）
8. `district_smallmul` 各区小图
9. `area_segment_count` 面积段成交量热力图
10. `area_segment_share` 面积段占比热力图

报告章节：摘要 / 全市 / 周度 / 区域 / 市场结构 / 数据说明（含媒体参考锚点）。

## 六、当前数据状态（官方自爬，可靠）

| 文件 | 覆盖 | 说明 |
|---|---|---|
| resale_monthly | 2025-01 ~ 2026-05（17月） | 全市月度，主序列 |
| resale_daily | 2025-04-21 ~ 2026-07-01（342天） | 周度聚合的来源 |
| district_monthly | 2025-12 ~ 2026-05（6月） | 各区月度 |
| area_monthly | 2025-12, 01, 02, **03**, 05（5月） | **缺 2026-04（永久丢失，见下）** |
| price_monthly | 2026-03/04/05（3月） | 发布数据全占位，**已从报告移除** |
| resale_5year | 2020-2024 年度 | 长周期 |

## 七、关键历史与陷阱（勿重复踩）

1. **面积段解析 bug（已修）**：北京住建委 2026-04~05 间把面积表从 3 行（成交）改版成 5 行（发布+成交），旧 `parse_area_data` 写死读 `rows[1]` 误把"发布套数"当"成交套数"。修复：按表头文本定位行（`_find_row_by_label`）。**勿改回位置索引**。

2. **校验门是防线**：`validate_integrity` 校验面积/价格段加总=全市（5%）。任何新解析器/数据改动后都要跑 `script/validate.py`，不一致会非零退出——避免静默脏数据。

3. **2026-04 面积段明细永久丢失**：2026-03 已从 git 快照 `267408a`(4-30) 恢复（当时旧格式解析正确）。但 2026-04 首次被抓是 5-6（commit 396efd1），恰在解析器失效之后，所有快照里都是坏的（6632=发布套数）。官方无回溯、日数据无面积段拆分，无法恢复。全市总量 19784 仍在 district_monthly。**已写进报告数据说明。**

4. **节假日判定**：`classify_day_type`（script/analyze.py，仅控制台分析用）已改用 `chinese_calendar` 法定日历，勿用旧的"低于工作日均值15%"循环逻辑。

5. **x 轴坑**：plots.py 里凡 DataFrame 直接 `plot.area`/`plot.bar` 的，宽表 index 必须先转字符串（`_str_index`），否则 pandas 用 Period 序数当 x 坐标，与 `set_xticks(range(n))` 错位。已统一处理，新增图注意。

6. **联网补历史已调研→砍掉**：官方 pageId=307749 仅当月、无回溯；遗留 `/publicbjjs/*.asp` 全 404；公共数据开放平台只有机构排行快照。第三方口径虽一致（媒体 2025 H1 88575=自爬加总，已验证）但干净月度表锁图片/微信门，未纳入。报告附录有 2022-2025 H1 媒体锚点（标注非自爬）。

## 八、已完成阶段

- **P0 数据正确性地基**：parse_area_data 修复 + 坏数据清理 + 校验门 + classify_day_type 法定日历。全部验证通过。
- **P2 联网补历史**：调研后砍掉（见陷阱 6），改用现有可靠数据 + 媒体锚点附录。
- **P3/P4 趋势分析**：analysis/ 包、10 张图、Markdown 报告、周度统计、面积段拆成交量/占比。
- **数据恢复**：2026-03 从 git 恢复；2026-04 确认永久丢失。

## 九、未完成 / 后续可选

- **AI 叙述层**：用户暂不做。若做，建议"代码拥有数字、AI 只拥有叙述"——程序出 stats，AI 写解读，AI 不进 cron。
- **新房（new_*）联动分析**：新房数据仅 ~2 个月，待累积。
- **面积段/区县更长历史**：随 main.py 每月自动累积而变厚。
- **预测**：用户明确不做。
- **图表视觉微调**：district 两图用 Top8+其他已改善可读性；如需更细可继续调。
- **自动化测试**：目前是功能级实测 + 数字对账，无 pytest。可补 `tests/` 固化校验门与指标。

## 十、近期改动文件（git 提交参考）

- parsers/monthly_parsers.py（area 修复）
- utils/validate.py（新建）、utils/__init__.py
- main.py（校验门接入）
- analysis/（新包：load/metrics/plots/report/__init__）
- script/analyze.py（--report）、script/validate.py（新建）
- config.py（REPORT_DIR）、requirements.txt（+chinese_calendar, matplotlib）
- run.bat（py -3.13）、report.bat（新建）
- data/area_monthly.csv（清理坏数据 + 恢复 2026-03）
- README.md（v3.0.0 更新）、HANDOFF.md（本文件）
- report/*（10 图 + trend_report.md）
