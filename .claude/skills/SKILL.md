---
name: analyze-housing
description: Use when the user asks about Beijing second-hand housing transaction volumes, trends, YoY/MoM comparisons, district breakdowns, area-range analysis, or any questions referencing data/*.csv files in this project.
---

# 北京二手住宅成交量分析

## Overview

从 `data/` 目录的 CSV 文件中读取北京二手住宅签约数据，生成同比/环比、周度/月度/工作日趋势、区县和面积段分析报告。核心指标为 **住宅签约套数**。

## When to Use

- 用户提到成交量、签约量、住宅、二手房等关键词
- 用户要求趋势分析、同比环比、按周按月按日分析
- 用户问区县对比、面积段占比
- 用户说"分析数据"、"看看趋势"、"数据怎么样"
- 用户引用 data/*.csv 中的数据

## Quick Reference

| 文件 | 关键列 | 说明 |
|------|--------|------|
| daily.csv | 日期, 住宅签约套数, 住宅签约面积 | 每日签约，含工作日和周末 |
| month.csv | 月份, 住宅签约套数, 住宅签约面积(m2) | 月度汇总 |
| month_district.csv | 年月, 区县, 签约套数, 成交面积 | 按区县 |
| month_area.csv | 年月, 面积区间, 成交套数, 成交面积 | 按面积段 |

## Data Loading Rules

**所有 CSV 读取必须指定 `encoding='utf-8-sig'`**。执行 Python 时用 `PYTHONIOENCODING=utf-8`。

### 关键陷阱

- **month.csv 列名** `住宅签约面积(m2)` 带括号后缀，不是 `住宅签约面积`
- **月份格式混合** `"2025-1"` 和 `"2026-01"` 并存，用 `format='mixed'` 或先 str 再 parse
- **区县名含全角空格** 如 `"全　市"`，匹配前须 `.str.replace(r'\s+', '', regex=True)` 去空格
- **district/area 文件可能不存在**，加载前检查 `os.path.exists()`，缺失时跳过对应章节

## Analysis Framework

用户未指定方向时覆盖全部 6 个维度：

1. **月度概览** — 当年每月住宅签约套数、同比(去年同月)、环比(上月)
2. **周度趋势** — ISO 周分组，周总量/日均/周环比
3. **工作日分析** — 区分工作日/周末/假期，工作日日均及月度趋势
4. **区县对比** — 最新月各区县成交量及环比，Top 3
5. **面积段分析** — 各面积段占比及环比变化
6. **关键发现** — 自动提炼趋势、异常值、拐点

### Holiday Detection

**不要硬编码节假日。** 数据驱动识别：工作日成交量 < 当月工作日均值 15%（最低 5 套）→ 判定为假期。任何年份自动适应。

规律：假期成交接近零，周末约为工作日 1/3~1/4。

## Output Format

- 中文，报告头部标注数据日期范围
- `=` / `-` 分隔章节，表格对齐，数字千位分隔符
- 百分比用 `+X.X%` / `-X.X%`

## Flexibility

- 用户问具体维度时聚焦回答，不必覆盖全部
- 支持追问（"朝阳最近怎样"、"小户型占比变了没"）
- 异常值（如春节零成交）要主动解释

## Common Mistakes

| 错误 | 正确做法 |
|------|----------|
| 忘记 `encoding='utf-8-sig'` | 始终指定，否则中文列名乱码 |
| 用 `住宅签约面积` 访问 month.csv | 实际列名是 `住宅签约面积(m2)`，用子串匹配 |
| 硬编码节假日列表 | 用数据驱动：成交量低于阈值的工作日 = 假期 |
| 直接用 `'全　市'` 匹配区县 | 全角空格不可靠，用正则去空格后比较 |
| 把春节零成交日当工作日计算日均 | 先 classify_day_type 排除假期，再算工作日均值 |
