# script/ — 分析脚本

存放对已抓取数据进行分析和可视化的脚本。

## 文件说明

### `analyze.py`

北京二手房成交量综合分析脚本。

**用法**：
```bash
python script/analyze.py                    # 使用默认 data/ 目录
python script/analyze.py --data-dir /path   # 指定数据目录
```

**数据来源**：
- `resale_daily.csv` — 每日签约数据
- `resale_monthly.csv` — 月度汇总数据
- `district_monthly.csv` — 区县分布数据
- `area_monthly.csv` — 面积区间数据

**分析内容**：
- 月度概览：签约套数/面积趋势
- 周度分析：按周统计成交量
- 工作日 vs 休息日对比
- 区县签约分布排名
- 面积区间分布统计
- 同比/环比趋势分析

**依赖**：`pandas`, `numpy`（已包含在项目 requirements.txt 中）
