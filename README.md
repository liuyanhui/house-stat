# 北京房地产数据抓取程序

从北京市住建委网站自动抓取房地产签约数据。

[![Python](https://img.shields.io/badge/Python-3.x-blue.svg)](https://www.python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 功能

**二手房数据**：经纪机构排行、区县/面积/价格段统计、每日/月度数据、五年历史

**新房数据**：每日签约统计、五年网签数据

**特性**：智能去重、自动重试、日志记录、向后兼容

## 数据来源

北京市住建委：http://bjjs.zjw.beijing.gov.cn/eportal/ui?pageId=307749

## 安装

```bash
pip install -r requirements.txt
```

## 使用

```bash
python main.py
```

程序自动抓取数据并保存到 `data/` 目录，已存在的数据会自动跳过。

## 输出文件

**二手房**：`agency_monthly.csv`, `district_monthly.csv`, `area_monthly.csv`, `price_monthly.csv`, `resale_daily.csv`, `resale_monthly.csv`, `resale_5year.csv`

**新房**：`new_daily.csv`, `new_5year.csv`

**日志**：`log/house_stat.log`

## 注意

- CSV使用UTF-8 with BOM编码，Excel可直接打开
- 数据抓取失败时，字段值为 -1
- 可设置定时任务自动运行（Windows任务计划或Linux cron）

## 常见问题

**Q: 程序报错"未找到表格"？**  
A: 网站结构可能变化，需检查网页并修改代码

**Q: 如何查看历史数据？**  
A: 直接打开CSV文件查看所有记录

**Q: 会重复抓取吗？**  
A: 不会，程序自动跳过已存在数据

## 文件结构

```
house-stat/
├── main.py           # 主程序
├── config.py         # 配置
├── requirements.txt  # 依赖
├── data/             # 数据输出
└── log/              # 日志
```

## 更新日志

**v2.0.0** - 重构文件命名（new_*/resale_*）、新增价格段统计、新房每日数据、五年历史数据、向后兼容

**v1.0.0** - 初始版本

---

本项目仅供学习研究使用，请遵守目标网站使用条款。
