# -*- coding: utf-8 -*-
"""调色板单一来源（零依赖，设计 v1.1 §2.4 / 前端评审：三处样式分立必漂移）。

消费方：analysis/charts.py（matplotlib + ECharts）、analysis/html_render.py（THEME）。
颜色为 CSS 十六进制字符串，matplotlib/ECharts 均可直接使用。
"""

PALETTE = {
    # 序列色（与旧 plots.py 视觉一致，迁移期像素 diff 容差内）
    'bar_main': '#9ecae1',      # 套数柱（浅蓝）
    'bar_partial': '#fdd0a2',   # 不完整周柱（浅橙）
    'line_main': '#08519c',     # 主折线（深蓝）
    'line_ma': '#de2d26',       # 移动均线（红）
    'line_aux': '#31a354',      # 面积线（绿，右轴）
    'up': '#31a354',            # 同比上涨（绿）
    'down': '#e6550d',          # 同比下跌（橙）
    'structure': '#6a51a3',     # 套均面积（紫）
    'trend_fit': '#e7298a',     # 拟合/均线辅助（品红）
    'muted': '#999999',         # 次要虚线（灰）
    # 热力图渐变（低→高）
    'heat_low': '#f7fbff',
    'heat_high': '#08306b',
}

# 区县/多序列循环色（取自 matplotlib tab20，17+ 区足够区分；与旧图一致）
DISTRICT_COLORS = [
    '#1f77b4', '#aec7e8', '#ff7f0e', '#ffbb78', '#2ca02c', '#98df8a',
    '#d62728', '#ff9896', '#9467bd', '#c5b0d5', '#8c564b', '#c49c94',
    '#e377c2', '#f7b6d2', '#7f7f7f', '#c7c7c7', '#bcbd22', '#dbdb8d',
    '#17becf', '#9edae5',
]
