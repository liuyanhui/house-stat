# -*- coding: utf-8 -*-
"""数据加载与标准化

读取 data/ 下各 CSV，解析日期、清洗 -1 占位、规范区县名，
返回各分析模块可直接使用的 DataFrame。

所有数据均为官方自爬（北京市住建委 pageId=307749），口径一致、可靠。
"""
import os
import pandas as pd
import numpy as np
import config

ENC = config.CSV_ENCODING


def _read(name):
    path = os.path.join(config.DATA_DIR, name)
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path, encoding=ENC)


def _norm_district(name):
    """统一区县名：去全角/半角空格。"""
    return str(name).strip().replace('　', '').replace(' ', '')


def _to_period_month(s):
    """把 '2025-1' / '2025-12' / '2025-01' 统一成 Period('2025-01')。"""
    return pd.to_datetime(s.astype(str).str.strip(), format='%Y-%m').dt.to_period('M')


def load_monthly():
    """全市月度网签。返回按月升序的 DataFrame，含 year/month/period。"""
    df = _read('resale_monthly.csv')
    if df.empty:
        return df
    df = df.copy()
    df['period'] = _to_period_month(df['月份'])
    dt = df['period'].dt.start_time
    df['year'] = dt.dt.year
    df['month'] = dt.dt.month
    df = df.sort_values('period').reset_index(drop=True)
    return df


def load_annual():
    """近五年年度住宅成交（官方）。"""
    return _read('resale_5year.csv')


def load_district():
    """各区县月度签约。返回不含"全市"行的明细，并附 area=成交面积。"""
    df = _read('district_monthly.csv')
    if df.empty:
        return df
    df = df.copy()
    df['period'] = _to_period_month(df['年月'])
    df['区县_clean'] = df['区县'].apply(_norm_district)
    # 仅保留有效数据（-1 为抓取失败占位）
    df = df[df['签约套数'] > 0].copy()
    return df


def load_area():
    """面积段月度成交。"""
    df = _read('area_monthly.csv')
    if df.empty:
        return df
    df = df.copy()
    df['period'] = _to_period_month(df['年月'])
    df = df[df['成交套数'] > 0].copy()
    return df


def load_price():
    """价格段月度成交（发布套数在近月为 0/占位，主用成交套数）。"""
    df = _read('price_monthly.csv')
    if df.empty:
        return df
    df = df.copy()
    df['period'] = _to_period_month(df['年月'])
    df = df[df['成交套数'] > 0].copy()
    return df


def load_daily():
    """每日存量房网签。返回按日期升序、住宅签约套数有效的 DataFrame。

    用于按周聚合（成交量 + 成交面积）。-1 为抓取失败占位，已剔除。
    """
    df = _read('resale_daily.csv')
    if df.empty:
        return df
    df = df.copy()
    df['日期'] = pd.to_datetime(df['日期'], format='mixed')
    df = df[df['住宅签约套数'] > 0].sort_values('日期').reset_index(drop=True)
    return df


def city_district_row(district_df, period):
    """从 district 明细取某月的"全市"行签约套数。"""
    if district_df.empty:
        return None
    row = district_df[(district_df['period'] == period) & (district_df['区县_clean'] == '全市')]
    if row.empty:
        return None
    return int(row['签约套数'].iloc[0])
