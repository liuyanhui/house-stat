# -*- coding: utf-8 -*-
"""图表引擎：ChartSpec 单一来源 → 双 sink（matplotlib PNG / ECharts option JSON）。

设计 v1.1 §2.4：
- prep（T1.4b 起各图）产 ChartSpec——纯 dict（无函数；拟合/均线/份额一律 prep 算好）；
- to_matplotlib(spec) → report/<name>.png（md 阅读器/GitHub fallback，视觉对齐旧 plots.py）；
- to_echarts(spec)   → 完整 ECharts option dict（前端零模板，M3 加图零 JS 改动）；
- 序列化：NaN/Inf→None、json.dumps(allow_nan=False)——浏览器拒绝 NaN 字面量；
- 契约：PNG 与 JSON 同 stem 成对（report/xxx.png + report/charts/xxx.json），
  verify 双向存在校验；缺 JSON 自动退化 PNG。

kind 家族（覆盖现有 10 图）：
  bars_lines    柱+线，可双轴、可按正负着色（同比）、可逐柱着色（不完整周）
  lines         折线（均线/拟合虚线由 prep 作为普通 series 给出）
  stacked_area  堆叠面积（100% 份额由 prep 预计算，禁用 percent 栈）
  heatmap       矩阵热力（含单元格数值标注；value_fmt: 'int'|'pct'）
  smallmul      小倍数折线（panels，各区县独立坐标）

annotations：质量标记通道（估算日/缺月/断档），T2.2 起两 sink 同渲染，当前透传。
"""
import json
import os

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import config
from palette import PALETTE, DISTRICT_COLORS

# 中文字体 + 负号（与 plots.py 同值；plots.py 退役后此处为唯一配置点）
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['figure.dpi'] = 100
plt.rcParams['savefig.dpi'] = 110
plt.rcParams['axes.grid'] = True
plt.rcParams['grid.alpha'] = 0.3


# ---------------------------------------------------------------- 序列化（契约核心）
def _isna(v):
    return v is None or (isinstance(v, float) and (np.isnan(v) or np.isinf(v)))


def _sanitize(obj):
    """NaN/Inf→None；numpy 标量→python 标量；递归 dict/list。"""
    if obj is None or isinstance(obj, (str, bool)):
        return obj
    if isinstance(obj, (int, np.integer)):
        return int(obj)
    if isinstance(obj, (float, np.floating)):
        f = float(obj)
        return None if (np.isnan(f) or np.isinf(f)) else f
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]
    return obj


def spec_to_json_text(spec):
    """ChartSpec → 合法 JSON 文本（无 NaN/Infinity 字面量）。"""
    return json.dumps(_sanitize(spec), ensure_ascii=False, allow_nan=False)


def write_chart(spec, out_dir=None, with_png=True):
    """写 charts/<name>.json（+ <name>.png，同 stem 契约）。返回产物描述 dict。"""
    out_dir = out_dir or config.REPORT_DIR
    json_dir = os.path.join(out_dir, 'charts')
    os.makedirs(json_dir, exist_ok=True)
    with open(os.path.join(json_dir, f"{spec['name']}.json"), 'w', encoding='utf-8') as f:
        f.write(spec_to_json_text(spec))
    png = to_matplotlib(spec, out_dir) if with_png else None
    return {'png': png, 'json': f"charts/{spec['name']}.json", 'spec': spec}


def _vals(values):
    """series['values'] → float ndarray（None/NaN 统一为 nan）。"""
    return np.asarray([np.nan if _isna(v) else float(v) for v in values], dtype=float)


# ---------------------------------------------------------------- ECharts sink
def _ec_bar_data(s):
    vals = s.get('values', [])
    if s.get('sign_color'):      # 同比：按正负着色
        return [None if _isna(v) else
                {'value': v, 'itemStyle': {'color': PALETTE['up'] if v >= 0 else PALETTE['down']}}
                for v in vals]
    if s.get('colors'):          # 逐柱着色（不完整周）
        return [None if _isna(v) else {'value': v, 'itemStyle': {'color': c}}
                for v, c in zip(vals, s['colors'])]
    return _sanitize(vals)


def _ec_line(s, **extra):
    ser = {
        'type': 'line', 'name': s.get('name', ''),
        'data': _sanitize(s.get('values', [])),
        'symbol': 'circle' if s.get('marker') else 'none',
        'symbolSize': s.get('marker_size', 6),
        'lineStyle': {'width': s.get('lw', 2),
                      'type': 'dashed' if s.get('dashed') else 'solid'},
        'itemStyle': {'color': s.get('color', PALETTE['line_main'])},
        'connectNulls': False,   # 断档/缺月必须断线（需求 §5.2）
    }
    if s.get('alpha') is not None:
        ser['lineStyle']['opacity'] = s['alpha']
    ser.update(extra)
    return ser


def _ec_base(spec, tooltip_trigger):
    return {
        'title': {'text': spec.get('title', ''), 'left': 'center', 'top': 4,
                  'textStyle': {'fontSize': 14}},
        'tooltip': {'trigger': tooltip_trigger},
        'toolbox': {'feature': {'saveAsImage': {}}},
        'legend': {'top': 30},
        'grid': {'top': 64, 'left': 56, 'right': 24, 'bottom': 56,
                 'containLabel': True},
    }


def _ec_bars_lines(spec):
    opt = _ec_base(spec, 'axis')
    opt['xAxis'] = [{'type': 'category', 'data': spec['x']['labels'],
                     'axisLabel': {'rotate': spec['x'].get('rot', 0),
                                   'fontSize': spec['x'].get('fontsize', 10)},
                     'boundaryGap': spec['kind'] != 'lines'}]
    step = int(spec['x'].get('tick_step', 1))
    if step > 1:
        opt['xAxis'][0]['axisLabel']['interval'] = step - 1
    axes = [{'type': 'value', 'name': spec.get('left', {}).get('label', '')}]
    has_right = spec.get('right') or any(s.get('axis') == 'right' for s in spec['series'])
    if has_right:
        axes.append({'type': 'value', 'name': spec.get('right', {}).get('label', '')})
    opt['yAxis'] = axes
    ri = len(axes) - 1 if has_right else 0
    series = []
    for s in spec['series']:
        if s['type'] == 'bar':
            ser = {'type': 'bar', 'name': s.get('name', ''), 'data': _ec_bar_data(s),
                   'itemStyle': {'opacity': s.get('alpha', 1)}}
            if s.get('color') and not (s.get('sign_color') or s.get('colors')):
                ser['itemStyle']['color'] = s['color']   # 纯色柱不能丢（默认蓝陷阱）
            if s.get('width'):
                ser['barWidth'] = f"{s['width'] * 100}%"
            if s.get('axis') == 'right':
                ser['yAxisIndex'] = ri
            series.append(ser)
        else:
            extra = {'yAxisIndex': ri} if s.get('axis') == 'right' else {}
            series.append(_ec_line(s, **extra))
    opt['series'] = series
    opt['dataZoom'] = [{'type': 'inside'}, {'type': 'slider', 'height': 16, 'bottom': 8}]
    return opt


def _ec_stacked_area(spec):
    opt = _ec_base(spec, 'axis')
    opt['xAxis'] = [{'type': 'category', 'data': spec['x']['labels'], 'boundaryGap': False}]
    y = {'type': 'value', 'name': spec.get('left', {}).get('label', '')}
    if spec.get('y_max') is not None:
        y['max'] = spec['y_max']
    opt['yAxis'] = [y]
    series = []
    for i, s in enumerate(spec['series']):
        series.append(_ec_line(
            s, stack='total', symbol='none',
            itemStyle={'color': DISTRICT_COLORS[i % len(DISTRICT_COLORS)]},
            areaStyle={'opacity': s.get('alpha', 0.85)},
            lineStyle={'width': 0.5},
            emphasis={'focus': 'series'}))
    opt['series'] = series
    opt['dataZoom'] = [{'type': 'inside'}, {'type': 'slider', 'height': 16, 'bottom': 8}]
    return opt


def _ec_heatmap(spec):
    cols, rows = spec['x']['labels'], spec['y']['labels']
    data = [[j, i, float(v)]
            for i, row in enumerate(spec['matrix'])
            for j, v in enumerate(row) if not _isna(v)]
    vmax = spec.get('vmax')
    if vmax is None:
        vmax = max((d[2] for d in data), default=0)
    opt = _ec_base(spec, 'item')
    opt['legend'] = {'show': False}
    opt['grid'] = {'top': 56, 'left': 90, 'right': 90, 'bottom': 64, 'containLabel': False}
    opt['xAxis'] = [{'type': 'category', 'data': cols, 'splitArea': {'show': True}}]
    opt['yAxis'] = [{'type': 'category', 'data': rows, 'splitArea': {'show': True}}]
    opt['visualMap'] = {
        'min': 0, 'max': vmax, 'calculable': True,
        'orient': 'horizontal', 'left': 'center', 'bottom': 0,
        'inRange': {'color': [PALETTE['heat_low'], PALETTE['heat_high']]},
    }
    suffix = '%' if spec.get('value_fmt') == 'pct' else ''
    opt['series'] = [{
        'type': 'heatmap', 'data': data,
        'label': {'show': True, 'formatter': '{c}' + suffix},
    }]
    return opt


def _ec_smallmul(spec):
    panels = spec['panels']
    ncols = spec.get('ncols', 5)
    nrows = int(np.ceil(len(panels) / ncols))
    opt = _ec_base(spec, 'axis')
    opt['legend'] = {'show': False}
    opt['grid'] = []
    opt['xAxis'], opt['yAxis'], series = [], [], []
    w, h = 0.94 / ncols, 0.74 / nrows
    for i, p in enumerate(panels):
        r, c = divmod(i, ncols)
        left = 0.03 + c * w
        top = 0.14 + r * h
        opt['grid'].append({'left': f'{left * 100:.1f}%', 'top': f'{top * 100:.1f}%',
                            'width': f'{(w - 0.03) * 100:.1f}%', 'height': f'{(h - 0.08) * 100:.1f}%'})
        opt['xAxis'].append({'type': 'category', 'data': spec['x']['labels'],
                             'gridIndex': i, 'show': r == nrows - 1,
                             'axisLabel': {'rotate': 45, 'fontSize': 7}})
        opt['yAxis'].append({'type': 'value', 'gridIndex': i,
                             'axisLabel': {'fontSize': 7}, 'scale': True})
        series.append({'type': 'line', 'name': p.get('title', ''),
                       'xAxisIndex': i, 'yAxisIndex': i,
                       'data': _sanitize(p.get('values', [])),
                       'symbol': 'circle', 'symbolSize': 3,
                       'itemStyle': {'color': PALETTE['line_main']}, 'connectNulls': False})
    opt['series'] = series
    return opt


def to_echarts(spec):
    """ChartSpec → 完整 ECharts option dict（可直接 json 序列化）。"""
    kind = spec['kind']
    if kind in ('bars_lines', 'lines'):
        return _ec_bars_lines(spec)
    if kind == 'stacked_area':
        return _ec_stacked_area(spec)
    if kind == 'heatmap':
        return _ec_heatmap(spec)
    if kind == 'smallmul':
        return _ec_smallmul(spec)
    raise ValueError(f'未知 kind: {kind}')


# ---------------------------------------------------------------- matplotlib sink
def _mpl_xticks(ax, spec):
    labels = spec['x']['labels']
    x = np.arange(len(labels))
    step = max(1, int(spec['x'].get('tick_step', 1)))
    rot = spec['x'].get('rot', 0)
    ax.set_xticks(x[::step])
    ax.set_xticklabels(labels[::step], rotation=rot,
                       ha='right' if rot else 'center',
                       fontsize=spec['x'].get('fontsize', 9))
    return x


def _mpl_bars_lines(spec):
    has_right = spec.get('right') or any(s.get('axis') == 'right' for s in spec['series'])
    fig, ax1 = plt.subplots(figsize=spec.get('figsize', (11, 5)))
    ax2 = None
    x = np.arange(len(spec['x']['labels']))
    for s in spec['series']:
        vals = _vals(s.get('values', []))
        ax = ax1
        if s.get('axis') == 'right':
            if ax2 is None:
                ax2 = ax1.twinx()
            ax = ax2
        if s['type'] == 'bar':
            if s.get('sign_color'):
                valid = ~np.isnan(vals)
                colors = [PALETTE['up'] if v >= 0 else PALETTE['down']
                          for v in np.where(valid, vals, 0)]
                ax.bar(x[valid], vals[valid],
                       color=[c for c, ok in zip(colors, valid) if ok],
                       alpha=s.get('alpha', 1), width=s.get('width', 0.8))
            elif s.get('colors'):
                ax.bar(x, vals, color=s['colors'], alpha=s.get('alpha', 1))
            else:
                ax.bar(x, vals, color=s.get('color', PALETTE['bar_main']),
                       alpha=s.get('alpha', 1), width=s.get('width', 0.8))
        else:
            m = ~np.isnan(vals)
            if m.any():
                ax.plot(x[m], vals[m], color=s.get('color', PALETTE['line_main']),
                        marker='o' if s.get('marker') else None,
                        markersize=s.get('marker_size', 5), lw=s.get('lw', 2),
                        ls='--' if s.get('dashed') else '-',
                        alpha=s.get('alpha', 1), label=s.get('name', ''))
    _mpl_xticks(ax1, spec)
    if spec.get('left', {}).get('label'):
        ax1.set_ylabel(spec['left']['label'])
    if ax2 is not None:
        if spec.get('right', {}).get('label'):
            ax2.set_ylabel(spec['right']['label'])
        ax2.axhline(0, color='#888', lw=0.8)
    if spec.get('title'):
        ax1.set_title(spec['title'], fontsize=13, fontweight='bold')
    h, l = ax1.get_legend_handles_labels()
    if ax2 is not None:
        h2, l2 = ax2.get_legend_handles_labels()
        h, l = h + h2, l + l2
    if h:
        ax1.legend(h, l, loc='upper left', fontsize=9)
    return fig


def _mpl_lines(spec):
    fig, ax = plt.subplots(figsize=spec.get('figsize', (11, 4)))
    x = np.arange(len(spec['x']['labels']))
    for s in spec['series']:
        vals = _vals(s.get('values', []))
        m = ~np.isnan(vals)
        if m.any():
            ax.plot(x[m], vals[m], color=s.get('color', PALETTE['line_main']),
                    marker='o' if s.get('marker') else None,
                    markersize=s.get('marker_size', 5), lw=s.get('lw', 2),
                    ls='--' if s.get('dashed') else '-',
                    alpha=s.get('alpha', 1), label=s.get('name', ''))
    _mpl_xticks(ax, spec)
    if spec.get('left', {}).get('label'):
        ax.set_ylabel(spec['left']['label'])
    if spec.get('title'):
        ax.set_title(spec['title'], fontsize=13, fontweight='bold')
    if any(s.get('name') for s in spec['series']):
        ax.legend(loc='upper left', fontsize=9)
    return fig


def _mpl_stacked_area(spec):
    fig, ax = plt.subplots(figsize=spec.get('figsize', (11, 5.5)))
    labels = spec['x']['labels']
    n = len(labels)
    cum = np.zeros(n)
    for i, s in enumerate(spec['series']):
        vals = np.nan_to_num(_vals(s.get('values', [])), nan=0.0)
        color = s.get('color') or DISTRICT_COLORS[i % len(DISTRICT_COLORS)]
        ax.fill_between(range(n), cum, cum + vals, color=color,
                        alpha=s.get('alpha', 0.85), label=s.get('name', ''))
        cum += vals
    if spec.get('y_max') is not None:
        ax.set_ylim(0, spec['y_max'])
    ax.set_xticks(range(n))
    ax.set_xticklabels(labels, rotation=0)
    if spec.get('left', {}).get('label'):
        ax.set_ylabel(spec['left']['label'])
    if spec.get('title'):
        ax.set_title(spec['title'], fontsize=13, fontweight='bold')
    ax.legend(loc='center left', bbox_to_anchor=(1.01, 0.5), fontsize=8, ncol=1)
    return fig


def _mpl_heatmap(spec):
    from matplotlib.colors import LinearSegmentedColormap
    rows, cols = spec['y']['labels'], spec['x']['labels']
    mat = np.asarray([[np.nan if _isna(v) else float(v) for v in row]
                      for row in spec['matrix']], dtype=float)
    nrow, ncol = mat.shape
    fig, ax = plt.subplots(figsize=(max(5.5, 1.9 * ncol + 2.2), max(3.2, 0.8 * nrow + 1.6)))
    cmap = LinearSegmentedColormap.from_list('blues', [PALETTE['heat_low'], PALETTE['heat_high']])
    vmax = spec.get('vmax')
    if vmax is None:
        vmax = np.nanmax(mat) if not np.all(np.isnan(mat)) else 1.0
    im = ax.imshow(mat, aspect='auto', cmap=cmap, vmin=0, vmax=vmax)
    ax.set_xticks(range(ncol))
    ax.set_xticklabels(cols, rotation=0)
    ax.set_yticks(range(nrow))
    ax.set_yticklabels(rows)
    pct = spec.get('value_fmt') == 'pct'
    for i in range(nrow):
        for j in range(ncol):
            v = mat[i, j]
            if not np.isnan(v):
                text = f'{v:.1f}%' if pct else f'{int(round(v)):,}'
                ax.text(j, i, text, ha='center', va='center', fontsize=10,
                        color='white' if v > vmax * (0.55 if pct else 0.6) else '#222')
    if spec.get('title'):
        ax.set_title(spec['title'], fontsize=13, fontweight='bold')
    fig.colorbar(im, ax=ax, label=spec.get('colorbar_label', ''), shrink=0.8)
    return fig


def _mpl_smallmul(spec):
    panels = spec['panels']
    ncols = spec.get('ncols', 5)
    nrows = int(np.ceil(len(panels) / ncols))
    fig, axes = plt.subplots(nrows, ncols,
                             figsize=spec.get('figsize', (13, 2.2 * nrows)),
                             sharex=True)
    axes = np.array(axes).reshape(-1)
    labels = spec['x']['labels']
    for i, p in enumerate(panels):
        ax = axes[i]
        vals = _vals(p.get('values', []))
        ax.plot(range(len(labels)), vals, color=PALETTE['line_main'], marker='o', lw=1.5)
        ax.set_title(p.get('title', ''), fontsize=9)
        ax.tick_params(labelsize=7)
        ax.grid(alpha=0.3)
    for j in range(len(panels), len(axes)):
        axes[j].axis('off')
    for k in range(ncols):
        idx = (nrows - 1) * ncols + k
        if idx < len(axes):
            axes[idx].set_xticks(range(len(labels)))
            axes[idx].set_xticklabels(labels, rotation=45, ha='right', fontsize=7)
    if spec.get('title'):
        fig.suptitle(spec['title'], fontsize=13, fontweight='bold', y=1.0)
    return fig


_MPL = {
    'bars_lines': _mpl_bars_lines,
    'lines': _mpl_lines,
    'stacked_area': _mpl_stacked_area,
    'heatmap': _mpl_heatmap,
    'smallmul': _mpl_smallmul,
}


def to_matplotlib(spec, out_dir=None):
    """ChartSpec → PNG（文件名返回，与 JSON 同 stem）。"""
    out_dir = out_dir or config.REPORT_DIR
    os.makedirs(out_dir, exist_ok=True)
    fig = _MPL[spec['kind']](spec)
    path = os.path.join(out_dir, f"{spec['name']}.png")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return f"{spec['name']}.png"


# ---------------------------------------------------------------- preps（T1.4b 起逐图迁移）
# prep 吃 load/metrics 的 DataFrame，产 ChartSpec；拟合/均线/份额等一切派生
# 序列在此算好（双 sink 只画不算）。视觉参数与旧 plots.py 对齐，迁移期用
# 像素 diff（tests/pixel_diff.py）对照旧图，超容差人工复核记录。
def _period_labels(periods):
    """Period[M] 列表 → '25-01' 风格标签。

    注意切片：str(p)[2:5] 会带上第 4 位的 '-'（旧 plots.py 的继承 bug，标签成
    '25--01'），此处修复为 [2:4]+[5:7]；修复会引入与旧基线的标签行像素差，
    属记录在案的修复性差异。
    """
    return [f"{str(p)[2:4]}-{str(p)[5:7]}" for p in periods]


def prep_city_trend(monthly_ma):
    """chart1 全市月度：套数柱+折线+MA12（左）+ 同比正负柱（右）。"""
    df = monthly_ma.dropna(subset=['住宅签约套数']).sort_values('period')
    ma = df['MA12'].tolist() if 'MA12' in df.columns else [None] * len(df)
    return {
        'name': 'city_trend', 'kind': 'bars_lines',
        'title': '北京二手住宅 月度网签量趋势', 'figsize': (11, 5),
        'x': {'labels': _period_labels(df['period'].tolist()), 'rot': 45},
        'left': {'label': '住宅网签套数'}, 'right': {'label': '同比 %'},
        'series': [
            {'type': 'bar', 'name': '月度网签套数', 'values': df['住宅签约套数'].tolist(),
             'color': PALETTE['bar_main'], 'alpha': 0.55},
            {'type': 'line', 'name': '套数', 'values': df['住宅签约套数'].tolist(),
             'color': PALETTE['line_main'], 'marker': True, 'lw': 1.5},
            {'type': 'line', 'name': '12月移动均值', 'values': ma,
             'color': PALETTE['line_ma'], 'lw': 2.4},
            {'type': 'bar', 'name': '同比', 'values': df['同比'].tolist(), 'axis': 'right',
             'sign_color': True, 'alpha': 0.35, 'width': 0.5},
        ],
        'annotations': [],
    }


def prep_weekly_trend(weekly):
    """chart9 周度：套数柱（不完整周浅橙）+MA4（左）+ 面积万m²（右）。"""
    wk = weekly.sort_values('week_start').reset_index(drop=True)
    partial = (wk['days'] < 7)
    step = max(1, len(wk) // 12)
    return {
        'name': 'weekly_trend', 'kind': 'bars_lines',
        'title': '北京二手住宅 周度成交趋势', 'figsize': (12, 5),
        'x': {'labels': [d.strftime('%m/%d') for d in wk['week_start']],
              'rot': 45, 'fontsize': 8, 'tick_step': step},
        'left': {'label': '住宅网签套数'}, 'right': {'label': '住宅网签面积 (万m²)'},
        'series': [
            {'type': 'bar', 'name': '周度套数（橙=该周天数<7）', 'values': wk['套数'].tolist(),
             'colors': [PALETTE['bar_partial'] if p else PALETTE['bar_main'] for p in partial],
             'alpha': 0.8},
            {'type': 'line', 'name': '4周移动均值', 'values': wk['MA4'].tolist(),
             'color': PALETTE['line_ma'], 'lw': 2.2},
            {'type': 'line', 'name': '周度面积(万m²)', 'values': (wk['面积'] / 1e4).tolist(),
             'axis': 'right', 'color': PALETTE['line_aux'], 'lw': 1.3, 'alpha': 0.85},
        ],
        'annotations': [],
    }


def prep_avg_area_trend(aua):
    """chart3 套均面积：折线 + 线性趋势虚线（拟合在此算好）。"""
    df = aua.sort_values('period').reset_index(drop=True)
    x = np.arange(len(df))
    y = df['套均面积'].values
    fit = np.poly1d(np.polyfit(x, y, 1))(x) if len(df) >= 2 else np.full_like(y, np.nan)
    return {
        'name': 'avg_unit_area', 'kind': 'lines',
        'title': '套均成交面积趋势（数值↑=改善化，↓=刚需化）', 'figsize': (11, 4),
        'x': {'labels': _period_labels(df['period'].tolist()), 'rot': 45},
        'left': {'label': '套均面积 (m²)'},
        'series': [
            {'type': 'line', 'name': '套均面积', 'values': y.tolist(),
             'color': PALETTE['structure'], 'marker': True, 'lw': 2},
            {'type': 'line', 'name': '线性趋势', 'values': fit.tolist(),
             'color': PALETTE['trend_fit'], 'dashed': True, 'lw': 1.5},
        ],
        'annotations': [],
    }
