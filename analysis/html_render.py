# -*- coding: utf-8 -*-
"""Markdown → 自包含 HTML 转换器（趋势报告用）。

移植 refined-stock/scripts/gen-html.mjs 的 inline/convert 逻辑到 Python，
扩展两类该参考未支持、但本报告用到的元素：
  - GFM 表格（| a | b | / | --- |）→ 带样式 <table>，外层横向滚动
  - 图片 ![alt](path.png) → base64 内嵌 <img>，输出单文件自包含

主题：微信胶囊风格（胶囊 h2、左条 callout h3、强调色 strong、中文字体栈），
集中在一个 THEME 常量。独立 HTML 用 <style> 块（不发布到微信，无需全 inline）。

零第三方依赖（仅 stdlib）。内容稳定、确定性输出。

图表注入（T1.6）：charts_dir 下存在与 md 图片同 stem 的 charts/<stem>.json
（analysis/charts.py 产出：{name, kind, height, option}）且 vendor 可用时，
<img> 被替换为占位 <div>，文末内联 vendor + 图表数据 + bootstrap（懒 init）。
DASHBOARD_NO_JS=1 或 vendor 缺失 → 纯 PNG 模式（与旧版一致）。
"""
import base64
import html as _html
import json
import os
import re

# --- 主题：所有颜色/字体/样式字面量集中于此 -------------------------------
THEME = {
    'accent': '#009874',          # 强调色（胶囊 h2 底、h3 左条、strong、表头）
    'accent_light': '#f3faf8',    # h3/引用 的浅底
    'link': '#576b95',            # 微信原生链接色
    'fg': '#3f3f3f',
    'muted': '#9a9a9a',
    'border': '#e1e4e8',
    'bg': '#ffffff',
    'code_bg': '#f6f8fa',
    'font_stack': ('-apple-system,BlinkMacSystemFont,"Helvetica Neue",'
                   '"PingFang SC","Hiragino Sans GB","Microsoft YaHei UI",'
                   '"Microsoft YaHei",Arial,sans-serif'),
    'code_font': 'ui-monospace,SFMono-Regular,Menlo,Consolas,monospace',
}


def _stylesheet(t=THEME):
    return f"""
:root {{
  --accent:{t['accent']}; --accent-light:{t['accent_light']}; --link:{t['link']};
  --fg:{t['fg']}; --muted:{t['muted']}; --border:{t['border']};
  --bg:{t['bg']}; --code-bg:{t['code_bg']};
}}
* {{ box-sizing: border-box; }}
body {{
  margin:0; color:var(--fg); background:#f4f6f8;
  font:16px/1.8 {t['font_stack']};
  -webkit-text-size-adjust:100%;
}}
.container {{ max-width:880px; margin:0 auto; padding:32px 20px 80px; background:var(--bg);
  box-shadow:0 1px 4px rgba(0,0,0,0.04); }}
h1 {{ font-size:26px; line-height:1.4; margin:0 0 16px; color:var(--accent);
  font-weight:bold; }}
/* h2：居中胶囊 */
h2 {{ display:table; margin:2.6em auto 1.8em; color:#fff; background:var(--accent);
  font-weight:bold; text-align:center; padding:0.32em 1.3em; font-size:21px;
  border-radius:8px 24px 8px 24px; box-shadow:0 2px 6px rgba(0,0,0,0.06); }}
/* h3：左条 callout */
h3 {{ margin:2em 0 0.9em; color:var(--fg); font-weight:bold; padding:0.5em 14px;
  font-size:18px; border-radius:6px; border-left:4px solid var(--accent);
  background:var(--accent-light); }}
h4 {{ margin:1.6em 0 0.6em; font-size:16px; font-weight:bold; color:var(--fg); }}
p {{ margin:1.2em 0; }}
ul,ol {{ margin:1.2em 0; padding-left:1.6em; }}
li {{ margin:0.45em 0; }}
li > ul, li > ol {{ margin:0.3em 0 0; }}
a {{ color:var(--link); text-decoration:none; }}
a:hover {{ text-decoration:underline; }}
strong {{ color:var(--accent); font-weight:bold; }}
em {{ font-style:normal; color:var(--muted); }}
small {{ display:block; color:var(--muted); font-size:13px; margin:0.8em 0; }}
code {{ background:var(--code-bg); border-radius:4px; padding:2px 6px; font-size:14px;
  font-family:{t['code_font']}; }}
pre {{ background:var(--code-bg); border:1px solid var(--border); border-radius:8px;
  padding:14px 16px; overflow:auto; margin:1.2em 0; }}
pre code {{ background:none; padding:0; }}
blockquote {{ margin:1.2em 0; padding:0.5em 16px; color:var(--fg);
  border-left:4px solid var(--accent); background:var(--accent-light);
  border-radius:0 6px 6px 0; }}
blockquote p {{ margin:0.4em 0; }}
hr {{ border:0; border-top:1px solid var(--border); margin:2em 0; }}
img {{ max-width:100%; height:auto; display:block; margin:1.2em auto;
  border:1px solid var(--border); border-radius:6px; }}
/* 表格：表头强调色浅底、斑马纹、横向滚动 */
.table-wrap {{ overflow-x:auto; margin:1.2em 0; }}
table {{ border-collapse:collapse; width:100%; font-size:14px; }}
th,td {{ border:1px solid var(--border); padding:7px 11px; text-align:left;
  white-space:nowrap; }}
th {{ background:var(--accent-light); color:var(--fg); font-weight:bold;
  border-bottom:2px solid var(--accent); }}
tbody tr:nth-child(even) {{ background:#fafbfc; }}
tbody tr:hover {{ background:var(--accent-light); }}
/* 可排序表：纵向滚动 + 表头吸顶（sticky 须在滚动容器内才生效） */
.table-wrap.sortable {{ max-height:520px; overflow:auto; }}
.table-wrap.sortable thead th {{ position:sticky; top:0; z-index:1; cursor:pointer; }}
.table-wrap.sortable thead th:hover {{ filter:brightness(0.96); }}
/* 图表容器（ECharts 注入位，视觉与 img 一致） */
.chart {{ width:100%; margin:1.2em auto; border:1px solid var(--border);
  border-radius:6px; box-sizing:border-box; }}
/* 目录（由 toc 占位注释渲染，勿在注释里写字面占位符） */
.toc {{ margin:1.2em 0; padding:0.6em 1.2em; background:var(--accent-light);
  border:1px solid var(--border); border-radius:8px; font-size:14px; }}
.toc ul {{ margin:0.4em 0; padding-left:1.2em; }}
.toc li {{ margin:0.25em 0; }}
.toc li.lvl3 {{ padding-left:1.2em; }}
/* KPI 卡（由 kpi 占位注释渲染，kpi.json 构建期静态产出） */
.kpi-grid {{ display:flex; gap:14px; flex-wrap:wrap; margin:1.2em 0; }}
.kpi-card {{ flex:1; min-width:220px; border:1px solid var(--border);
  border-left:4px solid var(--accent); border-radius:8px; padding:12px 16px;
  background:var(--accent-light); }}
.kpi-card.kpi-mtd {{ border-left-color:var(--muted); }}
.kpi-label {{ font-size:13px; color:var(--muted); }}
.kpi-value {{ font-size:26px; font-weight:bold; color:var(--accent); line-height:1.5; }}
.kpi-card.kpi-mtd .kpi-value {{ color:var(--fg); }}
.kpi-unit {{ font-size:14px; }}
.kpi-sub {{ font-size:13px; }}
@media (max-width:640px) {{
  .container {{ padding:20px 14px 60px; }}
  th,td {{ white-space:normal; }}
}}
"""


# --- 内联 markdown → HTML（移植参考 inline） -----------------------------
def _inline(text):
    # 先抽走 `code`，避免被后续转义/强调干扰
    codes = []

    def stash_code(m):
        codes.append(_html.escape(m.group(1)))
        return f"\x00C{len(codes) - 1}\x00"

    text = re.sub(r"`([^`]+)`", stash_code, text)

    text = _html.escape(text, quote=False)
    # 链接 [t](u)
    text = re.sub(r"\[([^\]]+)\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)",
                  lambda m: f'<a href="{_html.escape(m.group(2))}">{m.group(1)}</a>', text)
    # 裸 URL 自动链接
    text = re.sub(r"(^|\s)(https?://[^\s<)]+)",
                  lambda m: f'{m.group(1)}<a href="{_html.escape(m.group(2))}">{_html.escape(m.group(2))}</a>',
                  text, flags=re.M)
    # 图片占位（在块级处理时已替换为 <img>；此处兜底防止 ** 误伤）
    # 粗体/斜体
    text = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"\*([^\s*][^*]*?)\*", r"<em>\1</em>", text)
    # 还原 code
    text = re.sub(r"\x00C(\d+)\x00", lambda m: f"<code>{codes[int(m.group(1))]}</code>", text)
    return text


# --- 图片 → base64 内嵌 --------------------------------------------------
_IMG_RE = re.compile(r"!\[([^\]]*)\]\(([^)\s]+)\)")


def _img_tag(alt, src, image_dir, charts_ctx=None):
    """![alt](rel.png)：

    交互模式（charts_ctx 非空）且存在同 stem 的 charts/<stem>.json → 返回图表
    占位 <div>（option 注册进 charts_ctx，文末统一注入数据与 bootstrap）；
    否则 base64 内嵌 <img>；读不到图片则回退原样引用。
    """
    if charts_ctx is not None:
        stem = os.path.splitext(os.path.basename(src))[0]
        cjson = os.path.join(charts_ctx['dir'], stem + '.json')
        if os.path.isfile(cjson):
            try:
                with open(cjson, encoding='utf-8') as f:
                    payload = json.load(f)
                option = payload.get('option')
                if option:  # 缺 option 键/空 option → 视为坏 JSON 走 PNG（M2 修复）
                    charts_ctx['found'][stem] = option
                    h = int(payload.get('height') or 400)
                    return (f'<div class="chart" data-chart="{_html.escape(stem)}" '
                            f'style="height:{h}px"></div>')
            except (OSError, ValueError):
                pass  # JSON 损坏 → 走 PNG 兜底（设计：缺 json/坏 json 自动退化）
    path = src
    if image_dir and not os.path.isabs(src):
        path = os.path.join(image_dir, src)
    data_uri = None
    if os.path.isfile(path):
        ext = os.path.splitext(path)[1].lower().lstrip('.')
        mime = {'png': 'image/png', 'jpg': 'image/jpeg', 'jpeg': 'image/jpeg',
                'gif': 'image/gif', 'svg': 'image/svg+xml', 'webp': 'image/webp'}.get(ext, 'image/png')
        with open(path, 'rb') as f:
            data_uri = f"data:{mime};base64,{base64.b64encode(f.read()).decode('ascii')}"
    src_attr = data_uri or _html.escape(src)
    alt_attr = _html.escape(alt) if alt else ''
    return f'<img src="{src_attr}" alt="{alt_attr}">'


# --- 表格行解析 -----------------------------------------------------------
def _is_table_block(buf):
    """buf 为连续非空行；判断是否 GFM 表格（>=2 行，第 2 行是 | --- | 分隔）。"""
    if len(buf) < 2:
        return False
    cells = [c.strip() for c in buf[0].strip().strip('|').split('|')]
    sep = buf[1].strip().strip('|')
    return bool(re.match(r"^\|?[\s:|-]+$", buf[1].strip())) and '|' in buf[1] and len(cells) >= 1


def _cell_key(text):
    """显示串 → 可排序数值（Python 侧预计算，JS 零计算）。

    支持本项目格式：千分位/百分号/pp/⚠️ 后缀、+/- 前缀、'YYYY-MM[-DD]' 日期、
    '—'/'N/A' → None（排序置尾）。解析不了（如区县名、区间段）→ None。
    """
    s = str(text).strip()
    for ch in (',', '%', 'pp', '⚠️', '↑', '↓', ' '):
        s = s.replace(ch, '')
    if s in ('', '—', '-', 'N/A', 'None', '…'):
        return None
    try:
        return float(s)
    except ValueError:
        pass
    m = re.match(r'^(\d{4})-(\d{1,2})(?:-(\d{1,2}))?$', s)
    if m:
        return float(f"{m.group(1)}{int(m.group(2)):02d}{int(m.group(3) or 0):02d}")
    return None


def _fmt_key(v):
    return str(int(v)) if float(v).is_integer() else repr(v)


_SORT_JS = """(function () {
  document.querySelectorAll('.table-wrap.sortable table').forEach(function (t) {
    var ths = Array.prototype.slice.call(t.querySelectorAll('thead th'));
    ths.forEach(function (h, ci) {
      h.addEventListener('click', function () {
        var asc = h.getAttribute('data-dir') !== 'asc';
        ths.forEach(function (x) {
          x.removeAttribute('data-dir');
          x.textContent = x.textContent.replace(/ [\\u2191\\u2193]$/, '');
        });
        h.setAttribute('data-dir', asc ? 'asc' : 'desc');
        h.textContent += asc ? ' \\u2191' : ' \\u2193';
        var tbody = t.querySelector('tbody');
        var rows = Array.prototype.slice.call(tbody.querySelectorAll('tr'));
        rows.sort(function (a, b) {
          var va = a.children[ci].getAttribute('data-v');
          var vb = b.children[ci].getAttribute('data-v');
          va = va === null ? null : parseFloat(va);
          vb = vb === null ? null : parseFloat(vb);
          if (va === null && vb === null) return 0;
          if (va === null) return 1;      /* 缺失/NaN 置尾 */
          if (vb === null) return -1;
          return asc ? va - vb : vb - va;
        });
        rows.forEach(function (r) { tbody.appendChild(r); });
      });
    });
  });
})();"""


def _table_html(buf):
    def cells(line):
        return [c.strip() for c in line.strip().strip('|').split('|')]

    header = cells(buf[0])
    rows = [cells(r) for r in buf[2:]]
    keyed = False
    out = ['<div class="table-wrap sortable"><table>', '<thead><tr>']
    out += [f"<th>{_inline(h)}</th>" for h in header]
    out += ['</tr></thead>', '<tbody>']
    for r in rows:
        out.append('<tr>')
        ncol = len(header)
        for i in range(ncol):
            cell = _inline(r[i]) if i < len(r) else ''
            attr = ''
            k = _cell_key(re.sub(r'<[^>]+>', '', cell))
            if k is not None:
                attr = f' data-v="{_fmt_key(k)}"'
                keyed = True
            out.append(f'<td{attr}>{cell}</td>')
        out.append('</tr>')
    out.append('</tbody></table></div>')
    if not keyed:
        out[0] = '<div class="table-wrap"><table>'
    return '\n'.join(out)


# --- 块级 markdown → HTML（移植参考 convert + 表格/图片扩展） -------------
def _convert(md, image_dir, charts_ctx=None):
    lines = re.sub(r"\r\n?", "\n", md).split("\n")
    out = []
    para = []
    img_stash = []   # 行内图片/图表标签暂存：先占位，_inline 转义后再还原（防注入标签被转义）

    def flush_para():
        if para:
            text = _inline(' '.join(para))
            text = re.sub(r"\x00I(\d+)\x00",
                          lambda m: img_stash[int(m.group(1))], text)
            out.append(f"<p>{text}</p>")
            para.clear()

    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]

        # 围栏代码块
        if re.match(r"^```", line):
            flush_para()
            buf = []
            i += 1
            while i < n and not re.match(r"^```", lines[i]):
                buf.append(lines[i])
                i += 1
            i += 1  # 吃掉闭合围栏（若有）
            out.append(f'<pre><code>{_html.escape(chr(10).join(buf))}</code></pre>')
            continue

        # HTML 注释占位符直通（<!-- toc --> / <!-- kpi -->，单行，不转义不渲染）
        if re.match(r"^<!--.*-->\s*$", line.strip()) and '<!--' == line.strip()[:4]:
            flush_para()
            out.append(line.strip())
            i += 1
            continue

        # ATX 标题
        h = re.match(r"^(#{1,6})\s+(.+?)\s*#*\s*$", line)
        if h:
            flush_para()
            lvl = len(h.group(1))
            out.append(f"<h{lvl}>{_inline(h.group(2))}</h{lvl}>")
            i += 1
            continue

        # 水平分隔线
        if re.match(r"^\s*([-*_])(\s*\1){2,}\s*$", line):
            flush_para()
            out.append("<hr>")
            i += 1
            continue

        # 空行
        if re.match(r"^\s*$", line):
            flush_para()
            i += 1
            continue

        # 块引用
        if re.match(r"^>\s?", line):
            flush_para()
            buf = []
            while i < n and re.match(r"^>\s?", lines[i]):
                buf.append(re.sub(r"^>\s?", "", lines[i]))
                i += 1
            out.append(f"<blockquote>{_convert(chr(10).join(buf), image_dir, charts_ctx)}</blockquote>")
            continue

        # 表格：聚合连续非空行，判断是否表格块
        if '|' in line and not re.match(r"^\s{0,3}[-*+]\s", line):
            buf = []
            while i < n and re.match(r"^\s*$", lines[i]) is None and not re.match(r"^```", lines[i]) \
                    and not re.match(r"^#{1,6}\s", lines[i]):
                # 列表项/引用属下一块，停止聚合
                if re.match(r"^\s{0,3}([-*+]|\d+\.)\s", lines[i]) or re.match(r"^>\s?", lines[i]):
                    break
                buf.append(lines[i])
                i += 1
            if _is_table_block(buf):
                flush_para()
                out.append(_table_html(buf))
                continue
            # 不是表格则当作普通段落
            para.extend(b.strip() for b in buf)
            continue

        # 无序列表
        if re.match(r"^\s{0,3}[-*+]\s+", line):
            flush_para()
            items = []
            while i < n:
                m = re.match(r"^\s{0,3}[-*+]\s+(.*)$", lines[i])
                if m:
                    items.append(m.group(1))
                    i += 1
                    continue
                if re.match(r"^\s{2,}\S", lines[i]) and items:
                    items[-1] += " " + lines[i].strip()
                    i += 1
                    continue
                break
            out.append("<ul>\n" + "\n".join(f"  <li>{_inline(it)}</li>" for it in items) + "\n</ul>")
            continue

        # 有序列表
        if re.match(r"^\s{0,3}\d+\.\s+", line):
            flush_para()
            items = []
            while i < n:
                m = re.match(r"^\s{0,3}\d+\.\s+(.*)$", lines[i])
                if m:
                    items.append(m.group(1))
                    i += 1
                    continue
                if re.match(r"^\s{2,}\S", lines[i]) and items:
                    items[-1] += " " + lines[i].strip()
                    i += 1
                    continue
                break
            out.append("<ol>\n" + "\n".join(f"  <li>{_inline(it)}</li>" for it in items) + "\n</ol>")
            continue

        # 图片独占行（避免被并入段落）
        if _IMG_RE.match(line.strip()):
            flush_para()
            out.append(_img_tag(_IMG_RE.match(line.strip()).group(1),
                                _IMG_RE.match(line.strip()).group(2), image_dir, charts_ctx))
            i += 1
            continue

        # 普通文本行 → 累积成段落（行内图片/图表标签暂存，转义后还原）
        def _stash_img(m):
            img_stash.append(_img_tag(m.group(1), m.group(2), image_dir, charts_ctx))
            return f"\x00I{len(img_stash) - 1}\x00"

        ln = _IMG_RE.sub(_stash_img, line.strip())
        para.append(ln)
        i += 1

    flush_para()
    return "\n".join(out)


def _add_toc(body):
    """给 h2/h3 注入计数器 id（避中文 slug），把 <!-- toc --> 占位替换为目录。

    无标题或无占位符时静默跳过（占位符存在但无标题 → 移除占位）。
    """
    heads = []

    def repl(m):
        lvl, text = m.group(1), m.group(2)
        heads.append((int(lvl), re.sub(r'<[^>]+>', '', text)))
        return f'<h{lvl} id="sec-{len(heads) - 1}">{text}</h{lvl}>'

    body = re.sub(r'<h([23])>(.*?)</h\1>', repl, body)
    if '<!-- toc -->' not in body:
        return body
    if not heads:
        return body.replace('<!-- toc -->', '')
    items = ''.join(
        f'<li class="lvl{lvl}"><a href="#sec-{i}">{_html.escape(t)}</a></li>'
        for i, (lvl, t) in enumerate(heads))
    return body.replace('<!-- toc -->', f'<nav class="toc"><ul>{items}</ul></nav>')


# 图表 bootstrap：懒 init（IO 缺失回退全量）+ 单一 debounce resize 监听。
# 经典 script（file:// 禁 ES modules），无任何外部请求。
_BOOTSTRAP = """(function () {
  var el = document.getElementById('chart-data');
  if (!el || typeof echarts === 'undefined') return;
  var data = JSON.parse(el.textContent);
  var nodes = Array.prototype.slice.call(document.querySelectorAll('.chart'));
  var inited = [];
  function init(node) {
    var key = node.getAttribute('data-chart');
    if (!data[key] || !data[key].series) return;
    var c = echarts.init(node);
    c.setOption(data[key]);
    inited.push(c);
  }
  if ('IntersectionObserver' in window) {
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        if (e.isIntersecting) { io.unobserve(e.target); init(e.target); }
      });
    }, { rootMargin: '120px' });
    nodes.forEach(function (n) { io.observe(n); });
  } else {
    nodes.forEach(init);
  }
  var timer = null;
  window.addEventListener('resize', function () {
    clearTimeout(timer);
    timer = setTimeout(function () {
      inited.forEach(function (c) { c.resize(); });
    }, 120);
  });
})();"""


def _kpi_cards(kpi):
    """kpi.json → 静态 HTML 卡片（构建期渲染，无 JS，交互/纯图两模式通用）。"""
    def pct(v):
        return f'{v:+.1f}%' if isinstance(v, (int, float)) else '—'

    cards = []
    lat = kpi.get('latest') or {}
    if lat:
        sub = (f'住宅网签 · 环比 {pct(lat.get("mom"))} · 同比 {pct(lat.get("yoy"))}'
               + (f' · 套均 {lat["avg_area"]:.1f} m²' if lat.get('avg_area') else ''))
        cards.append(f'<div class="kpi-card"><div class="kpi-label">最新完整月 '
                     f'{_html.escape(str(lat.get("period", "")))}</div>'
                     f'<div class="kpi-value">{lat.get("units", 0):,}<span class="kpi-unit"> 套</span></div>'
                     f'<div class="kpi-sub">{sub}</div></div>')
    mtd = kpi.get('mtd')
    if mtd:
        cards.append(f'<div class="kpi-card kpi-mtd"><div class="kpi-label">'
                     f'{_html.escape(str(mtd.get("period", "")))}（{_html.escape(str(mtd.get("note", "进行中")))})</div>'
                     f'<div class="kpi-value">{mtd.get("units", 0):,}<span class="kpi-unit"> 套</span></div>'
                     f'<div class="kpi-sub">仅 {mtd.get("days", "?")} 天 · 非全月，勿与完整月直接比较</div></div>')
    return f'<div class="kpi-grid">{"".join(cards)}</div>' if cards else ''


def _extract_title(md):
    m = re.search(r"^#{1}\s+(.+?)\s*#*\s*$", md, flags=re.M)
    return re.sub(r"[*_`]", "", m.group(1)).strip() if m else "趋势报告"


def md_to_html(md_text, image_dir=None, charts_dir=None, vendor_path=None,
               no_js=False, kpi_path=None):
    """markdown → 自包含 HTML 字符串。

    charts_dir + vendor_path 同时可用且非 no_js 时启用交互模式：
    与 md 图片同 stem 的 charts/<stem>.json → 图表 div（文末内联 vendor +
    数据 + bootstrap）。DASHBOARD_NO_JS=1（render_file 层读取）或 vendor
    缺失 → 纯 PNG 模式，与旧版输出一致。
    kpi_path 存在时，`<!-- kpi -->` 占位 → 静态 KPI 卡片（构建期渲染）。
    """
    title = _extract_title(md_text)
    interactive = (not no_js) and charts_dir and vendor_path \
        and os.path.isfile(vendor_path)
    charts_ctx = {'dir': charts_dir, 'found': {}} if interactive else None
    kpi = None
    if kpi_path and os.path.isfile(kpi_path):
        try:
            with open(kpi_path, encoding='utf-8') as f:
                kpi = json.load(f)
        except (OSError, ValueError):
            kpi = None
    body = _add_toc(_convert(md_text, image_dir, charts_ctx))
    body = body.replace('<!-- kpi -->', _kpi_cards(kpi) if kpi else '')
    scripts = ''
    if charts_ctx is not None and charts_ctx['found']:
        with open(vendor_path, encoding='utf-8') as f:
            vendor_js = f.read().replace('</script>', '<\\/script>')
        chart_json = json.dumps(charts_ctx['found'], ensure_ascii=False,
                                separators=(',', ':')).replace('</', '<\\/')
        scripts = (f'<script>{vendor_js}</script>\n'
                   f'<script type="application/json" id="chart-data">{chart_json}</script>\n'
                   f'<script>{_BOOTSTRAP}</script>')
    if not no_js and 'table-wrap sortable' in body:
        scripts += ('\n' if scripts else '') + f'<script>{_SORT_JS}</script>'
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{_html.escape(title)}</title>
<style>{_stylesheet()}</style>
</head>
<body>
<div class="container">
{body}
{scripts}
</div>
</body>
</html>
"""


def render_file(md_path, out_html_path, charts_dir=None, vendor_path=None, no_js=None):
    """读 md_path（图片相对其目录解析），写自包含 html 到 out_html_path。

    charts_dir 缺省 = md 同目录下 charts/；vendor_path 缺省 = 仓库根
    assets/vendor/echarts.min.js；no_js 缺省读环境变量 DASHBOARD_NO_JS=1。
    """
    with open(md_path, encoding='utf-8') as f:
        md_text = f.read()
    image_dir = os.path.dirname(os.path.abspath(md_path))
    if charts_dir is None:
        charts_dir = os.path.join(image_dir, 'charts')
    if vendor_path is None:
        vendor_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'assets', 'vendor', 'echarts.min.js')
    if no_js is None:
        no_js = os.environ.get('DASHBOARD_NO_JS') == '1'
    html_text = md_to_html(md_text, image_dir=image_dir, charts_dir=charts_dir,
                           vendor_path=vendor_path, no_js=no_js,
                           kpi_path=os.path.join(image_dir, 'kpi.json'))
    os.makedirs(os.path.dirname(os.path.abspath(out_html_path)), exist_ok=True)
    with open(out_html_path, 'w', encoding='utf-8') as f:
        f.write(html_text)
    return out_html_path
