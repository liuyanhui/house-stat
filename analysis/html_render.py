# -*- coding: utf-8 -*-
"""Markdown → 自包含 HTML 转换器（趋势报告用）。

移植 refined-stock/scripts/gen-html.mjs 的 inline/convert 逻辑到 Python，
扩展两类该参考未支持、但本报告用到的元素：
  - GFM 表格（| a | b | / | --- |）→ 带样式 <table>，外层横向滚动
  - 图片 ![alt](path.png) → base64 内嵌 <img>，输出单文件自包含

主题：微信胶囊风格（胶囊 h2、左条 callout h3、强调色 strong、中文字体栈），
集中在一个 THEME 常量。独立 HTML 用 <style> 块（不发布到微信，无需全 inline）。

零第三方依赖（仅 stdlib）。内容稳定、确定性输出。
"""
import base64
import html as _html
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


def _img_tag(alt, src, image_dir):
    """把 ![alt](rel.png) 转成 base64 内嵌 <img>；读不到则回退原样引用。"""
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


def _table_html(buf):
    def cells(line):
        return [c.strip() for c in line.strip().strip('|').split('|')]

    header = cells(buf[0])
    rows = [cells(r) for r in buf[2:]]
    out = ['<div class="table-wrap"><table>', '<thead><tr>']
    out += [f"<th>{_inline(h)}</th>" for h in header]
    out += ['</tr></thead>', '<tbody>']
    for r in rows:
        out.append('<tr>')
        ncol = len(header)
        for i in range(ncol):
            out.append(f"<td>{_inline(r[i]) if i < len(r) else ''}</td>")
        out.append('</tr>')
    out += ['</tbody></table></div>']
    return '\n'.join(out)


# --- 块级 markdown → HTML（移植参考 convert + 表格/图片扩展） -------------
def _convert(md, image_dir):
    lines = re.sub(r"\r\n?", "\n", md).split("\n")
    out = []
    para = []

    def flush_para():
        if para:
            out.append(f"<p>{_inline(' '.join(para))}</p>")
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
            out.append(f"<blockquote>{_convert(chr(10).join(buf), image_dir)}</blockquote>")
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
                                _IMG_RE.match(line.strip()).group(2), image_dir))
            i += 1
            continue

        # 普通文本行 → 累积成段落（行内图片就地替换）
        ln = _IMG_RE.sub(lambda m: _img_tag(m.group(1), m.group(2), image_dir), line.strip())
        para.append(ln)
        i += 1

    flush_para()
    return "\n".join(out)


def _extract_title(md):
    m = re.search(r"^#{1}\s+(.+?)\s*#*\s*$", md, flags=re.M)
    return re.sub(r"[*_`]", "", m.group(1)).strip() if m else "趋势报告"


def md_to_html(md_text, image_dir=None):
    """把 markdown 文本转成自包含 HTML 字符串。image_dir 用于解析相对图片路径。"""
    title = _extract_title(md_text)
    body = _convert(md_text, image_dir)
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
</div>
</body>
</html>
"""


def render_file(md_path, out_html_path):
    """读 md_path（图片相对其目录解析），写自包含 html 到 out_html_path。返回 out 路径。"""
    with open(md_path, encoding='utf-8') as f:
        md_text = f.read()
    image_dir = os.path.dirname(os.path.abspath(md_path))
    html_text = md_to_html(md_text, image_dir=image_dir)
    os.makedirs(os.path.dirname(os.path.abspath(out_html_path)), exist_ok=True)
    with open(out_html_path, 'w', encoding='utf-8') as f:
        f.write(html_text)
    return out_html_path
