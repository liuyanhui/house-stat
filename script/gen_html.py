# -*- coding: utf-8 -*-
"""Markdown 趋势报告 → 自包含 HTML（独立转换 CLI）。

默认：report/trend_report.md → report/trend_report.html
用法:
  python script/gen_html.py
  python script/gen_html.py --md path/to/in.md --out path/to/out.html
"""
import argparse
import os
import sys

try:
    sys.stdout.reconfigure(encoding='utf-8')
except (AttributeError, OSError):
    pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from analysis import html_render  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description='趋势报告 Markdown → 自包含 HTML')
    parser.add_argument('--md', default=os.path.join(BASE_DIR, 'report', 'trend_report.md'),
                        help='输入 Markdown 路径')
    parser.add_argument('--out', default=os.path.join(BASE_DIR, 'report', 'trend_report.html'),
                        help='输出 HTML 路径')
    args = parser.parse_args()

    if not os.path.isfile(args.md):
        print(f'错误: 未找到 {args.md}', file=sys.stderr)
        sys.exit(1)

    out = html_render.render_file(args.md, args.out)
    size_kb = os.path.getsize(out) / 1024
    print(f'HTML 已生成: {out} ({size_kb:.0f} KB，图片 base64 内嵌、自包含)')


if __name__ == '__main__':
    main()
