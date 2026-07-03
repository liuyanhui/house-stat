# -*- coding: utf-8 -*-
"""导出 AI 分析输入（客观 digest + 可粘贴 prompt）。

输出 report/ai_digest.md：整篇复制粘贴给 LLM 即可得到客观视角分析。
不含价格、不含预测。用法:
  python script/gen_ai_digest.py
  python script/gen_ai_digest.py --out /path/to/ai_digest.md
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

from analysis import ai_digest  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description='导出 AI 客观分析 digest+prompt')
    parser.add_argument('--out', default=os.path.join(BASE_DIR, 'report', 'ai_digest.md'),
                        help='输出路径（默认 report/ai_digest.md）')
    args = parser.parse_args()

    out = ai_digest.render_file(args.out)
    print(f'AI 分析输入已导出: {out}')
    print('整篇复制粘贴给 LLM → 得到客观视角分析（不含价格、不含预测）。')


if __name__ == '__main__':
    main()
