# -*- coding: utf-8 -*-
"""数据完整性校验脚本（独立运行）

校验面积段 / 价格段每月成交套数加总是否与区县"全市"一致（阈值 5%）。

用法:
  python script/validate.py                    # 校验默认 data/ 目录
  python script/validate.py --data-dir /path   # 指定数据目录
"""
import argparse
import os
import sys

# 确保中文输出不乱码
try:
    sys.stdout.reconfigure(encoding='utf-8')
except (AttributeError, OSError):
    pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from utils import validate_integrity  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description='数据完整性校验')
    parser.add_argument(
        '--data-dir', default=os.path.join(BASE_DIR, 'data'),
        help='数据目录路径',
    )
    args = parser.parse_args()

    if not os.path.isdir(args.data_dir):
        print(f'错误: 数据目录不存在: {args.data_dir}', file=sys.stderr)
        sys.exit(2)

    print(f'校验目录: {os.path.abspath(args.data_dir)}')
    print()

    ok, issues = validate_integrity(data_dir=args.data_dir)

    if ok:
        print('✓ 完整性校验通过：面积段/价格段加总与全市一致')
        sys.exit(0)
    else:
        print(f'✗ 发现 {len(issues)} 处不一致：')
        print(f"{'月份':>8s} | {'类型':>6s} | {'分段加总':>8s} | {'全市':>8s} | {'偏差':>6s}")
        print('-' * 50)
        for it in sorted(issues, key=lambda x: (x['月份'], x['类型'])):
            print(f"{it['月份']:>8s} | {it['类型']:>6s} | {it['分段加总']:>8d} | {it['全市']:>8d} | {it['偏差']:>6s}")
        sys.exit(1)


if __name__ == '__main__':
    main()
