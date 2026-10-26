# -*- coding: utf-8 -*-
"""像素 diff（开发计划 T1.4b 验收工具，QA 口径：尺寸相等 + 逐像素均值差 < 容差）。

迁移期对照：旧 plots.py 产 report/xxx.png（基线）vs 新 charts 引擎产出。
均值差超容差不是自动失败——按计划"超差图人工复核并记录"（常见超差原因：
图例措辞改变、刻度稀疏化），复核结论记录在提交信息里。

用法：py -3.14 tests/pixel_diff.py <a.png> <b.png> [容差，默认 8.0]
退出码：0 过 / 1 超容差 / 2 尺寸不一致。
"""
import sys

import numpy as np
from PIL import Image, ImageChops


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(2)
    a = Image.open(sys.argv[1]).convert('RGB')
    b = Image.open(sys.argv[2]).convert('RGB')
    tol = float(sys.argv[3]) if len(sys.argv) > 3 else 8.0
    if a.size != b.size:
        print(f'SIZE MISMATCH: {a.size} vs {b.size}（布局漂移，需人工复核）')
        sys.exit(2)
    diff = np.asarray(ImageChops.difference(a, b), dtype=np.float32)
    mean = float(diff.mean())
    print(f'{sys.argv[1].split(chr(92))[-1].split("/")[-1]}: mean diff = {mean:.2f} '
          f'(tol {tol}) -> {"PASS" if mean < tol else "REVIEW"}')
    sys.exit(0 if mean < tol else 1)


if __name__ == '__main__':
    main()
