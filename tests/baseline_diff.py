# -*- coding: utf-8 -*-
"""基线 diff 白名单校验：对比当前产物与 tests/baseline/ 基线。

diff 中的每一行（+/-）必须命中白名单文件里的任一正则，否则非零退出。
基线生成原则：改代码前先用【旧代码 + 当前数据】生成并存档，这样 diff 只含代码改动。

用法：py -3.14 tests/baseline_diff.py <baseline.md> <current.md> <whitelist.txt>
"""
import difflib
import re
import sys

# Windows 控制台默认 GBK，报告里的 ⚠️/中文会崩；统一 utf-8
try:
    sys.stdout.reconfigure(encoding='utf-8')
except (AttributeError, OSError):
    pass


def main():
    if len(sys.argv) != 4:
        print(__doc__)
        sys.exit(2)
    baseline, current, wl = sys.argv[1:4]
    with open(baseline, encoding='utf-8') as f:
        old = f.readlines()
    with open(current, encoding='utf-8') as f:
        new = f.readlines()
    with open(wl, encoding='utf-8') as f:
        patterns = [ln.strip() for ln in f
                    if ln.strip() and not ln.strip().startswith('#')]
    regexes = [re.compile(p) for p in patterns]

    diff_lines = [ln for ln in difflib.unified_diff(old, new)
                  if ln[0] in '+-' and not ln.startswith(('+++', '---'))]
    bad = [ln for ln in diff_lines if not any(r.search(ln) for r in regexes)]
    print(f'{current}: diff {len(diff_lines)} 行，白名单外 {len(bad)} 行')
    for ln in bad[:20]:
        print('  白名单外 >>', ln.rstrip())
    sys.exit(1 if bad else 0)


if __name__ == '__main__':
    main()
