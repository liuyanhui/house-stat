#!/bin/bash

# 固定提交信息
COMMIT_MSG="append data from http://bjjs.zjw.beijing.gov.cn/eportal/ui?pageId=307749"

# 启用错误检测：任一命令失败则退出
set -e

# 检查当前目录是否为 Git 仓库
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo "错误：当前目录不是 Git 仓库，请进入正确的仓库目录后重试。"
    exit 1
fi

# 检查是否有待提交的变更（包括未暂存、已暂存、未跟踪文件）
if git diff --quiet && git diff --cached --quiet && [ -z "$(git ls-files --others --exclude-standard)" ]; then
    echo "没有需要提交的更改，已跳过 commit 和 push。"
    exit 0
fi

# 添加所有变更（包括新增、修改、删除）
git add .

# 提交变更（若没有任何变更被添加，commit 会失败，但上面已提前判断）
git commit -m "$COMMIT_MSG"

# 推送到远程仓库（使用当前分支的上游配置，若未配置则自动推断）
git push

echo "操作成功完成：已提交并推送至远程仓库。"
