"""目录管理"""
import os
import config


def ensure_directories():
    """
    确保所有必需的目录存在
    如果目录不存在，则创建它们
    """
    for directory in config.DIRECTORIES:
        if not os.path.exists(directory):
            try:
                os.makedirs(directory, exist_ok=True)
                print(f"[INFO] 创建目录: {directory}")
            except OSError as e:
                print(f"[ERROR] 创建目录失败: {directory}, 错误: {e}")
                raise
        else:
            print(f"[INFO] 目录已存在: {directory}")
