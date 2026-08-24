"""网页抓取"""
import os
from datetime import date
import requests
import time
import config


def _archive_html(html, logger):
    """原始 HTML 存档到 data/raw/。

    页面只显示最新一天/一月的数据，解析器改版或故障后若无存档则无法回补
    （经纪机构表 2026-05/06 两个月即因此永久丢失）。存档不进 git。
    """
    try:
        os.makedirs(config.RAW_DIR, exist_ok=True)
        path = os.path.join(config.RAW_DIR, f"{date.today().isoformat()}.html")
        # 同日多次抓取以最后一次为准
        with open(path, 'w', encoding='utf-8') as f:
            f.write(html)
        logger.info(f"原始HTML已存档：{path}（{len(html)} 字符）")
    except Exception as e:
        # 存档失败不阻断抓取流程
        logger.warning(f"HTML存档失败（不影响抓取）：{e}")


def fetch_html(logger):
    """
    抓取网页HTML内容
    失败时自动重试
    """
    for attempt in range(config.MAX_RETRIES):
        try:
            logger.info(f"正在抓取数据（第 {attempt + 1} 次尝试）...")
            response = requests.get(
                config.BASE_URL,
                headers=config.HEADERS,
                timeout=config.TIMEOUT
            )
            response.raise_for_status()
            response.encoding = 'utf-8'

            logger.info("成功获取网页内容")
            _archive_html(response.text, logger)
            return response.text

        except requests.RequestException as e:
            logger.error(f"请求失败：{e}")
            if attempt < config.MAX_RETRIES - 1:
                logger.info(f"等待 {config.RETRY_DELAY} 秒后重试...")
                time.sleep(config.RETRY_DELAY)
            else:
                logger.error("已达到最大重试次数，放弃抓取")
                raise
