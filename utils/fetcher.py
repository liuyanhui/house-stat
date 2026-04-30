"""网页抓取"""
import requests
import time
import config


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
            return response.text

        except requests.RequestException as e:
            logger.error(f"请求失败：{e}")
            if attempt < config.MAX_RETRIES - 1:
                logger.info(f"等待 {config.RETRY_DELAY} 秒后重试...")
                time.sleep(config.RETRY_DELAY)
            else:
                logger.error("已达到最大重试次数，放弃抓取")
                raise
