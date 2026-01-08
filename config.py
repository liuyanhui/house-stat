"""
房地产签约数据抓取程序 - 配置文件
"""
import os

# 获取脚本文件所在目录的绝对路径
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 数据源 URL
BASE_URL = "http://bjjs.zjw.beijing.gov.cn/eportal/ui?pageId=307749"

# 目录配置（使用绝对路径）
DATA_DIR = os.path.join(SCRIPT_DIR, "data")      # 数据文件目录
LOG_DIR = os.path.join(SCRIPT_DIR, "log")        # 日志文件目录

# CSV 文件路径（使用os.path.join跨平台兼容）
AGENCY_CSV = os.path.join(DATA_DIR, "month_agency.csv")
DISTRICT_CSV = os.path.join(DATA_DIR, "month_district.csv")
AREA_CSV = os.path.join(DATA_DIR, "month_area.csv")
DAILY_CSV = os.path.join(DATA_DIR, "daily.csv")  # 每日存量房网上签约数据
MONTH_CSV = os.path.join(DATA_DIR, "month.csv")  # 月度汇总数据（网上签约套数、网上签约面积、住宅签约套数、住宅签约面积）

# 日志文件路径
LOG_FILE = os.path.join(LOG_DIR, "house_stat.log")

# 需要创建的目录列表
DIRECTORIES = [DATA_DIR, LOG_DIR]

# HTTP 请求配置
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# 重试配置
MAX_RETRIES = 3
RETRY_DELAY = 5  # 秒

# 请求超时（秒）
TIMEOUT = 30

# CSV 编码（Excel 友好的 UTF-8 with BOM）
CSV_ENCODING = 'utf-8-sig'
