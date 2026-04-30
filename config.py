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
# 二手房数据 (resale)
RESALE_DAILY_CSV = os.path.join(DATA_DIR, "resale_daily.csv")  # 每日存量房网上签约数据
RESALE_MONTHLY_CSV = os.path.join(DATA_DIR, "resale_monthly.csv")  # 月度汇总数据
RESALE_5YEAR_CSV = os.path.join(DATA_DIR, "resale_5year.csv")  # 近五年存量房交易情况

# 新房数据 (new)
NEW_DAILY_CSV = os.path.join(DATA_DIR, "new_daily.csv")  # 每日商品房数据统计
NEW_5YEAR_CSV = os.path.join(DATA_DIR, "new_5year.csv")  # 近五年新建商品房网签情况

# 分类数据 (agency, district, area, price)
AGENCY_CSV = os.path.join(DATA_DIR, "agency_monthly.csv")
DISTRICT_CSV = os.path.join(DATA_DIR, "district_monthly.csv")
AREA_CSV = os.path.join(DATA_DIR, "area_monthly.csv")
PRICE_CSV = os.path.join(DATA_DIR, "price_monthly.csv")  # 按价格统计月度数据

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
