"""
房地产签约数据抓取程序 - 配置文件
"""

# 数据源 URL
BASE_URL = "http://bjjs.zjw.beijing.gov.cn/eportal/ui?pageId=307749"

# 目录配置
DATA_DIR = "data"      # 数据文件目录
LOG_DIR = "log"        # 日志文件目录

# CSV 文件路径
AGENCY_CSV = f"{DATA_DIR}/month_agency.csv"
DISTRICT_CSV = f"{DATA_DIR}/month_district.csv"
AREA_CSV = f"{DATA_DIR}/month_area.csv"
DAILY_CSV = f"{DATA_DIR}/daily.csv"  # 每日存量房网上签约数据
MONTH_CSV = f"{DATA_DIR}/month.csv"  # 月度汇总数据（网上签约套数、网上签约面积、住宅签约套数、住宅签约面积）

# 日志文件路径
LOG_FILE = f"{LOG_DIR}/house_stat.log"

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
