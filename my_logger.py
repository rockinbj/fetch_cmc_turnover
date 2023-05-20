from pathlib import Path
ROOT_PATH = Path(__file__).resolve().parent.parent
import sys
sys.path.append(str(ROOT_PATH))

import logging

from concurrent_log_handler import ConcurrentRotatingFileHandler
from os import path

# from config import LOG_LEVEL_CONSOLE, LOG_LEVEL_FILE, LOG_PATH
LOG_LEVEL_CONSOLE = "debug"
LOG_LEVEL_FILE = "debug"
LOG_PATH = ROOT_PATH/"data"/"logs"

# 定义log文件的存储路径和名称
logPath = LOG_PATH
logName = "log.current"
logFile = path.join(logPath, logName)

# 定义logger对象
logger = logging.getLogger("app")
logger.setLevel(logging.DEBUG)

# 定义logger的记录格式
strFmt = "%(asctime)s|%(name)-15s:%(lineno)4d|%(processName)-12s|%(levelname)-8s %(message)s"
logFmt = logging.Formatter(strFmt)

# 定义屏幕输出
hConsole = logging.StreamHandler(stream=sys.stdout)
hConsole.setLevel(getattr(logging, LOG_LEVEL_CONSOLE.upper()))
hConsole.setFormatter(logFmt)

# 定义文件输出
hFile = ConcurrentRotatingFileHandler(logFile, maxBytes=1024*1024*10, backupCount=30, encoding="utf-8")
hFile.setLevel(getattr(logging, LOG_LEVEL_FILE.upper()))
hFile.setFormatter(logFmt)

# 将输出添加到logger
logger.addHandler(hConsole)
logger.addHandler(hFile)


def get_logger(_name):
    logger = logging.getLogger(_name)
    logger.setLevel(logging.DEBUG)
    return logger
