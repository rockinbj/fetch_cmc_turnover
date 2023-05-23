import errno
import os
import platform
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path
ROOT_PATH = Path(__file__).resolve().parent
from random import randint

import pandas as pd
import requests
from joblib import Parallel, delayed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, \
    TimeoutException, StaleElementReferenceException, InvalidSelectorException
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm
from my_logger import get_logger
logger = get_logger("app.turnover")
chrome_options = Options()
chrome_options.add_argument('--no-sandbox')  # 在centos运行需要打开
chrome_options.add_argument('--disable-dev-shm-usage')    # 在centos运行需要打开
chrome_options.add_argument("--headless")  # 无头模式，不显示浏览器界面
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)

TEST = False
TEST_SYMBOLS = ["SXP"]
OTHER_SYMBOL_NUM = 1  # 最小1
PARALLEL = True
RAND_WAIT_SEC = 0.5
THREADS = 5
CSV_FILE = ROOT_PATH/"data"/"cmc_cap_vol_tor.csv" if TEST is False else ROOT_PATH/"data"/"temp"/"test.csv"

# cmc页面上有错误数据，此手写列表用来修正错误
# 包含"KNC"的symbol，用指定的str作为name
STATIC_LIST = {
    "KNC": "kyber-network-crystal-v2",
}


def retry_wrapper(func, func_name='', retry_times=5, sleep_seconds=5, if_exit=True, **params):
    """
    需要在出错时不断重试的函数，例如和交易所交互，可以使用本函数调用。

    :param func:            需要重试的函数名
    :param params:          参数
    :param func_name:       方法名称
    :param retry_times:     重试次数
    :param sleep_seconds:   报错后的sleep时间
    :param if_exit:         报错超过上限是否退出
    :return:                func运行的结果
    """
    for _ in range(retry_times):
        try:
            result = func(**params)
            return result
        except TimeoutException as err:
            logger.error(f"{func_name} 超时，{sleep_seconds} 秒后重试: {err}")
            time.sleep(sleep_seconds)
        except OSError as err:
            # 如果是OSError: [Errno 26] Text file busy: 'chromedriver'，暂停重试 通常就没问题
            if err.errno == errno.ETXTBSY:
                logger.error(f"{func_name} 访问冲突，{sleep_seconds} 秒后重试: {err}")
                time.sleep(sleep_seconds)
            else:
                logger.error(f"{func_name} 报错，程序暂停 {sleep_seconds} 秒： {err}")
                time.sleep(sleep_seconds)
        except Exception as err:
            logger.error(f"{func_name} 报错，程序暂停 {sleep_seconds} 秒： {err}")
            logger.exception(err)
            time.sleep(sleep_seconds)
    else:
        if if_exit:
            raise RuntimeError(f'{func_name} 重试无效，程序退出')
        else:
            logger.error(f'{func_name} 重试无效，程序不退出，跳过')


def get_cmc_market_pairs():
    url_entry = "https://api.coinmarketcap.com/data-api/v3/exchange/market-pairs/latest?" \
                "slug=binance&category=perpetual&start=1&quoteCurrencyId=825&limit=200"

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                             'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'}

    try:
        r = requests.get(url_entry, headers=headers)
        r = r.json()
        marketPairs = r["data"]["marketPairs"]

        # 用 固定 list 进行修正
        for p in marketPairs:
            for symbol, slug in STATIC_LIST.items():
                if symbol in p['marketPair']:
                    slug_ori = p['baseCurrencySlug']
                    p['baseCurrencySlug'] = slug
                    logger.info(f"手动修正：{p['marketPair']} 用 {slug} 替换 {slug_ori}")

        return marketPairs
    except Exception as e:
        # logger.exception(e)
        return None


def get_cmc_turnover_rate(_name, _symbol, _driver):
    page_url = f"https://coinmarketcap.com/currencies/{_name}"
    logger.debug(f"{page_url}")

    retry_wrapper(_driver.get, func_name=f"webpage {_symbol}", sleep_seconds=2, if_exit=False, url=page_url)

    pct = -1.0

    selectors = [
        'dd.sc-8755d3ba-0.eXRmzO.base-text',
        'div.priceValue',
    ]
    # 尝试使用多个 CSS 选择器
    percent_element = None
    for selector in selectors:
        try:
            percent_element = _driver.find_elements_by_css_selector(selector)
            # logger.debug(percent_element)
            if percent_element:
                logger.debug(f"{_symbol} 父元素 {selector} 匹配")
                break
        except NoSuchElementException:
            logger.debug(f"{_symbol} 父元素 {selector} 不匹配")
            continue

    if percent_element:
        for e in percent_element:
            try:
                _text = e.text

                if "%" in _text:
                    pct = _text.replace("%", "")
                    pct = float(pct)
                    pct /= 100
                    pct = round(pct, 4)
                    logger.debug(f"{_symbol} 子元素 {_text} 匹配")
                    break
                elif 0 < float(_text) < 1:
                    pct = float(_text)
                    pct = round(pct, 4)
                    logger.debug(f"{_symbol} 子元素 {_text} 匹配")
                    break
            except StaleElementReferenceException as err:
                logger.error(f"{_symbol} 获取 子元素 文本 失败，跳过: {err}")
                continue
            except Exception as err:
                logger.debug(f"{_symbol} 子元素 {_text} 不匹配: {err}")
                continue
    else:
        logger.warning(f"{_symbol} 父元素 最终失败")

    # driver.quit()
    if pct == -1.0: logger.warning(f"{_symbol} 子元素 最终失败")
    return pct


def get_cmc_cap_vol_tor(_name, _symbol, _driver):
    """
    获取cmc 币种页面 中的：
        1、流通市值
        2、24h CEX成交量
        3、24h 换手率
    举例：https://coinmarketcap.com/currencies/qtum/
    :param _name:
    :param _symbol:
    :param _driver:
    :return:
    """
    page_url = f"https://coinmarketcap.com/currencies/{_name}"
    logger.debug(f"{page_url}")

    retry_wrapper(_driver.get, func_name=f"webpage {_symbol}", sleep_seconds=2, if_exit=False, url=page_url)

    _cap = -1.0
    _vol = -1.0
    _tor = -1.0

    # 获取 流通市值
    # 多个xpath，逐一尝试
    _cap_xpaths = [
        '//div[contains(@class, "cPJgvg")][.//dt[contains(text(), "Market cap") and not(contains(text(), "Volume"))]]//dd[@class="sc-8755d3ba-0 eXRmzO base-text"]',
        '//div[contains(@class, "statsBlockInner")][.//div[contains(text(), "Market Cap") and not(contains(text(), "24h Volume / Market Cap"))]]//div[@class="statsValue"]',
        '//*[@id="section-coin-stats"]/div/dl/div[1]/div[1]/dd',
        '//*[@id="__next"]/div/div[1]/div[2]/div/div[1]/div[2]/div/div[3]/div[1]/div[1]/div[1]/div[2]/div',
        '//*[@id="__next"]/div/div[1]/div[2]/div/div[1]/div[3]/div/div[3]/div[1]/div[1]/div[1]/div[2]/div',
    ]
    for x in _cap_xpaths:
        try:
            _cap_item = _driver.find_element_by_xpath(x)
            _cap_str = _cap_item.text
            _cap_str = _cap_str.replace("$","").replace(",","")
            _cap = float(_cap_str)

            logger.debug(f"{_symbol} 流通市值 找到 {_cap}")
            break
        except NoSuchElementException as err:
            # logger.debug(f"{_symbol} 获取 流通市值 未找到，继续搜索: {err}")
            continue
        except StaleElementReferenceException as err:
            logger.error(f"{_symbol} 获取 流通市值 元素失效，继续搜索: {err}")
            continue
        except Exception as err:
            if _symbol != "DEFI/USDT":  # DEFI本身就没有数据，跳过告警
                logger.error(f"{_symbol} 获取 流通市值 报错，继续搜索: {err}")
                logger.exception(err)
            continue
    if _cap == -1.0: logger.warning(f"{_symbol} 流通市值 最终失败")

    # 获取 成交量
    _vol_xpaths = [
        '//div[contains(@class, "cPJgvg")][.//dt[contains(text(), "Volume (24h)") and not(contains(text(), "Volume/Market cap"))]]//dd[@class="sc-8755d3ba-0 eXRmzO base-text"]',
        '//div[contains(@class, "statsBlockInner")][.//div[contains(@class, "statsLabel") and contains(text(), "Volume") and not(contains(text(), "24h Volume / Market Cap"))]]//div[@class="statsValue"]',
        '//*[@id="section-coin-stats"]/div/dl/div[2]/div[1]/dd',
        '//*[@id="__next"]/div/div[1]/div[2]/div/div[1]/div[2]/div/div[3]/div[1]/div[3]/div[1]/div[2]/div',
        '//*[@id="__next"]/div/div[1]/div[2]/div/div[1]/div[3]/div/div[3]/div[1]/div[3]/div[1]/div[2]/div',
    ]
    for x in _vol_xpaths:
        try:
            _vol_item = _driver.find_element_by_xpath(x)
            _vol_str = _vol_item.text
            _vol_str = _vol_str.replace("$", "").replace(",", "")
            _vol = float(_vol_str)

            logger.debug(f"{_symbol} 成交量 找到 {_vol}")
            break
        except NoSuchElementException as err:
            continue
        except StaleElementReferenceException as err:
            logger.error(f"{_symbol} 获取 成交量 元素失效，继续搜索: {err}")
            continue
        except Exception as err:
            if _symbol != "DEFI/USDT":  # DEFI本身就没有数据，跳过告警
                logger.error(f"{_symbol} 获取 成交量 报错，继续搜索: {err}")
                logger.exception(err)
            continue
    if _vol == -1.0: logger.warning(f"{_symbol} 成交量 最终失败")

    # 获取 换手率
    _tor_xpaths = [
        '//div[contains(@class, "cPJgvg")][.//dt[contains(text(), "Volume/Market cap")]]//dd[@class="sc-8755d3ba-0 eXRmzO base-text"]',
        '//div[contains(@class, "statsBlockInner")][.//div[contains(text(), "24h Volume / Market Cap")]]//div[@class="priceValue"]',
        '//*[@id="section-coin-stats"]/div/dl/div[3]/div/dd',
        '//*[@id="__next"]/div/div[1]/div[2]/div/div[1]/div[2]/div/div[3]/div[1]/div[1]/div[2]/div/div[2]',
        '//*[@id="__next"]/div/div[1]/div[2]/div/div[1]/div[3]/div/div[3]/div[1]/div[1]/div[3]/div/div[2]',
    ]
    for x in _tor_xpaths:
        try:
            _tor_item = _driver.find_element_by_xpath(x)
            _tor_str = _tor_item.text
            _tor_str = _tor_str.replace("%", "")
            _tor = float(_tor_str)
            _tor = _tor if 0 <= _tor <= 1 else _tor / 100

            logger.debug(f"{_symbol} 换手率 找到 {_tor}")
            break

        except NoSuchElementException as err:
            # logger.debug(f"{_symbol} 获取 换手率 未找到，继续搜索: {err}")
            # logger.exception(err)
            continue
        except StaleElementReferenceException as err:
            logger.error(f"{_symbol} 获取 换手率 元素失效，继续搜索: {err}")
            logger.exception(err)
            continue
        except Exception as err:
            if _symbol != "DEFI/USDT":  # DEFI本身就没有数据，跳过告警
                logger.error(f"{_symbol} 获取 换手率 报错，继续搜索: {err}")
                logger.exception(err)
            continue
    if _tor == -1.0: logger.warning(f"{_symbol} 换手率 最终失败")

    return _cap, _vol, _tor


def save_for_one(pair, driver_path):
    _ms = int(RAND_WAIT_SEC * 1000)
    time.sleep(randint(_ms, _ms*2)/1000)

    _name = pair["baseCurrencySlug"]
    _symbol = pair["marketPair"]

    # 为每个线程创建独立的drvier，防止冲突
    temp_dir = Path(tempfile.mkdtemp(dir=str(ROOT_PATH/"data"/"temp")))
    temp_file = temp_dir / "chromedriver"
    shutil.copy(driver_path, str(temp_file))
    driver = retry_wrapper(webdriver.Chrome, func_name=f"{_symbol} create webdriver.Chrome",
                           retry_times=3, sleep_seconds=1, if_exit=False,
                           executable_path=str(temp_file), options=chrome_options)

    _cap, _vol, _tor = get_cmc_cap_vol_tor(_name, _symbol, driver)

    # 清理driver和临时目录
    driver.quit()
    shutil.rmtree(temp_dir)

    # 获取当前时间并将分钟和秒设置为0，以便时间戳仅精确到小时
    _now = datetime.now().replace(minute=0, second=0, microsecond=0)
    df = pd.DataFrame({
        'candle_begin_time': [_now],
        'symbol': [_symbol],
        'name': [_name],
        'market_cap': [_cap],  # 流通市值
        'vol': [_vol],  # 24h CEX交易量
        'turnover_rate': [_tor],  # 换手率
    })

    logger.info(f"{_symbol} 爬取 完成:\n{df}")
    return df


def format_csv():
    _df = pd.read_csv(str(CSV_FILE))
    _df = _df.sort_values(by=['symbol', 'candle_begin_time'])
    _df = _df.drop_duplicates(subset=['symbol', 'candle_begin_time'], keep='last')
    _df = _df.sort_values(by="candle_begin_time")
    _df = _df.reset_index(drop=True)
    _df.to_csv(str(CSV_FILE), index=False)
    return _df


def backup_csv():
    if CSV_FILE.exists():
        _df = pd.read_csv(str(CSV_FILE))
        last_time = _df.iloc[-1]["candle_begin_time"].replace(" ", "_").replace(":", "-")
        backup_path = CSV_FILE.with_name(CSV_FILE.stem + CSV_FILE.suffix + f".{last_time}")
        shutil.copy(CSV_FILE, backup_path)
        return backup_path
    else:
        return None


def clear_chrom():
    system = platform.system()
    if system == "Linux":
        os.system("pkill -f chrom")


def check_running():
    main_program_name = os.path.basename(sys.argv[0])
    current_pid = os.getpid()  # 获取pid，排除检查自己
    command = "ps ax -o pid,args"
    process_list = subprocess.check_output(command, shell=True, text=True)
    process_lines = process_list.strip().split("\n")

    for line in process_lines[1:]:  # 跳过表头行
        pid, args = line.strip().split(maxsplit=1)

        if int(pid) != current_pid and main_program_name in args:
            logger.info(f"爬虫 主程序 {main_program_name} 已经有进程，本次不运行，退出")
            exit()


def main():
    # 检查 是否已经有 爬虫在运行，不争抢
    if platform.system() == "Linux":
        check_running()

    # 备份当前csv，以防破坏已有数据，备份文件名 是最后写入的 candle_begin_time
    bk_file = backup_csv()
    if isinstance(bk_file, Path) and bk_file.exists():
        logger.info(f"备份csv 完成")
    else:
        logger.warning(f"备份csv 失败，请检查，程序继续")

    # 获取cmc symbol列表，当前获取的是binance USDT prep币种
    cmc_pairs = get_cmc_market_pairs()
    # 如果是测试局，减少币种数量，可以指定 测试币种
    if TEST:
        cmc_pairs_ori = cmc_pairs
        cmc_pairs = cmc_pairs_ori[-OTHER_SYMBOL_NUM:]
        for s in TEST_SYMBOLS:
            for p in cmc_pairs_ori:
                if s in p["marketPair"]: cmc_pairs.append(p)

    # 单线程 或者 多线程 爬取内容
    # 单线程 约 30分钟 一轮，多线程约 20 分钟 一轮
    driver_path = ChromeDriverManager().install()
    dfs = []
    if PARALLEL is False:
        global RAND_WAIT_SEC
        RAND_WAIT_SEC = 0
        for pair in tqdm(cmc_pairs):
            _s_sub = time.time()
            dfs.append(save_for_one(pair, driver_path))
            logger.debug(f"本轮用时: {(time.time()-_s_sub):.2f}s")
    else:
        dfs = Parallel(n_jobs=THREADS, backend="threading")(
            delayed(save_for_one)(pair, driver_path) for pair in tqdm(cmc_pairs)
        )

    all_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"汇总 完成：\n{all_df}")

    # 写入csv
    if not CSV_FILE.exists():
        all_df.to_csv(str(CSV_FILE), index=False)
    else:
        all_df.to_csv(str(CSV_FILE), mode="a", header=False, index=False)

    # 重新整理csv，将candle_begin_time相同的 按照最新覆盖，最后 按时间排序
    _df = format_csv()
    logger.info(f"整理csv文件 完成:\n{_df}")


if __name__ == '__main__':

    try:
        _s = time.time()
        retry_wrapper(main, func_name="主程序", retry_times=2, sleep_seconds=300, if_exit=True)
        logger.info(f"总共用时: {(time.time() - _s):.2f}s")
    except Exception as e:
        logger.error(f"主程序错误，退出: {e}")
        logger.exception(e)
    finally:
        clear_chrom()
        logger.info(f"Linux 清理残留 chrom 进程 完成")
