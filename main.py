import os
import platform
import time
from datetime import datetime
from pathlib import Path
from random import randint

import pandas as pd
import requests
from joblib import Parallel, delayed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager
from my_logger import get_logger
logger = get_logger("app.turnover")
chrome_options = Options()
chrome_options.add_argument('--no-sandbox')  # 在centos运行需要打开
chrome_options.add_argument('--disable-dev-shm-usage')    # 在centos运行需要打开
chrome_options.add_argument("--headless")  # 无头模式，不显示浏览器界面

TEST = False
PARALLEL = True
THREADS = 20
CSV_FILE = Path("data/cmc_turnover_rate.csv")
RAND_WAIT_SEC = 0.5


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
        except TimeoutException as e:
            logger.error(f"{func_name} 超时，{sleep_seconds} 秒后重试")
        except Exception as e:
            logger.error(f"{func_name} 报错，程序暂停 {sleep_seconds} 秒： {e}")
            logger.exception(e)
            time.sleep(sleep_seconds)
    else:
        if if_exit:
            raise ValueError(f'{func_name} 重试无效，程序退出')
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
        return marketPairs
    except Exception as e:
        # logger.exception(e)
        return None


def get_cmc_turnover_rate(_name, _symbol):
    page_url = f"https://coinmarketcap.com/currencies/{_name}"
    logger.debug(f"{page_url}")
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

    retry_wrapper(driver.get, func_name=f"webpage {_symbol}", sleep_seconds=2, if_exit=False, url=page_url)

    pct = -1.0

    selectors = [
        'dd.sc-8755d3ba-0.eXRmzO.base-text',
        'div.priceValue',
    ]
    # 尝试使用多个 CSS 选择器
    percent_element = None
    for selector in selectors:
        try:
            percent_element = driver.find_elements_by_css_selector(selector)
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
                if "%" in e.text:
                    pct = e.text.replace("%", "")
                    pct = float(pct)
                    pct /= 100
                    pct = round(pct, 4)
                    logger.debug(f"{_symbol} 子元素 {e.text} 匹配")
                    break
                elif 0 < float(e.text) < 1:
                    pct = float(e.text)
                    pct = round(pct, 4)
                    logger.debug(f"{_symbol} 子元素 {e.text} 匹配")
                    break
            except Exception as err:
                logger.debug(f"{_symbol} 子元素 {e.text} 不匹配: {err}")
                continue
    else:
        logger.warning(f"{_symbol} 父元素 最终失败")

    driver.quit()
    if pct == -1.0: logger.warning(f"{_symbol} 子元素 最终失败")
    return pct


def save_for_one(pair):
    _ms = int(RAND_WAIT_SEC * 1000)
    time.sleep(randint(_ms, _ms*2)/1000)

    _name = pair["baseCurrencySlug"]
    _symbol = pair["marketPair"]
    _pct = get_cmc_turnover_rate(_name, _symbol)
    # logger.debug(f"symbol: {_symbol} turnover_rate: {_pct}")

    # 获取当前时间并将分钟和秒设置为0，以便时间戳仅精确到小时
    _now = datetime.now().replace(minute=0, second=0, microsecond=0)
    df = pd.DataFrame({'candle_begin_time': [_now], 'symbol': [_symbol], 'name': [_name], 'turnover_rate': [_pct]})

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


def clear_chrom():
    system = platform.system()
    if system == "Linux":
        os.system("pkill -f chrom")


def main():

    cmc_pairs = get_cmc_market_pairs()
    if TEST: cmc_pairs = cmc_pairs[-5:]

    dfs = []
    if PARALLEL is False:
        for pair in tqdm(cmc_pairs):
            _s_sub = time.time()
            dfs.append(save_for_one(pair))
            logger.debug(f"本轮用时: {(time.time()-_s_sub):.2f}s")
    else:
        dfs = Parallel(n_jobs=THREADS, backend="threading")(
            delayed(save_for_one)(pair) for pair in tqdm(cmc_pairs)
        )

    all_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"汇总 完成：\n{all_df}")

    if not CSV_FILE.exists():
        all_df.to_csv(str(CSV_FILE), index=False)
    else:
        all_df.to_csv(str(CSV_FILE), mode="a", header=False, index=False)

    _df = format_csv()
    logger.info(f"整理csv文件 完成:\n{_df}")
    clear_chrom()
    logger.info(f"Linux 清理残留 chrom 进程 完成")


if __name__ == '__main__':
    _s = time.time()
    main()
    logger.info(f"总共用时: {(time.time()-_s):.2f}s")
