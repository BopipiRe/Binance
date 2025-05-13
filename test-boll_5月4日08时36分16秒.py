import asyncio
from datetime import datetime, timedelta

import aiohttp
import pandas as pd
import requests
import numpy as np

symbols_list = []
symbols_have_res = set()


async def push_wechat(msg):
    token = "2fb9c4804bd8400684d60e4905365978"  # 从PushPlus官网获取
    url = f"https://www.pushplus.plus/send?token={token}&title=涨跌幅提醒&content={msg}"
    requests.get(url)


def get_all_futures_symbols_sync():
    """同步获取所有USDT合约交易对"""
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    response = requests.get(url, proxies={"http": "http://127.0.0.1:7890"})
    data = response.json()
    global symbols_list
    symbols_list = [s['symbol'] for s in data['symbols'] if 'USDT' in s['symbol']]


def calculate_bollinger_bands(prices, window=20, std_dev=2):
    """计算布林带中轨、上轨、下轨"""
    if len(prices) < window:
        return None, None, None
    sma = np.mean(prices[-window:])
    std = np.std(prices[-window:])
    return sma, sma + std_dev * std, sma - std_dev * std


async def get_7d_high_low(session, symbol):
    """获取过去7天的最高价和最低价（基于日线）"""
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        'symbol': symbol,
        'interval': '1d',  # 日线级别
        'limit': 8  # 最近7天
    }
    async with session.get(url, params=params, proxy="http://127.0.0.1:7890") as response:
        data = await response.json()
        highs = [float(kline[2]) for kline in data[:-1]]  # 最高价数组
        lows = [float(kline[3]) for kline in data[:-1]]  # 最低价数组
        closes = [float(kline[4]) for kline in data[:-1]]  # 收盘价数组
        return max(closes), min(closes), float(data[-1][2]), float(data[-1][3])  # 返回过去7天最高价和最低价


async def get_closed_kline(session, symbol, interval):
    """异步获取最近一根已闭合的5分钟K线"""
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        'symbol': symbol,
        'interval': interval,
        'limit': 21  # 获取最近21根K线
    }
    async with session.get(url, params=params, proxy="http://127.0.0.1:7890") as response:
        data = await response.json()
        if len(data) < 2:
            return None

        latest_kline = data[-1]
        prev_kline = data[-2]

        # 获取K线收盘时间的分钟数（UTC时间）
        latest_close_time = pd.to_datetime(latest_kline[6], unit='ms')
        prev_close_time = pd.to_datetime(prev_kline[6], unit='ms')
        closes = [float(kline[4]) for kline in data]

        # 获取当前时间的分钟数（UTC时间）
        current_time = datetime.utcnow()

        # 仅当当前分钟等于收盘时间的分钟时才返回数据
        if current_time < latest_close_time:
            res_kline = prev_kline
            # _, high_band, lower_band = calculate_bollinger_bands(closes[0:-1])
        else:
            res_kline = latest_kline
            # _, high_band, lower_band = calculate_bollinger_bands(closes[1:])

        open_price = float(res_kline[1])
        close_price = float(res_kline[4])

        # 计算涨跌幅
        price_change = (close_price - open_price) / open_price * 100
        seven_day_high, seven_day_low, today_high, today_low = await get_7d_high_low(session, symbol)

        return {
            'symbol': symbol,
            'open_time': pd.to_datetime(res_kline[0], unit='ms'),
            'close_time': pd.to_datetime(res_kline[6], unit='ms'),
            'close': close_price,
            # 'lower_band': lower_band,
            # 'high_band': high_band,
            '7d_high': seven_day_high,
            '7d_low': seven_day_low,
            'today_high': today_high,
            'today_low': today_low,
            'price_change': price_change,  # 涨跌幅百分比
        }


async def scan_symbol(session, symbol, results, interval):
    """异步扫描单个交易对"""
    try:
        kline = await get_closed_kline(session, symbol, interval)
        change = 5 if interval == '5m' else 7
        if (kline and
                (kline['price_change'] >= change
                 # and kline['close'] > kline['high_band']
                #  and kline['today_high'] > kline['7d_high']
                 ) or
                (kline['price_change'] <= 0 - change
                #  and kline['close'] < kline['lower_band'] 
                 and kline['today_low'] < kline['7d_low'])):
            results.append(kline)
    except Exception as e:
        pass


async def scan_high_change_contracts(interval):
    """并发扫描所有合约"""
    async with aiohttp.ClientSession() as session:
        global symbols_list
        results = []
        tasks = [scan_symbol(session, symbol, results, interval) for symbol in symbols_list]
        await asyncio.gather(*tasks)  # 并发执行所有任务

        if not results:
            print("未找到符合条件的合约")
            return None

        # 直接返回结果列表，不进行排序
        return results


async def reset_symbols_have_res():
    """每天UTC 0点和12点重置symbols_have_res集合"""
    while True:
        symbols_have_res.clear()
        now = datetime.utcnow()
        # 计算下一个重置时间 (0:00或12:00)
        if now.hour < 12:
            next_reset = now.replace(hour=12, minute=0, second=0, microsecond=0)
        else:
            next_reset = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

        delay = (next_reset - now).total_seconds()
        await asyncio.sleep(delay)
        print(f"已重置symbols_have_res集合，时间: {datetime.now()}")


async def periodic_scan(interval):
    """每隔interval分钟执行一次扫描"""
    while True:
        now = datetime.now()
        gap = 5 if interval == '5m' else 15
        next_scan = (
                now.replace(second=0, microsecond=0)
                + timedelta(minutes=(gap - now.minute % gap))
        )

        delay = (next_scan - now).total_seconds()
        if delay > 0:
            await asyncio.sleep(delay)
        print(f"{interval}开始扫描，时间: {datetime.now()}")
        try:
            high_change_klines = await scan_high_change_contracts(interval)
            if high_change_klines is not None:
                # 直接输出结果，不转换为DataFrame
                msg = interval + ",".join([kline['symbol'] for kline in high_change_klines])

                for kline in high_change_klines:
                    print(' '.join([str(value) for value in kline.values()]))
                # if kline['symbol'] not in symbols_have_res:
                # direction = "上涨" if kline['price_change'] >= 0 else "下跌"
                # msg += f"{kline['symbol']}: {direction} {abs(kline['price_change']):.2f}%"
                # symbols_have_res.add(kline['symbol'])

                print(msg)
                await push_wechat(msg)
        except Exception as e:
            print(f"Error scan process: {str(e)}")


# 运行事件循环
if __name__ == "__main__":
    get_all_futures_symbols_sync()
    loop = asyncio.get_event_loop()
    # 同时运行两个协程
    loop.run_until_complete(asyncio.gather(
        periodic_scan('15m'),
        # periodic_scan('5m')
    ))
