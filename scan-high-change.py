import asyncio
from datetime import datetime, timedelta

import aiohttp
import numpy as np
import pandas as pd
import requests
from win10toast import ToastNotifier

symbols_list_binance = []
symbols_list_gateio = []


# region 通用
async def push_wechat(title, pairs):
    token = "2fb9c4804bd8400684d60e4905365978"  # 从PushPlus官网获取
    token_iphone = "e91fc3d7210641908abc048ccaf6852a"  # 从PushPlus官网获取
    url = f"https://www.pushplus.plus/send?token={token}&title={title}&content={pairs}"
    requests.get(url)


async def push_windows(title, pairs):
    """使用Windows通知"""
    try:
        # 初始化通知器
        toaster = ToastNotifier()

        # 显示通知（标题为交易所名称，内容为交易对信息）
        toaster.show_toast(
            title=title,
            msg=pairs,
            duration=15,  # 通知显示10秒
            threaded=True  # 非阻塞模式
        )
        print(f"{title}:{pairs}")
    except Exception as e:
        print(f"Windows通知发送失败: {str(e)}")


# endregion

# region Binance
def get_all_futures_symbols_binance():
    """同步获取所有USDT合约交易对"""
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    response = requests.get(url)
    data = response.json()
    current_pairs = [s['symbol'] for s in data['symbols'] if 'USDT' in s['symbol']]
    return current_pairs


async def get_closed_kline_binance(session, symbol):
    """异步获取最近一根已闭合的5分钟K线，并计算最高价/最低价到收盘价的幅度"""
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        'symbol': symbol,
        'interval': '5m',
        'limit': 2  # 获取最近2根K线
    }

    async with session.get(url, params=params) as response:
        data = await response.json()
        if len(data) < 2:
            return None

        latest_kline = data[-1]
        prev_kline = data[-2]

        # 获取K线收盘时间的分钟数（UTC时间）
        latest_close_time = pd.to_datetime(latest_kline[6], unit='ms')

        # 获取当前时间的分钟数（UTC时间）
        current_time = datetime.utcnow()

        # 确定使用哪根K线
        res_kline = latest_kline if current_time >= latest_close_time else prev_kline

        # 解析K线数据
        open_price = float(res_kline[1])
        high_price = float(res_kline[2])
        low_price = float(res_kline[3])
        close_price = float(res_kline[4])

        # 计算涨跌状态
        is_bearish = close_price < open_price  # 是否为阴线（下跌）

        # 计算幅度
        if is_bearish:
            high_to_close = (high_price - close_price) / high_price * 100
            low_to_close = 0
        else:
            low_to_close = (close_price - low_price) / low_price * 100
            high_to_close = 0

        price_change = max(high_to_close, low_to_close)

        return {
            'symbol': symbol,
            'open_time': pd.to_datetime(res_kline[0], unit='ms'),
            'close_time': pd.to_datetime(res_kline[6], unit='ms'),
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'is_bearish': is_bearish,
            'high_to_close_pct': high_to_close,  # 最高价到收盘价跌幅百分比
            'low_to_close_pct': low_to_close,  # 最低价到收盘价涨幅百分比
            'price_change': price_change,  # 价格变化幅度
        }


async def scan_symbol_binance(session, symbol, results):
    """异步扫描单个交易对，基于最高价/最低价到收盘价的幅度条件"""
    try:
        kline = await get_closed_kline_binance(session, symbol)
        if kline and (abs(kline['price_change']) >= 7):
            results.append(kline)
    except aiohttp.ClientError as e:
        print(f"请求{symbol}时发生网络错误: {str(e)}")
    except Exception as e:
        print(f"处理{symbol}时发生错误: {str(e)}")


async def scan_high_change_contracts_binance():
    """并发扫描所有合约"""
    async with aiohttp.ClientSession() as session:
        global symbols_list_binance
        results = []
        tasks = [scan_symbol_binance(session, symbol, results) for symbol in symbols_list_binance]
        await asyncio.gather(*tasks)  # 并发执行所有任务

        if not results:
            print("未找到符合条件的合约")
            return None

        # 直接返回结果列表，不进行排序
        return sorted(results, key=lambda x: abs(x['price_change']), reverse=True)


# endregion


# region Gate.io
def get_all_futures_symbols_gateio():
    """同步获取Gate.io所有USDT永续合约交易对"""
    url = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
    response = requests.get(url)
    data = response.json()
    return [s["name"] for s in data if s["in_delisting"] is False]  # 排除已下架合约


async def get_new_symbol_gateio():
    try:
        global symbols_list_gateio
        current_symbols = set(get_all_futures_symbols_gateio())
        new_symbols = current_symbols - set(symbols_list_gateio)
        if new_symbols:
            await push_windows("Gate.io", ','.join(new_symbols))
            await push_wechat("Gate.io", ','.join(new_symbols))
            symbols_list_gateio = list(current_symbols)
    except Exception as e:
        print(f"[Gate.io] 扫描错误: {str(e)}")


# endregion


# region Scan
async def coordinated_scan():
    """协调三个交易所的扫描任务，并在完成后重置集合"""
    while True:
        now = datetime.now()
        next_scan = now.replace(second=0, microsecond=0) + timedelta(minutes=(5 - now.minute % 5))
        delay = (next_scan - now).total_seconds()
        if delay > 0:
            await asyncio.sleep(delay)

        print(f"\n===== 开始全量扫描 {datetime.now()} =====")

        await get_new_symbol_gateio()
        await periodic_scan_binance()

        print(f"下次扫描时间: {next_scan + timedelta(minutes=5)}")


async def periodic_scan_binance():
    """Binance扫描（不再包含循环，单次执行）"""
    try:
        print(f"[Binance] 扫描启动 {datetime.now()}")
        high_change_klines = await scan_high_change_contracts_binance()
        if high_change_klines:
            pairs = []
            for kline in high_change_klines:
                direction = "下跌" if kline['is_bearish'] else "上涨"
                pairs.append(f"{kline['symbol']}: {direction}{kline['price_change']:.2f}%")
            await push_windows("Binance", ','.join(pairs))
            await push_wechat("Binance", ','.join(pairs))

        # 检测新增合约（独立于涨跌幅扫描）
        global symbols_list_binance
        current_symbols = set(get_all_futures_symbols_binance())
        new_symbols = current_symbols - set(symbols_list_binance)
        if new_symbols:
            await push_windows("Binance", ','.join(new_symbols))
            await push_wechat("Binance", ','.join(new_symbols))
            symbols_list_binance = list(current_symbols)
    except Exception as e:
        print(f"[Binance] 扫描错误: {str(e)}")


# endregion

# 运行事件循环
if __name__ == "__main__":
    symbols_list_binance = get_all_futures_symbols_binance()
    symbols_list_gateio = get_all_futures_symbols_gateio()
    # 启动主循环
    asyncio.run(coordinated_scan())
