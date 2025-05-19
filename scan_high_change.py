import asyncio
from datetime import datetime

import aiohttp
import requests
from win10toast import ToastNotifier

symbols_list_binance = []
symbols_list_gateio = []


# region 通用工具函数
def parse_timestamp(ts: int) -> datetime:
    """将毫秒时间戳转为datetime对象（替代pandas.to_datetime）"""
    return datetime.utcfromtimestamp(ts / 1000)


async def push_wechat(title: str, pairs: str) -> None:
    """推送微信通知（改用aiohttp异步请求）"""
    url = "https://www.pushplus.plus/send"
    params = {
        "token": "2fb9c4804bd8400684d60e4905365978",
        "title": title,
        "content": pairs
    }
    try:
        async with aiohttp.ClientSession() as session:
            await session.get(url, params=params)
    except Exception as e:
        print(f"微信推送失败: {e}")


async def push_windows(title: str, pairs: str) -> None:
    """Windows桌面通知（保持原逻辑）"""
    try:
        ToastNotifier().show_toast(title, pairs, duration=15, threaded=True)
    except Exception as e:
        print(f"Windows通知失败: {e}")


# endregion

# region Binance API
def get_all_futures_symbols_binance() -> list:
    """获取Binance合约交易对（同步请求，改用列表推导式简化）"""
    try:
        data = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo").json()
        symbols = [s['symbol'] for s in data['symbols'] if 'USDT' in s['symbol'] and s['symbol'] != "BTCSTUSDT"]
        return symbols
    except Exception as e:
        print(f"获取Binance合约交易对时发生错误: {str(e)}")
        return []


async def get_closed_kline_binance(session: aiohttp.ClientSession, symbol: str) -> dict:
    """获取K线数据（完全移除pandas依赖）"""
    params = {'symbol': symbol, 'interval': '5m', 'limit': 2}
    try:
        async with session.get("https://fapi.binance.com/fapi/v1/klines", params=params) as resp:
            data = await resp.json()
            if len(data) < 2:
                return None

            # 确定使用哪根K线（改用原生时间比较）
            current_time = datetime.utcnow()
            latest_close_time = parse_timestamp(data[-1][6])
            res_kline = data[-1] if current_time >= latest_close_time else data[-2]

            # 解析K线数据
            open_p, high_p, low_p, close_p = map(float, res_kline[1:5])
            is_bearish = close_p < open_p
            change = (
                (high_p - close_p) / high_p * 100 if is_bearish
                else (close_p - low_p) / low_p * 100
            )

            return {
                'symbol': symbol,
                'open_time': parse_timestamp(res_kline[0]),
                'close_time': parse_timestamp(res_kline[6]),
                'price_change': abs(change),
                'is_bearish': is_bearish
            }
    except Exception as e:
        print(f"请求{symbol}时发生错误: {str(e)}")


# endregion

# region 核心扫描逻辑
async def scan_binance() -> None:
    """Binance扫描逻辑（合并函数简化流程）"""
    async with aiohttp.ClientSession() as session:
        # 检测新增合约
        global symbols_list_binance
        current_symbols = set(get_all_futures_symbols_binance())
        if new_symbols := current_symbols - set(symbols_list_binance):
            await push_windows("Binance新增", ','.join(new_symbols))
            symbols_list_binance = list(current_symbols)

        # 扫描价格波动
        results = []
        tasks = [
            get_closed_kline_binance(session, sym)
            for sym in symbols_list_binance
        ]
        for future in asyncio.as_completed(tasks):
            if (kline := await future) and kline['price_change'] >= 7:
                results.append(
                    f"{kline['symbol']}: {'跌' if kline['is_bearish'] else '涨'}{kline['price_change']:.1f}%"
                )

        if results:
            import pyperclip
            pyperclip.copy(results[0].split(':')[0])
            await push_wechat("Binance波动", ','.join(results))
            await push_windows("Binance波动", ','.join(results))
        else:
            print("Binance无波动")


# endregion

# region Gate.io API
def get_all_futures_symbols_gateio() -> list:
    """获取Gate.io合约交易对（保持原逻辑）"""
    try:
        data = requests.get("https://api.gateio.ws/api/v4/futures/usdt/contracts").json()
        return [s["name"] for s in data if not s["in_delisting"]]
    except Exception as e:
        print(f"获取Gate.io合约交易对时发生错误: {str(e)}")
        return []


async def scan_gateio() -> None:
    """Gate.io扫描逻辑"""
    global symbols_list_gateio
    current_symbols = set(get_all_futures_symbols_gateio())
    if new_symbols := current_symbols - set(symbols_list_gateio):
        await push_wechat("Gate.io新增", ','.join(new_symbols))
        await push_windows("Gate.io新增", ','.join(new_symbols))
        symbols_list_gateio = list(current_symbols)


# endregion

# region 主循环
async def main_loop() -> None:
    """主循环（改用asyncio.sleep简化时间计算）"""
    while True:
        print(f"\n==== 扫描开始 {datetime.utcnow()} ====")
        await scan_binance()
        # await asyncio.gather(scan_binance(), scan_gateio())
        now = datetime.utcnow()
        current_seconds = now.minute * 60 + now.second
        delay = (300 - current_seconds % 300) % 300  # 处理余数为0的情况
        await asyncio.sleep(delay)


# endregion

if __name__ == "__main__":
    symbols_list_binance = get_all_futures_symbols_binance()
    symbols_list_gateio = get_all_futures_symbols_gateio()
    asyncio.run(main_loop())
