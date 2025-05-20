import json
import os
from datetime import datetime
from typing import Dict, List, Union

# 类型别名
TradeRecord = Dict[str, Union[str, float]]
DataStructure = Dict[str, Union[List[TradeRecord], Dict[str, List[TradeRecord]]]]

# 数据文件路径
DATA_FILE = "trading_records.json"


def load_data() -> DataStructure:
    """加载或初始化交易记录数据"""
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return {"win": [], "lose": {}}


def save_data(data: DataStructure) -> None:
    """保存数据到JSON文件"""
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)


def show_statistics(data: DataStructure) -> None:
    """展示增强版交易统计数据（含方向分类）"""

    def _calc_stats(records: List[TradeRecord]) -> Dict[str, Dict[str, float]]:
        """计算指定记录集的多空分类统计"""
        stats = {
            "LONG": {"count": 0, "profit": 0.0},
            "SHORT": {"count": 0, "profit": 0.0}
        }
        for r in records:
            direction = r["direction"].upper()
            stats[direction]["count"] += 1
            stats[direction]["profit"] += float(r.get("profit", 0))
        return stats

    # 盈利交易统计
    win_stats = _calc_stats(data["win"])
    total_win = len(data["win"])
    total_win_profit = sum(float(r.get("profit", 0)) for r in data["win"])

    # 亏损交易统计
    lose_records = [r for pair in data["lose"].values() for r in pair]
    lose_stats = _calc_stats(lose_records)
    total_lose = len(lose_records)
    total_lose_loss = sum(float(r.get("profit", 0)) for r in lose_records)

    # 打印基础统计
    print("\n=== 全局统计 ===")
    print(f"总交易: {total_win + total_lose}笔 | 胜率: "
          f"{(total_win / (total_win + total_lose) * 100):.1f}%" if (total_win + total_lose) > 0 else "N/A")
    print(f"净收益: ${total_win_profit + total_lose_loss:+,.2f}")

    # 打印多空分类统计
    print("\n=== 多空分类统计 ===")
    for direction in ["LONG", "SHORT"]:
        win_pct = (win_stats[direction]["count"] / total_win * 100) if total_win > 0 else 0
        lose_pct = (lose_stats[direction]["count"] / total_lose * 100) if total_lose > 0 else 0

        print(f"\n【{direction}方向】")
        print(f"盈利交易: {win_stats[direction]['count']}笔 ({win_pct:.1f}%) | "
              f"总利润: ${win_stats[direction]['profit']:+,.2f}")
        print(f"亏损交易: {lose_stats[direction]['count']}笔 ({lose_pct:.1f}%) | "
              f"总亏损: ${lose_stats[direction]['profit']:+,.2f}")
        print(f"净收益: ${win_stats[direction]['profit'] + lose_stats[direction]['profit']:+,.2f}")


def input_float(prompt: str) -> float:
    """安全获取浮点数输入"""
    while True:
        try:
            return float(input(prompt).strip())
        except ValueError:
            print("请输入有效数字！")


def add_record_interactive() -> None:
    """交互式添加交易记录（含利润）"""
    data = load_data()

    while True:
        print("\n=== 新建交易记录 ===")
        pair = input("货币对（如EURUSD，留空返回）: ").strip().upper()
        if not pair:
            break

        # 输入验证
        direction = ""
        while direction not in ["LONG", "SHORT"]:
            direction = input("方向（LONG/SHORT）: ").strip().upper()

        result = ""
        while result not in ["WIN", "LOSE"]:
            result = input("结果（WIN/LOSE）: ").strip().upper()

        profit = input_float(f"利润金额（{result}为正数，LOSE请用负数）: $")

        # 构建记录
        record = {
            "pair": pair,
            "direction": direction.lower(),
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "profit": round(profit, 2)
        }

        # 保存记录
        if result == "WIN":
            data["win"].append(record)
        else:
            if pair not in data["lose"]:
                data["lose"][pair] = []
            data["lose"][pair].append(record)

        save_data(data)
        print(f"✅ 已保存: {pair} {direction} | 金额: ${profit:+,.2f}")


def query_records() -> None:
    """带方向分类的查询功能"""
    data = load_data()

    print("\n=== 盈利交易 ===")
    for i, r in enumerate(data["win"], 1):
        print(f"{i}. {r['time']} {r['pair']} {r['direction'].upper()} | "
              f"利润: ${float(r.get('profit', 0)):+,.2f}")

    print("\n=== 亏损交易 ===")
    for pair, records in data["lose"].items():
        for i, r in enumerate(records, 1):
            print(f"{i}. {r['time']} {pair} {r['direction'].upper()} | "
                  f"亏损: ${float(r.get('profit', 0)):+,.2f}")


def main_menu() -> None:
    """增强版主菜单"""
    while True:
        print("\n===== 外汇交易记录系统 =====")
        print("1. 新建交易记录")
        print("2. 查看完整记录")
        print("3. 显示统计报告（含多空分类）")
        print("4. 退出系统")

        choice = input("请选择操作: ").strip()

        if choice == "1":
            add_record_interactive()
        elif choice == "2":
            query_records()
        elif choice == "3":
            show_statistics(load_data())
        elif choice == "4":
            print("已退出系统")
            break
        else:
            print("无效输入，请重新选择")


if __name__ == "__main__":
    # 启动时显示欢迎信息和初始统计
    print("""
    ███████╗ ██████╗  █████╗ ██████╗ ███████╗
    ██╔════╝██╔═══██╗██╔══██╗██╔══██╗██╔════╝
    █████╗  ██║   ██║███████║██████╔╝███████╗
    ██╔══╝  ██║   ██║██╔══██║██╔═══╝ ╚════██║
    ██║     ╚██████╔╝██║  ██║██║     ███████║
    ╚═╝      ╚═════╝ ╚═╝  ╚═╝╚═╝     ╚══════╝
    """)
    show_statistics(load_data())
    main_menu()