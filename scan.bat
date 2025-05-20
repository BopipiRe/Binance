@echo off
chcp 65001

call activate Binance
python scan_high_change.py

pause