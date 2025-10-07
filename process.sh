# 数据分析和技术指标计算
python -m src.tools.data_analyzer --ticker 688122 --start-date 2024-09-01 --end-date 2025-10-01

# 新闻获取测试
python -m src.tools.news_crawler --ticker 688122 --start-date 2024-09-01 --end-date 2025-10-01

#训练
python -u -m model.train.train --ticker 688122 --start-date 2024-09-01 --end-date 2025-08-30 --model all

#回测
# python -u test/test_backtest.py --ticker 688122 --start-date 2025-08-01 --end-date 2025-10-01
python -u -m src.main_backtester --ticker 688122 --start-date 2025-07-01 --end-date 2025-10-01 --export-report --save-results --initial-capital 100000

# 预测
python -u -m src.main --ticker 688122 --start-date 2025-09-01 --end-date 2025-10-01 --summary --show-reasoning