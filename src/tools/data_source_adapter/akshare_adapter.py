import pandas as pd
from typing import Dict, Any, List
import time
import random

from src.utils.logging_config import setup_logger

logger = setup_logger('akshare_adapter')

max_retries = 100

def get_akshare_price_data(akshare_code: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame:
    """从AKShare获取价格数据"""
    def fetch_data():
        import akshare as ak
        logger.info(f"Fetching price history using AKShare for {akshare_code}")
        df = ak.stock_zh_a_hist(
            symbol=akshare_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust=adjust
        )
        
        if not df.empty:
            # 转换AKShare数据格式
            df = df.rename(columns={
                "日期": "date",
                "开盘": "open",
                "最高": "high",
                "最低": "low",
                "收盘": "close",
                "成交量": "volume",
                "成交额": "amount",
                "振幅": "amplitude",
                "涨跌幅": "pct_change",
                "涨跌额": "change_amount",
                "换手率": "turnover"
            })
            df["date"] = pd.to_datetime(df["date"])
            
            # 确保所有数值列已转换为float类型
            for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'pct_change', 'change_amount', 'turnover']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            logger.info(f"Successfully retrieved data from AKShare: {len(df)} records")
            return df
        return pd.DataFrame()
    
    for attempt in range(max_retries):
        try:
            import akshare as ak
            return fetch_data()
        except ImportError:
            logger.warning("AKShare not installed, returning empty DataFrame")
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for get_akshare_price_data: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(1, 3))  # 随机延时1-3秒后重试
            else:
                logger.error(f"All attempts failed for get_akshare_price_data: {str(e)}")
                return pd.DataFrame()

def get_akshare_financial_metrics(akshare_code: str, exchange_prefix: str) -> List[Dict[str, Any]]:
    """从AKShare获取财务指标"""
    def fetch_data():
        import akshare as ak
        logger.info(f"Fetching financial metrics using AKShare for {akshare_code}")
        
        # 获取实时行情数据
        realtime_data = ak.stock_zh_a_spot_em()
        if not realtime_data.empty:
            stock_data = realtime_data[realtime_data['代码'] == akshare_code]
            if not stock_data.empty:
                stock_data = stock_data.iloc[0]
                
                # 获取财务指标
                # 这里简化了部分代码，实际实现应根据具体的AKShare API进行调整
                agent_metrics = {
                    "return_on_equity": 0.12,  # 示例值
                    "net_margin": 0.15,
                    "operating_margin": 0.18,
                    "revenue_growth": 0.1,
                    "earnings_growth": 0.08,
                    "book_value_growth": 0.05,
                    "current_ratio": 1.5,
                    "debt_to_equity": 0.4,
                    "free_cash_flow_per_share": 2.5,
                    "earnings_per_share": 1.2,
                    "pe_ratio": float(stock_data.get("市盈率-动态", 0)),
                    "price_to_book": float(stock_data.get("市净率", 0)),
                    "price_to_sales": 3.0
                }
                
                return [agent_metrics]
        return [{}]
    
    for attempt in range(max_retries):
        try:
            import akshare as ak
            return fetch_data()
        except ImportError:
            logger.warning("AKShare not installed, returning empty list")
            return [{}]
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for get_akshare_financial_metrics: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(1, 3))
            else:
                logger.error(f"All attempts failed for get_akshare_financial_metrics: {str(e)}")
                return [{}]

def get_akshare_financial_statements(akshare_code: str, exchange_prefix: str) -> List[Dict[str, Any]]:
    """从AKShare获取财务报表数据"""
    def fetch_data():
        import akshare as ak
        logger.info(f"Fetching financial statements using AKShare for {akshare_code}")
        
        # 获取资产负债表、利润表、现金流量表
        # 由于实现较复杂，这里简化为返回模拟数据
        # 实际实现应根据具体的AKShare API进行调整
        current_item = {
            "net_income": 5000000000,
            "operating_revenue": 20000000000,
            "operating_profit": 6000000000,
            "working_capital": 10000000000,
            "depreciation_and_amortization": 2000000000,
            "capital_expenditure": 3000000000,
            "free_cash_flow": 3000000000
        }
        
        previous_item = {
            "net_income": 4500000000,
            "operating_revenue": 18000000000,
            "operating_profit": 5500000000,
            "working_capital": 9000000000,
            "depreciation_and_amortization": 1800000000,
            "capital_expenditure": 2800000000,
            "free_cash_flow": 2700000000
        }
        
        return [current_item, previous_item]
    
    for attempt in range(max_retries):
        try:
            import akshare as ak
            return fetch_data()
        except ImportError:
            logger.warning("AKShare not installed, returning empty list")
            return [{}, {}]
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for get_akshare_financial_statements: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(1, 3))
            else:
                logger.error(f"All attempts failed for get_akshare_financial_statements: {str(e)}")
                return [{}, {}]

def get_akshare_market_data(akshare_code: str) -> Dict[str, Any]:
    """从AKShare获取市场数据"""
    def fetch_data():
        import akshare as ak
        logger.info(f"Fetching market data using AKShare for {akshare_code}")
        
        # 获取实时行情
        realtime_data = ak.stock_zh_a_spot_em()
        stock_data = realtime_data[realtime_data['代码'] == akshare_code]
        
        if not stock_data.empty:
            stock_data = stock_data.iloc[0]
            
            # 获取价格历史数据计算52周最高/最低价
            hist_data = ak.stock_zh_a_hist(symbol=akshare_code, period="daily", adjust="qfq")
            
            week_high = hist_data["最高"].max() if not hist_data.empty else stock_data.get("最高", 0)
            week_low = hist_data["最低"].min() if not hist_data.empty else stock_data.get("最低", 0)
            avg_volume = hist_data.tail(30)["成交量"].mean() if not hist_data.empty else stock_data.get("成交量", 0)
            
            return {
                "market_cap": float(stock_data.get("总市值", 0)),
                "volume": float(stock_data.get("成交量", 0)),
                "average_volume": float(avg_volume),
                "fifty_two_week_high": float(week_high),
                "fifty_two_week_low": float(week_low)
            }
        return {}
    
    for attempt in range(max_retries):
        try:
            import akshare as ak
            return fetch_data()
        except ImportError:
            logger.warning("AKShare not installed, returning empty dict")
            return {}
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for get_akshare_market_data: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(1, 3))
            else:
                logger.error(f"All attempts failed for get_akshare_market_data: {str(e)}")
                return {}