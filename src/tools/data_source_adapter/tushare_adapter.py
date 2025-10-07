import os
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime, timedelta
import time
import random

from src.utils.logging_config import setup_logger
from dotenv import load_dotenv

load_dotenv()
logger = setup_logger('tushare_adapter')

max_retries = 100

# 获取TuShare API密钥
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN")

def safe_df(df):
    """确保返回的是 DataFrame，避免 None"""
    return df if df is not None else pd.DataFrame()

def get_tushare_price_data(tushare_code: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame:
    """从TuShare获取价格数据"""
    def fetch_data():
        import tushare as ts
        if not TUSHARE_TOKEN:
            logger.warning("TuShare token not found")
            return pd.DataFrame()
        
        ts.set_token(TUSHARE_TOKEN)
        pro = ts.pro_api()
        
        logger.info(f"Fetching price history using TuShare for {tushare_code}")
        
        # 转换调整类型
        adj = None
        if adjust == 'qfq':
            adj = 'qfq'
        elif adjust == 'hfq':
            adj = 'hfq'
        
        # 尝试使用pro_bar获取复权数据
        try:
            if adj:
                df = ts.pro_bar(ts_code=tushare_code, adj=adj, start_date=start_date, end_date=end_date)
            else:
                df = ts.pro_bar(ts_code=tushare_code, start_date=start_date, end_date=end_date)
            df = safe_df(df)
        except Exception as e:
            logger.warning(f"TuShare pro_bar failed: {str(e)}, trying daily API...")
            # 备选方法：获取日线数据
            df = pro.daily(ts_code=tushare_code, start_date=start_date, end_date=end_date)
            df = safe_df(df)
            
            # 如果需要复权
            if adj and not df.empty:
                adj_factor = pro.adj_factor(ts_code=tushare_code, start_date=start_date, end_date=end_date)
                adj_factor = safe_df(adj_factor)
                if not adj_factor.empty:
                    df = df.merge(adj_factor, on=['ts_code', 'trade_date'])
                    
                    # 前复权
                    if adj == 'qfq':
                        latest_factor = adj_factor['adj_factor'].iloc[0]
                        df['adj_factor'] = df['adj_factor'] / latest_factor
                        
                    # 应用复权因子
                    for col in ['open', 'high', 'low', 'close']:
                        if col in df.columns:
                            df[col] = df[col] * df['adj_factor']
        
        # 转换TuShare数据格式
        if not df.empty:
            # 创建统一的列名映射
            column_mapping = {
                "trade_date": "date",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "vol": "volume",
                "volume": "volume",  # pro_bar可能使用volume列名
                "amount": "amount",
                "pct_chg": "pct_change",
                "change": "change_amount",
                "turnover_rate": "turnover",
                "turn": "turnover"    # pro_bar可能使用turn列名
            }
            
            # 只重命名存在的列
            rename_cols = {col: new_col for col, new_col in column_mapping.items() if col in df.columns}
            df = df.rename(columns=rename_cols)
            
            # 确保日期格式统一
            if "date" not in df.columns and "trade_date" in df.columns:
                df["date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
            elif "date" in df.columns:
                if df["date"].dtype == object:  # 如果是字符串格式
                    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
            
            # 确保所有数值列已转换为float类型
            for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'pct_change', 'change_amount', 'turnover']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 排序
            df = df.sort_values("date", ascending=True)
            
            logger.info(f"Successfully retrieved data from TuShare: {len(df)} records")
            return df
        return pd.DataFrame()
    
    for attempt in range(max_retries):
        try:
            import tushare as ts
            if not TUSHARE_TOKEN:
                logger.warning("TuShare token not found")
                return pd.DataFrame()
            return fetch_data()
        except ImportError:
            logger.warning("TuShare not installed, returning empty DataFrame")
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for get_tushare_price_data: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(1, 3))  # 随机延时1-3秒后重试
            else:
                logger.error(f"All attempts failed for get_tushare_price_data: {str(e)}")
                return pd.DataFrame()

def get_tushare_financial_metrics(tushare_code: str) -> List[Dict[str, Any]]:
    """从TuShare获取财务指标"""
    def fetch_data():
        import tushare as ts
        if not TUSHARE_TOKEN:
            logger.warning("TuShare token not found")
            return [{}]
        
        ts.set_token(TUSHARE_TOKEN)
        pro = ts.pro_api()
        
        logger.info(f"Fetching financial metrics using TuShare for {tushare_code}")
        
        # 获取基本信息
        basic_info = pro.daily_basic(ts_code=tushare_code)
        basic_info = safe_df(basic_info)
        
        if not basic_info.empty:
            basic_info = basic_info.iloc[0]
            
            # 获取财务指标
            fin_indicator = pro.fina_indicator(ts_code=tushare_code)
            fin_indicator = safe_df(fin_indicator)
            latest_fin = fin_indicator.iloc[0] if not fin_indicator.empty else pd.Series()
            
            # 获取利润表
            income = pro.income(ts_code=tushare_code)
            income = safe_df(income)
            latest_income = income.iloc[0] if not income.empty else pd.Series()
            prev_income = income.iloc[1] if len(income) > 1 else pd.Series()
            
            # 计算增长率
            revenue_current = float(latest_income.get('revenue', 0))
            revenue_prev = float(prev_income.get('revenue', 0))
            revenue_growth = (revenue_current - revenue_prev) / revenue_prev if revenue_prev > 0 else 0
            
            net_profit_current = float(latest_income.get('n_income', 0))
            net_profit_prev = float(prev_income.get('n_income', 0))
            earnings_growth = (net_profit_current - net_profit_prev) / net_profit_prev if net_profit_prev > 0 else 0
            
            # 构建指标字典
            agent_metrics = {
                # 盈利能力指标
                "return_on_equity": float(latest_fin.get("roe", 0)),
                "net_margin": float(latest_fin.get("netprofit_margin", 0)),
                "operating_margin": float(latest_fin.get("profit_dedt", 0)) / float(latest_income.get("revenue", 1)) if float(latest_income.get("revenue", 0)) > 0 else 0,
                
                # 增长指标
                "revenue_growth": revenue_growth,
                "earnings_growth": earnings_growth,
                "book_value_growth": float(latest_fin.get("equity_yoy", 0)),
                
                # 财务健康指标
                "current_ratio": float(latest_fin.get("current_ratio", 0)),
                "debt_to_equity": float(latest_fin.get("debt_to_assets", 0)),
                "free_cash_flow_per_share": float(latest_fin.get("fcff_ps", 0)),
                "earnings_per_share": float(latest_fin.get("eps", 0)),
                
                # 估值比率
                "pe_ratio": float(basic_info.get("pe", 0)),
                "price_to_book": float(basic_info.get("pb", 0)),
                "price_to_sales": float(basic_info.get("ps", 0)),
            }
            
            return [agent_metrics]
        return [{}]
    
    for attempt in range(max_retries):
        try:
            import tushare as ts
            if not TUSHARE_TOKEN:
                logger.warning("TuShare token not found")
                return [{}]
            return fetch_data()
        except ImportError:
            logger.warning("TuShare not installed, returning empty list")
            return [{}]
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for get_tushare_financial_metrics: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(1, 3))
            else:
                logger.error(f"All attempts failed for get_tushare_financial_metrics: {str(e)}")
                return [{}]

def get_tushare_financial_statements(tushare_code: str) -> List[Dict[str, Any]]:
    """从TuShare获取财务报表数据"""
    def fetch_data():
        import tushare as ts
        if not TUSHARE_TOKEN:
            logger.warning("TuShare token not found")
            return [{}, {}]
        
        ts.set_token(TUSHARE_TOKEN)
        pro = ts.pro_api()
        
        logger.info(f"Fetching financial statements using TuShare for {tushare_code}")
        
        # 获取资产负债表
        balance = pro.balancesheet(ts_code=tushare_code)
        balance = safe_df(balance)
        if not balance.empty:
            latest_balance = balance.iloc[0]
            previous_balance = balance.iloc[1] if len(balance) > 1 else balance.iloc[0]
            
            # 获取利润表
            income = pro.income(ts_code=tushare_code)
            income = safe_df(income)
            latest_income = income.iloc[0] if not income.empty else pd.Series()
            previous_income = income.iloc[1] if len(income) > 1 else income.iloc[0]
            
            # 获取现金流量表
            cashflow = pro.cashflow(ts_code=tushare_code)
            cashflow = safe_df(cashflow)
            latest_cashflow = cashflow.iloc[0] if not cashflow.empty else pd.Series()
            previous_cashflow = cashflow.iloc[1] if len(cashflow) > 1 else cashflow.iloc[0]
            
            # 构建财务数据项
            current_item = {
                "net_income": float(latest_income.get("n_income", 0)),
                "operating_revenue": float(latest_income.get("revenue", 0)),
                "operating_profit": float(latest_income.get("operate_profit", 0)),
                "working_capital": float(latest_balance.get("total_cur_assets", 0)) - float(latest_balance.get("total_cur_liab", 0)),
                "depreciation_and_amortization": float(latest_cashflow.get("depreciation_amort_cba", 0)),
                "capital_expenditure": abs(float(latest_cashflow.get("stot_outflows_inv_act", 0))),
                "free_cash_flow": float(latest_cashflow.get("n_cashflow_act", 0)) - abs(float(latest_cashflow.get("stot_outflows_inv_act", 0)))
            }
            
            previous_item = {
                "net_income": float(previous_income.get("n_income", 0)),
                "operating_revenue": float(previous_income.get("revenue", 0)),
                "operating_profit": float(previous_income.get("operate_profit", 0)),
                "working_capital": float(previous_balance.get("total_cur_assets", 0)) - float(previous_balance.get("total_cur_liab", 0)),
                "depreciation_and_amortization": float(previous_cashflow.get("depreciation_amort_cba", 0)),
                "capital_expenditure": abs(float(previous_cashflow.get("stot_outflows_inv_act", 0))),
                "free_cash_flow": float(previous_cashflow.get("n_cashflow_act", 0)) - abs(float(previous_cashflow.get("stot_outflows_inv_act", 0)))
            }
            
            return [current_item, previous_item]
        return [{}, {}]
    
    for attempt in range(max_retries):
        try:
            import tushare as ts
            if not TUSHARE_TOKEN:
                logger.warning("TuShare token not found")
                return [{}, {}]
            return fetch_data()
        except ImportError:
            logger.warning("TuShare not installed, returning empty list")
            return [{}, {}]
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for get_tushare_financial_statements: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(1, 3))
            else:
                logger.error(f"All attempts failed for get_tushare_financial_statements: {str(e)}")
                return [{}, {}]

def get_tushare_market_data(tushare_code: str) -> Dict[str, Any]:
    """从TuShare获取市场数据"""
    def fetch_data():
        import tushare as ts
        if not TUSHARE_TOKEN:
            logger.warning("TuShare token not found")
            return {}
        
        ts.set_token(TUSHARE_TOKEN)
        pro = ts.pro_api()
        
        logger.info(f"Fetching market data using TuShare for {tushare_code}")
        
        # 获取基本行情
        daily_basic = pro.daily_basic(ts_code=tushare_code)
        daily_basic = safe_df(daily_basic)
        if not daily_basic.empty:
            latest_data = daily_basic.iloc[0]
            
            # 获取历史数据计算52周最高最低
            today = datetime.now()
            start_date = (today - timedelta(days=365)).strftime('%Y%m%d')
            end_date = today.strftime('%Y%m%d')
            
            hist_data = pro.daily(ts_code=tushare_code, start_date=start_date, end_date=end_date)
            hist_data = safe_df(hist_data)
            
            if not hist_data.empty:
                week_high = hist_data["high"].max()
                week_low = hist_data["low"].min()
                avg_volume = hist_data.head(30)["vol"].mean()
            else:
                week_high = latest_data.get("high", 0)
                week_low = latest_data.get("low", 0)
                avg_volume = latest_data.get("vol", 0)
            
            return {
                "market_cap": float(latest_data.get("total_mv", 0)),  # 总市值
                "volume": float(latest_data.get("vol", 0)),           # 成交量
                "average_volume": float(avg_volume),                   # 平均成交量
                "fifty_two_week_high": float(week_high),              # 52周最高
                "fifty_two_week_low": float(week_low)                 # 52周最低
            }
        return {}
    
    for attempt in range(max_retries):
        try:
            import tushare as ts
            if not TUSHARE_TOKEN:
                logger.warning("TuShare token not found")
                return {}
            return fetch_data()
        except ImportError:
            logger.warning("TuShare not installed, returning empty dict")
            return {}
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for get_tushare_market_data: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(1, 3))
            else:
                logger.error(f"All attempts failed for get_tushare_market_data: {str(e)}")
                return {}