import pandas as pd
import traceback
from datetime import datetime, timedelta
from typing import Dict, Any, List
from .retry_decorator import retry
from .cache import get_cached_data
from .akshare_adapter import get_akshare_price_data, get_akshare_financial_metrics, get_akshare_financial_statements, get_akshare_market_data
from .tushare_adapter import get_tushare_price_data, get_tushare_financial_metrics, get_tushare_financial_statements, get_tushare_market_data
from src.utils.logging_config import setup_logger

logger = setup_logger('data_source_adapter')

class DataSourceAdapter:
    """数据源适配器，支持从AKShare和TuShare获取数据"""
    
    @staticmethod
    def convert_stock_code(symbol: str) -> tuple:
        """
        转换股票代码格式，返回AKShare和TuShare对应的代码格式
        返回元组: (akshare_code, tushare_code, exchange_prefix)
        """
        # 对已有代码格式的兼容
        if symbol.startswith(('sh', 'sz', 'bj')):
            # 已经带有交易所前缀的情况
            exchange_prefix = symbol[:2]
            code = symbol[2:]
        else:
            # 根据代码判断交易所
            if symbol.startswith('6'):
                exchange_prefix = 'sh'
            elif symbol.startswith(('0', '3')):
                exchange_prefix = 'sz'
            elif symbol.startswith('4'):
                exchange_prefix = 'bj'
            else:
                exchange_prefix = 'sh'  # 默认使用上交所
            code = symbol
        
        # AKShare格式
        akshare_code = symbol
        # TuShare格式: 代码.交易所缩写 (sh->SH, sz->SZ, bj->BJ)
        tushare_code = f"{code}.{exchange_prefix.upper()}"
        
        return akshare_code, tushare_code, exchange_prefix

    @retry(max_tries=3, delay_seconds=2)
    def get_price_history(self, symbol: str, start_date: str = None, end_date: str = None, adjust: str = "qfq") -> pd.DataFrame:
        """
        获取历史价格数据，优先使用AKShare，失败时切换到TuShare
        
        Args:
            symbol: 股票代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            adjust: 复权类型，qfq: 前复权, hfq: 后复权, "": 不复权
            
        Returns:
            包含价格数据的DataFrame
        """
        logger.info(f"Getting price history for {symbol} from {start_date} to {end_date}")
        
        # 处理日期参数
        start_date, end_date = self._process_dates(start_date, end_date)
        
        # 转换股票代码
        akshare_code, tushare_code, exchange_prefix = self.convert_stock_code(symbol)
        
        # 缓存键
        cache_key = f"price_hist_{symbol}_{start_date}_{end_date}_{adjust}"
        
        # 如果是当日数据，直接获取不使用缓存
        current_date = datetime.now()
        end_date_obj = datetime.strptime(end_date, "%Y%m%d") if isinstance(end_date, str) else end_date
        if (current_date - end_date_obj).days < 1:
            df = self._fetch_price_history(akshare_code, tushare_code, start_date, end_date, adjust)
        else:
            # 使用缓存
            df_dict = get_cached_data(
                cache_key, 
                lambda: self._fetch_price_history(akshare_code, tushare_code, start_date, end_date, adjust), 
                ttl_days=3
            )
            
            if isinstance(df_dict, list):
                df = pd.DataFrame(df_dict)
                if not df.empty and 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                logger.info(f"Successfully retrieved {len(df)} records from cache")
            else:
                df = pd.DataFrame()
                
        return df
    
    def _process_dates(self, start_date, end_date):
        """处理日期参数"""
        current_date = datetime.now()
        yesterday = current_date - timedelta(days=1)
        
        if not end_date:
            end_date = yesterday
        else:
            end_date = datetime.strptime(end_date, "%Y-%m-%d") if isinstance(end_date, str) else end_date
            if end_date > yesterday:
                end_date = yesterday
                
        if not start_date:
            start_date = end_date - timedelta(days=365)
        else:
            start_date = datetime.strptime(start_date, "%Y-%m-%d") if isinstance(start_date, str) else start_date
            
        # 转换为字符串格式用于API调用    
        start_date_str = start_date.strftime("%Y%m%d") if isinstance(start_date, datetime) else start_date
        end_date_str = end_date.strftime("%Y%m%d") if isinstance(end_date, datetime) else end_date
        
        return start_date_str, end_date_str
    
    def _fetch_price_history(self, akshare_code: str, tushare_code: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame:
        """内部方法：从数据源获取价格历史数据"""
        df = pd.DataFrame()
        
        # 首先尝试使用AKShare
        # try:
        #     df = get_akshare_price_data(akshare_code, start_date, end_date, adjust)
        #     if not df.empty:
        #         return df
        # except Exception as e:
        #     logger.warning(f"AKShare data fetch failed: {str(e)}")
        #     logger.debug(f"AKShare error details: {traceback.format_exc()}")
        #     logger.info("Falling back to TuShare...")
        
        # 如果AKShare失败或不可用，尝试使用TuShare
        try:
            df = get_tushare_price_data(tushare_code, start_date, end_date, adjust)
            if not df.empty:
                return df
            else:
                logger.warning("TuShare returned empty DataFrame")
        except Exception as e:
            logger.error(f"TuShare data fetch failed: {str(e)}")
            logger.debug(f"TuShare error details: {traceback.format_exc()}")
        
        # 如果两种数据源都失败，返回空DataFrame
        if df.empty:
            logger.warning("Both AKShare and TuShare data fetch failed, returning empty DataFrame")
        
        return df
    
    @retry(max_tries=3, delay_seconds=2)
    def get_financial_metrics(self, symbol: str) -> Dict[str, Any]:
        """
        获取财务指标数据，优先使用AKShare，失败时切换到TuShare
        
        Args:
            symbol: 股票代码
            
        Returns:
            包含财务指标的字典
        """
        logger.info(f"Getting financial metrics for {symbol}")
        
        # 转换股票代码
        akshare_code, tushare_code, exchange_prefix = self.convert_stock_code(symbol)
        
        # 缓存键
        cache_key = f"fin_metrics_{symbol}"
        
        # 使用缓存
        metrics = get_cached_data(
            cache_key,
            lambda: self._fetch_financial_metrics(akshare_code, tushare_code, exchange_prefix),
            ttl_days=1  # 财务数据每天更新一次即可
        )
        
        return metrics
    
    def _fetch_financial_metrics(self, akshare_code: str, tushare_code: str, exchange_prefix: str) -> List[Dict[str, Any]]:
        """内部方法：从数据源获取财务指标"""
        metrics = [{}]  # 默认返回一个空字典的列表
        
        # 首先尝试使用AKShare
        # try:
        #     metrics = get_akshare_financial_metrics(akshare_code, exchange_prefix)
        #     if metrics != [{}]:
        #         return metrics
        # except Exception as e:
        #     logger.warning(f"AKShare financial metrics fetch failed: {str(e)}")
        #     logger.info("Falling back to TuShare...")
        
        # 如果AKShare失败或不可用，尝试使用TuShare
        try:
            metrics = get_tushare_financial_metrics(tushare_code)
            return metrics
        except Exception as e:
            logger.error(f"TuShare financial metrics fetch failed: {str(e)}")
        
        return metrics  # 返回默认的空指标列表
    
    @retry(max_tries=3, delay_seconds=2)
    def get_financial_statements(self, symbol: str) -> List[Dict[str, Any]]:
        """
        获取财务报表数据，优先使用AKShare，失败时切换到TuShare
        
        Args:
            symbol: 股票代码
            
        Returns:
            包含财务报表数据的字典列表
        """
        logger.info(f"Getting financial statements for {symbol}")
        
        # 转换股票代码
        akshare_code, tushare_code, exchange_prefix = self.convert_stock_code(symbol)
        
        # 缓存键
        cache_key = f"fin_statements_{symbol}"
        
        # 使用缓存
        statements = get_cached_data(
            cache_key,
            lambda: self._fetch_financial_statements(akshare_code, tushare_code, exchange_prefix),
            ttl_days=7  # 财务报表可以缓存更长时间
        )
        
        return statements
    
    def _fetch_financial_statements(self, akshare_code: str, tushare_code: str, exchange_prefix: str) -> List[Dict[str, Any]]:
        """内部方法：从数据源获取财务报表"""
        default_items = [
            {
                "net_income": 0,
                "operating_revenue": 0,
                "operating_profit": 0,
                "working_capital": 0,
                "depreciation_and_amortization": 0,
                "capital_expenditure": 0,
                "free_cash_flow": 0
            },
            {
                "net_income": 0,
                "operating_revenue": 0,
                "operating_profit": 0,
                "working_capital": 0,
                "depreciation_and_amortization": 0,
                "capital_expenditure": 0,
                "free_cash_flow": 0
            }
        ]
        
        # 首先尝试使用AKShare
        # try:
        #     statements = get_akshare_financial_statements(akshare_code, exchange_prefix)
        #     if statements != default_items:
        #         return statements
        # except Exception as e:
        #     logger.warning(f"AKShare financial statements fetch failed: {str(e)}")
        #     logger.info("Falling back to TuShare...")
        
        # 如果AKShare失败或不可用，尝试使用TuShare
        try:
            statements = get_tushare_financial_statements(tushare_code)
            return statements
        except Exception as e:
            logger.error(f"TuShare financial statements fetch failed: {str(e)}")
        
        return default_items
    
    @retry(max_tries=3, delay_seconds=2)
    def get_market_data(self, symbol: str) -> Dict[str, Any]:
        """
        获取市场数据，现在包含行业指标
        
        Args:
            symbol: 股票代码
            
        Returns:
            包含市场数据和行业指标的字典
        """
        logger.info(f"Getting market data for {symbol}")
        
        # 转换股票代码
        akshare_code, tushare_code, exchange_prefix = self.convert_stock_code(symbol)
        
        # 缓存键
        cache_key = f"market_data_with_industry_{symbol}"
        
        # 市场数据更新频率高，使用较短的缓存时间
        market_data = get_cached_data(
            cache_key,
            lambda: self._fetch_market_data_with_industry(akshare_code, tushare_code),
            ttl_days=0.5  # 12小时
        )
        
        return market_data
    
    def _fetch_market_data_with_industry(self, akshare_code: str, tushare_code: str) -> Dict[str, Any]:
        """内部方法：获取包含行业数据的市场信息"""
        # 获取基础市场数据
        market_data = self._fetch_market_data(akshare_code, tushare_code)
        
        # 添加行业指标
        industry_data = self._fetch_industry_metrics(akshare_code)
        
        # 合并数据
        combined_data = {**market_data, **industry_data}
        return combined_data
    
    def _fetch_market_data(self, akshare_code: str, tushare_code: str) -> Dict[str, Any]:
        """内部方法：从数据源获取市场数据"""
        default_data = {
            "market_cap": 0,
            "volume": 0,
            "average_volume": 0,
            "fifty_two_week_high": 0,
            "fifty_two_week_low": 0
        }
        
        # 首先尝试使用AKShare
        # try:
        #     market_data = get_akshare_market_data(akshare_code)
        #     if market_data != default_data:
        #         return market_data
        # except Exception as e:
        #     logger.warning(f"AKShare market data fetch failed: {str(e)}")
        #     logger.info("Falling back to TuShare...")
        
        # 如果AKShare失败或不可用，尝试使用TuShare
        try:
            market_data = get_tushare_market_data(tushare_code)
            return market_data
        except Exception as e:
            logger.error(f"TuShare market data fetch failed: {str(e)}")
        
        return default_data
    
    retry(max_tries=3, delay_seconds=2)
    def get_industry_metrics(self, symbol: str) -> Dict[str, Any]:
        """
        获取行业指标数据
        
        Args:
            symbol: 股票代码
            
        Returns:
            包含行业指标的字典
        """
        logger.info(f"Getting industry metrics for {symbol}")
        
        # 转换股票代码
        akshare_code, tushare_code, exchange_prefix = self.convert_stock_code(symbol)
        
        # 缓存键
        cache_key = f"industry_metrics_{symbol}"
        
        # 使用缓存
        industry_metrics = get_cached_data(
            cache_key,
            lambda: self._fetch_industry_metrics(akshare_code),
            ttl_days=1  # 行业数据每天更新一次即可
        )
        
        return industry_metrics
    
    def _fetch_industry_metrics(self, akshare_code: str) -> Dict[str, Any]:
        """内部方法：从数据源获取行业指标"""
        from .industry import query_industry_metrics
        
        try:
            # 使用行业模块查询指标
            industry_data = query_industry_metrics(akshare_code)
            return industry_data
        except Exception as e:
            logger.error(f"Industry metrics fetch failed: {str(e)}")
            # 返回默认值
            return {
                "stock": akshare_code,
                "industry": "未知行业",
                "industry_avg_pe": 15,
                "industry_avg_pb": 1.5,
                "industry_growth": 0.05
            }