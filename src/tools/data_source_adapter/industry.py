import os
import pandas as pd
from datetime import datetime, timedelta
import time
import random
from .cache import get_cached_data
from src.utils.logging_config import setup_logger

logger = setup_logger('tushare_industry_adapter')

# TuShare API密钥
TUSHARE_TOKEN = 'f7d9df1785ff7ab8fc18f6cc175c12f56a7b5a14176a385d09dfc505'

def get_tushare_data_with_retry(fetch_func, *args, max_retries: int = 3, **kwargs):
    """
    通用的TuShare数据获取函数，带重试机制
    """
    for attempt in range(max_retries):
        try:
            import tushare as ts
            if not TUSHARE_TOKEN:
                logger.warning("TuShare token not found")
                return None
            
            ts.set_token(TUSHARE_TOKEN)
            pro = ts.pro_api()
            
            # 将pro对象传递给fetch函数
            result = fetch_func(pro, *args, **kwargs)
            if result is not None and not (isinstance(result, pd.DataFrame) and result.empty):
                return result
            else:
                logger.warning(f"Attempt {attempt + 1}: Empty result returned")
        except ImportError:
            logger.error("TuShare not installed")
            break
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
        
        if attempt < max_retries - 1:
            delay = random.uniform(1, 3)
            logger.info(f"Waiting {delay:.2f} seconds before retry...")
            time.sleep(delay)
    
    logger.error(f"All {max_retries} attempts failed")
    return None

def get_industry_by_code(stock_code: str) -> str:
    """
    从TuShare获取股票所属行业
    """
    cache_key = f"tushare_industry_by_code_{stock_code}"
    
    def _fetch_industry():
        import tushare as ts
        if not TUSHARE_TOKEN:
            logger.warning("TuShare token not found")
            return "未知行业"
        
        ts.set_token(TUSHARE_TOKEN)
        pro = ts.pro_api()
        
        # 获取股票基本信息，包含行业信息
        try:
            # 获取股票基本信息
            stock_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,industry')
            stock_info = stock_basic[stock_basic['ts_code'].str.contains(stock_code)]
            
            if not stock_info.empty:
                return stock_info.iloc[0]['industry']
            else:
                logger.warning(f"未找到股票 {stock_code} 的行业信息")
                return "未知行业"
        except Exception as e:
            logger.error(f"获取股票 {stock_code} 行业信息失败: {str(e)}")
            return "未知行业"
    
    # 使用较长缓存期，因为行业分类不常变化
    try:
        industry = get_cached_data(cache_key, _fetch_industry, ttl_days=30)
        logger.info(f"获取股票 {stock_code} 所属行业: {industry}")
        return industry
    except Exception as e:
        logger.error(f"获取行业信息失败: {e}")
        return "未知行业"

def get_industry_valuation(industry: str) -> tuple[float, float]:
    """
    从TuShare获取行业估值数据
    注意：TuShare没有直接的行业估值API，这里使用行业成分股的平均估值
    """
    cache_key = f"tushare_industry_valuation_{industry.replace(' ', '_')}"
    
    def _fetch_valuation():
        import tushare as ts
        if not TUSHARE_TOKEN:
            logger.warning("TuShare token not found")
            return 15.0, 1.5
        
        ts.set_token(TUSHARE_TOKEN)
        pro = ts.pro_api()
        
        try:
            # 获取属于该行业的所有股票
            stock_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,industry')
            industry_stocks = stock_basic[stock_basic['industry'] == industry]
            
            if industry_stocks.empty:
                logger.warning(f"未找到行业 {industry} 的成分股")
                return 15.0, 1.5
            
            # 获取这些股票的估值数据
            stock_codes = industry_stocks['ts_code'].tolist()[:50]  # 限制数量避免API限制
            
            # 获取基本面数据
            daily_basic_list = []
            for i in range(0, len(stock_codes), 50):  # 分批获取
                batch_codes = stock_codes[i:i+50]
                batch_str = ','.join(batch_codes)
                try:
                    batch_basic = pro.daily_basic(ts_code=batch_str)
                    daily_basic_list.append(batch_basic)
                except Exception as e:
                    logger.warning(f"获取批次数据失败: {str(e)}")
                    continue
            
            if daily_basic_list:
                all_basic = pd.concat(daily_basic_list, ignore_index=True)
                
                # 计算行业平均PE和PB
                valid_pe = all_basic[all_basic['pe'] > 0]['pe']
                valid_pb = all_basic[all_basic['pb'] > 0]['pb']
                
                avg_pe = valid_pe.mean() if not valid_pe.empty else 15.0
                avg_pb = valid_pb.mean() if not valid_pb.empty else 1.5
                
                return float(avg_pe), float(avg_pb)
            else:
                logger.warning(f"未能获取行业 {industry} 的估值数据")
                return 15.0, 1.5
                
        except Exception as e:
            logger.error(f"获取行业估值失败: {str(e)}")
            return 15.0, 1.5
    
    try:
        # 行业估值指标每天更新一次即可
        result = get_cached_data(cache_key, _fetch_valuation, ttl_days=1)
        logger.info(f"获取行业 {industry} 估值: PE={result[0]}, PB={result[1]}")
        return result
    except Exception as e:
        logger.error(f"获取行业估值失败: {e}")
        return 15.0, 1.5

def get_industry_growth(industry: str, window_days: int = 252) -> float:
    """
    计算行业近 window_days 个交易日的平均涨幅
    使用行业成分股的平均表现作为行业表现
    """
    cache_key = f"tushare_industry_growth_{industry.replace(' ', '_')}_{window_days}days"
    
    def _fetch_growth():
        import tushare as ts
        if not TUSHARE_TOKEN:
            logger.warning("TuShare token not found")
            return 0.05
        
        ts.set_token(TUSHARE_TOKEN)
        pro = ts.pro_api()
        
        try:
            # 获取属于该行业的股票
            stock_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,industry')
            industry_stocks = stock_basic[stock_basic['industry'] == industry]
            
            if industry_stocks.empty:
                logger.warning(f"未找到行业 {industry} 的成分股")
                return 0.05
            
            # 获取最近的交易日数据
            today = datetime.today()
            end_date = today.strftime('%Y%m%d')
            start_date = (today - timedelta(days=window_days*1.4)).strftime('%Y%m%d')
            
            # 选择部分股票计算平均涨幅（避免API限制）
            sample_stocks = industry_stocks.head(20)['ts_code'].tolist()
            
            growth_rates = []
            for stock_code in sample_stocks:
                try:
                    # 获取股票历史数据
                    hist = pro.daily(ts_code=stock_code, start_date=start_date, end_date=end_date)
                    if not hist.empty and len(hist) >= 2:
                        # 计算期间涨幅
                        hist_sorted = hist.sort_values('trade_date')
                        hist_window = hist_sorted.tail(window_days)
                        if len(hist_window) >= 2:
                            start_price = hist_window.iloc[0]['close']
                            end_price = hist_window.iloc[-1]['close']
                            if start_price > 0:
                                growth_rate = (end_price - start_price) / start_price
                                growth_rates.append(growth_rate)
                except Exception as e:
                    logger.warning(f"计算股票 {stock_code} 涨幅失败: {str(e)}")
                    continue
            
            if growth_rates:
                avg_growth = sum(growth_rates) / len(growth_rates)
                return round(avg_growth, 4)
            else:
                logger.warning(f"未能计算行业 {industry} 的涨幅数据")
                return 0.05
                
        except Exception as e:
            logger.error(f"计算行业涨幅失败: {str(e)}")
            return 0.05
    
    try:
        # 历史涨幅每小时更新一次即可
        growth = get_cached_data(cache_key, _fetch_growth, ttl_days=0.04)  # 约1小时
        logger.info(f"获取行业 {industry} 近 {window_days} 日涨幅: {growth*100:.2f}%")
        return growth
    except Exception as e:
        logger.error(f"获取行业涨幅失败: {e}")
        return 0.05

def query_industry_metrics(stock_code: str) -> dict:
    """
    综合查询股票的行业指标
    """
    try:
        logger.info(f"开始查询股票 {stock_code} 的行业指标")
        ind = get_industry_by_code(stock_code)
        
        if ind == "未知行业":
            logger.warning(f"无法确定股票 {stock_code} 的行业")
            return {
                "stock": stock_code,
                "industry": "未知行业",
                "industry_avg_pe": 15,
                "industry_avg_pb": 1.5,
                "industry_growth": 0.05
            }
        
        pe, pb = get_industry_valuation(ind)
        growth = get_industry_growth(ind)        # 近一年涨幅
        
        result = {
            "stock": stock_code,
            "industry": ind,
            "industry_avg_pe": pe,
            "industry_avg_pb": pb,
            "industry_growth": growth
        }
        logger.info(f"成功获取行业指标: {result}")
        return result
    except Exception as e:
        logger.error(f"查询行业指标失败: {e}")
        # 返回默认值
        return {
            "stock": stock_code,
            "industry": "未知行业",
            "industry_avg_pe": 15,
            "industry_avg_pb": 1.5,
            "industry_growth": 0.05
        }

# 测试函数
if __name__ == "__main__":
    res = query_industry_metrics("600519")
    print(res)