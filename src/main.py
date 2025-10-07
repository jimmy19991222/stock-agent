import sys
import argparse
import uuid
import threading
import uvicorn
import logging

from datetime import datetime, timedelta
from langgraph.graph import END, StateGraph
from langchain_core.messages import HumanMessage
import pandas as pd
import akshare as ak

# --- Agent Imports ---
from src.agents.valuation import valuation_agent
from src.agents.state import AgentState
from src.agents.sentiment import sentiment_agent
from src.agents.risk_manager import risk_management_agent
from src.agents.technicals import technical_analyst_agent
from src.agents.portfolio_manager import portfolio_management_agent
from src.agents.market_data import market_data_agent
from src.agents.fundamentals import fundamentals_agent
from src.agents.researcher_bull import researcher_bull_agent
from src.agents.researcher_bear import researcher_bear_agent
from src.agents.debate_room import debate_room_agent
from src.agents.macro_analyst import macro_analyst_agent
from src.agents.portfolio_analyzer import portfolio_analyzer_agent
from src.agents.ai_model_analyst import ai_model_analyst_agent 

# --- Logging Imports ---
from src.utils.output_logger import SimpleConsoleLogger
from src.utils.logging_config import setup_global_logging, set_console_level
from src.utils.llm_interaction_logger import (
    set_global_log_storage,
    get_log_storage,
)
from src.utils.api_utils import app as fastapi_app
from src.utils.json_unicode_handler import monkey_patch_all_agents, patch_json_dumps


patch_json_dumps()  # 修改默认json.dumps行为
monkey_patch_all_agents()  # 修补所有代理类

# --- Import Summary Report Generator ---
try:
    from src.utils.summary_report import print_summary_report
    from src.utils.agent_collector import store_final_state, get_enhanced_final_state
    HAS_SUMMARY_REPORT = True
except ImportError:
    HAS_SUMMARY_REPORT = False

# --- Import Structured Terminal Output ---
try:
    from src.utils.structured_terminal import print_structured_output
    HAS_STRUCTURED_OUTPUT = True
except ImportError:
    HAS_STRUCTURED_OUTPUT = False

logger = logging.getLogger(__name__)

# Initialize logging system
# 设置全局日志配置：控制台只显示WARNING及以上，文件记录所有级别
setup_global_logging(console_level=logging.INFO, file_level=logging.DEBUG)

# Use simple console logger that doesn't create additional files
sys.stdout = SimpleConsoleLogger()

# 1. Initialize Log Storage
log_storage = get_log_storage()
set_global_log_storage(log_storage)


# --- Run the Hedge Fund Workflow ---
def run_hedge_fund(run_id: str, ticker: str, start_date: str, end_date: str, portfolio: dict, 
                   show_reasoning: bool = False, num_of_news: int = 5, show_summary: bool = False,
                   tickers = None):
    print(f"--- Starting Workflow Run ID: {run_id} ---")

    # 设置API状态
    try:
        from src.utils.api_utils import api_state
        api_state.current_run_id = run_id
        print(f"--- API State updated with Run ID: {run_id} ---")
    except Exception as e:
        print(f"Note: Could not update API state: {str(e)}")
    
    ticker_list = None
    if tickers:
        if isinstance(tickers, str):
            # 如果是字符串，则按逗号分割
            ticker_list = [t.strip() for t in tickers.split(',')]
        elif isinstance(tickers, list):
            # 如果已经是列表，直接使用
            ticker_list = tickers
        else:
            # 其他类型，尝试转换为字符串
            try:
                ticker_list = [str(tickers)]
            except:
                ticker_list = None
                
        if ticker_list:
            print(f"--- Processing multiple tickers: {ticker_list} ---")
            # 确保主要ticker也在列表中
            if ticker not in ticker_list:
                ticker_list.insert(0, ticker)
    
    initial_state = {
        "messages": [
            HumanMessage(
                content="Make a trading decision based on the provided data.",
            )
        ],
        "data": {
            "ticker": ticker,
            "portfolio": portfolio,
            "start_date": start_date,
            "end_date": end_date,
            "num_of_news": num_of_news,
        },
        "metadata": {
            "show_reasoning": show_reasoning,
            "run_id": run_id,  # Pass run_id in metadata
            "show_summary": show_summary,  # 是否显示汇总报告
        }
    }
    
    # 如果有多个股票代码，添加到初始状态中
    if ticker_list:
        initial_state["data"]["tickers"] = ticker_list
    
    # 直接执行工作流
    final_state = app.invoke(initial_state)
    print(f"--- Finished Workflow Run ID: {run_id} ---")

    # 在工作流结束后保存最终状态并生成汇总报告（如果启用）
    if HAS_SUMMARY_REPORT and show_summary:
        # 保存最终状态到收集器
        store_final_state(final_state)
        # 获取增强的最终状态（包含所有收集到的数据）
        enhanced_state = get_enhanced_final_state()
        # 打印汇总报告
        print_summary_report(enhanced_state)

    # 如果启用了显示推理，显示结构化输出
    if HAS_STRUCTURED_OUTPUT and show_reasoning:
        print_structured_output(final_state)

    # 尝试更新API状态
    try:
        from src.utils.api_utils import api_state
        api_state.update_agent_data(run_id, "status", "completed")
    except Exception:
        pass

    # 保持原有的返回格式：最后一条消息的内容
    return final_state["messages"][-1].content


# --- 定义工作流图 ---
workflow = StateGraph(AgentState)

# 添加节点
workflow.add_node("market_data_agent", market_data_agent)
workflow.add_node("ai_model_analyst_agent", ai_model_analyst_agent)
workflow.add_node("technical_analyst_agent", technical_analyst_agent)
workflow.add_node("fundamentals_agent", fundamentals_agent)
workflow.add_node("sentiment_agent", sentiment_agent)
workflow.add_node("valuation_agent", valuation_agent)
workflow.add_node("portfolio_analyzer_agent", portfolio_analyzer_agent)
workflow.add_node("researcher_bull_agent", researcher_bull_agent)
workflow.add_node("researcher_bear_agent", researcher_bear_agent)
workflow.add_node("debate_room_agent", debate_room_agent)
workflow.add_node("risk_management_agent", risk_management_agent)
workflow.add_node("macro_analyst_agent", macro_analyst_agent)
workflow.add_node("portfolio_management_agent", portfolio_management_agent)

# 定义工作流边缘
workflow.set_entry_point("market_data_agent")

# 市场数据到各分析师 - 第一层
workflow.add_edge("market_data_agent", "ai_model_analyst_agent")
workflow.add_edge("market_data_agent", "technical_analyst_agent")
workflow.add_edge("market_data_agent", "fundamentals_agent")
workflow.add_edge("market_data_agent", "sentiment_agent")
workflow.add_edge("market_data_agent", "valuation_agent")
workflow.add_edge("market_data_agent", "portfolio_analyzer_agent")

# 确保所有第一层分析完成后再进行研究员分析 - 第二层
workflow.add_conditional_edges(
    "ai_model_analyst_agent",
    lambda x: "researcher_bull_agent",
)
workflow.add_conditional_edges(
    "technical_analyst_agent",
    lambda x: "researcher_bull_agent" if "researcher_bull_agent" not in [msg.name for msg in x["messages"]] else "researcher_bear_agent",
)
workflow.add_conditional_edges(
    "fundamentals_agent",
    lambda x: "researcher_bull_agent" if "researcher_bull_agent" not in [msg.name for msg in x["messages"]] else "researcher_bear_agent",
)
workflow.add_conditional_edges(
    "sentiment_agent",
    lambda x: "researcher_bull_agent" if "researcher_bull_agent" not in [msg.name for msg in x["messages"]] else "researcher_bear_agent",
)
workflow.add_conditional_edges(
    "valuation_agent",
    lambda x: "researcher_bull_agent" if "researcher_bull_agent" not in [msg.name for msg in x["messages"]] else "researcher_bear_agent",
)
workflow.add_conditional_edges(
    "portfolio_analyzer_agent",
    lambda x: "researcher_bull_agent" if "researcher_bull_agent" not in [msg.name for msg in x["messages"]] else "researcher_bear_agent",
)

# 研究员到辩论室 - 第三层
workflow.add_conditional_edges(
    "researcher_bull_agent",
    lambda x: "researcher_bear_agent" if "researcher_bear_agent" not in [msg.name for msg in x["messages"]] else "debate_room_agent",
)
workflow.add_edge("researcher_bear_agent", "debate_room_agent")

# 辩论室到风险管理 - 第四层
workflow.add_edge("debate_room_agent", "risk_management_agent")

# 风险管理到宏观分析 - 第五层
workflow.add_edge("risk_management_agent", "macro_analyst_agent")

# 宏观分析到投资组合管理 - 第六层
workflow.add_edge("macro_analyst_agent", "portfolio_management_agent")
workflow.add_edge("portfolio_management_agent", END)

# 编译工作流图
app = workflow.compile()


# --- FastAPI Background Task ---
def run_fastapi():
    print("--- Starting FastAPI server in background (port 8000) ---")
    # Disable Uvicorn's own logging config to avoid conflicts with app's logging
    uvicorn.run(fastapi_app, host="0.0.0.0", port=8000, log_config=None)


# --- Main Execution Block ---
if __name__ == "__main__":
    # Start FastAPI server in a background thread
    fastapi_thread = threading.Thread(target=run_fastapi, daemon=True)
    fastapi_thread.start()

    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(
        description='Run the hedge fund trading system')
    parser.add_argument('--ticker', type=str, required=True,
                        help='Primary stock ticker symbol')
    parser.add_argument('--tickers', type=str,
                        help='Multiple stock tickers separated by commas (e.g., "600519,000858,601398")')
    parser.add_argument('--start-date', type=str,
                        help='Start date (YYYY-MM-DD). Defaults to 1 year before end date')
    parser.add_argument('--end-date', type=str,
                        help='End date (YYYY-MM-DD). Defaults to yesterday')
    parser.add_argument('--show-reasoning', action='store_true',
                        help='Show reasoning from each agent')
    parser.add_argument('--num-of-news', type=int, default=5,
                        help='Number of news articles to analyze for sentiment (default: 5)')
    parser.add_argument('--initial-capital', type=float, default=100000.0,
                        help='Initial cash amount (default: 100,000)')
    parser.add_argument('--initial-position', type=int, default=0,
                        help='Initial stock position (default: 0)')
    parser.add_argument('--summary', action='store_true',
                        help='Show beautiful summary report at the end')
    parser.add_argument('--log-level', type=str, default='DEBUG',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Console log level (default: WARNING)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose output (equivalent to --log-level INFO)')

    args = parser.parse_args()

    # 根据参数设置日志级别
    if args.verbose:
        log_level = logging.INFO
    else:
        log_level = getattr(logging, args.log_level.upper())
    
    # 重新设置控制台日志级别
    set_console_level(log_level)
    
    print(f"日志级别设置为: {logging.getLevelName(log_level)}")

    # --- Date Handling ---
    current_date = datetime.now()
    yesterday = current_date - timedelta(days=1)
    end_date = yesterday if not args.end_date else min(
        datetime.strptime(args.end_date, '%Y-%m-%d'), yesterday)

    if not args.start_date:
        start_date = end_date - timedelta(days=365)
    else:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')

    if start_date > end_date:
        raise ValueError("Start date cannot be after end date")
    if args.num_of_news < 1:
        raise ValueError("Number of news articles must be at least 1")
    if args.num_of_news > 100:
        raise ValueError("Number of news articles cannot exceed 100")

    # --- Portfolio Setup ---
    portfolio = {
        "cash": args.initial_capital,
        "stock": args.initial_position
    }

    # --- Execute Workflow ---
    # Generate run_id here when running directly
    main_run_id = str(uuid.uuid4())
    
    # 如果启用了show_reasoning，自动设置日志级别为INFO以显示推理过程
    if args.show_reasoning:
        set_console_level(logging.DEBUG)
        print("已启用推理显示模式，日志级别已调整为DEBUG")
    
    result = run_hedge_fund(
        run_id=main_run_id,
        ticker=args.ticker,
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d'),
        portfolio=portfolio,
        show_reasoning=args.show_reasoning,
        num_of_news=args.num_of_news,
        show_summary=args.summary,
        tickers=args.tickers  # 传入多个股票代码
    )
    logger.info("\nFinal Result:")
    logger.info(result)

# --- Historical Data Function ---
def get_historical_data(symbol: str) -> pd.DataFrame:
    current_date = datetime.now()
    yesterday = current_date - timedelta(days=1)
    end_date = yesterday
    target_start_date = yesterday - timedelta(days=365)

    print(f"\n正在获取 {symbol} 的历史行情数据...")
    print(f"目标开始日期：{target_start_date.strftime('%Y-%m-%d')}")
    print(f"结束日期：{end_date.strftime('%Y-%m-%d')}")

    try:
        df = ak.stock_zh_a_hist(symbol=symbol,
                                period="daily",
                                start_date=target_start_date.strftime(
                                    "%Y%m%d"),
                                end_date=end_date.strftime("%Y%m%d"),
                                adjust="qfq")

        actual_days = len(df)
        target_days = 365

        if actual_days < target_days:
            print(f"提示：实际获取到的数据天数({actual_days}天)少于目标天数({target_days}天)")
            print(f"将使用可获取到的所有数据进行分析")

        print(f"成功获取历史行情数据，共 {actual_days} 条记录\n")
        return df

    except Exception as e:
        print(f"获取历史数据时发生错误: {str(e)}")
        print("将尝试获取最近可用的数据...")

        try:
            df = ak.stock_zh_a_hist(
                symbol=symbol, period="daily", adjust="qfq")
            print(f"成功获取历史行情数据，共 {len(df)} 条记录\n")
            return df
        except Exception as e:
            print(f"获取历史数据失败: {str(e)}")
            return pd.DataFrame()