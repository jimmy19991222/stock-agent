import json
from typing import Dict, Any, List

from src.utils.logging_config import setup_logger

# 设置日志记录器
logger = setup_logger('structured_terminal')

# 格式化符号
SYMBOLS = {
    "border": "═",
    "header_left": "╔",
    "header_right": "╗",
    "footer_left": "╚",
    "footer_right": "╝",
    "separator": "─",
    "vertical": "║",
    "tree_branch": "├─",
    "tree_last": "└─",
    "section_prefix": "● ",
    "bullet": "• ",
}

# 状态图标
STATUS_ICONS = {
    "bearish": "📉",
    "bullish": "📈",
    "neutral": "◽",
    "hold": "⏸️",
    "buy": "🛒",
    "sell": "💰",
    "completed": "✅",
    "in_progress": "🔄",
    "error": "❌",
    "warning": "⚠️",
}

# Agent图标和名称映射
AGENT_MAP = {
    "market_data_agent": {"icon": "📊", "name": "市场数据分析"},
    "technical_analyst_agent": {"icon": "📈", "name": "技术分析"},
    "fundamentals_agent": {"icon": "📝", "name": "基本面分析"},
    "sentiment_agent": {"icon": "🔍", "name": "情感分析"},
    "valuation_agent": {"icon": "💰", "name": "估值分析"},
    "researcher_bull_agent": {"icon": "🐂", "name": "多方研究"},
    "researcher_bear_agent": {"icon": "🐻", "name": "空方研究"},
    "debate_room_agent": {"icon": "🗣️", "name": "辩论室分析"},
    "risk_management_agent": {"icon": "⚠️", "name": "风险管理"},
    "macro_analyst_agent": {"icon": "🌍", "name": "宏观分析"},
    "portfolio_management_agent": {"icon": "📂", "name": "投资组合管理"}
}

# Agent显示顺序
AGENT_ORDER = [
    "market_data_agent",
    "technical_analyst_agent",
    "fundamentals_agent",
    "sentiment_agent",
    "valuation_agent",
    "researcher_bull_agent",
    "researcher_bear_agent",
    "debate_room_agent",
    "risk_management_agent",
    "macro_analyst_agent",
    "portfolio_management_agent"
]


class StructuredTerminalOutput:
    """结构化终端输出类"""

    def __init__(self):
        """初始化"""
        self.data = {}
        self.metadata = {}

    def set_metadata(self, key: str, value: Any) -> None:
        """设置元数据"""
        self.metadata[key] = value

    def add_agent_data(self, agent_name: str, data: Any) -> None:
        """添加agent数据"""
        self.data[agent_name] = data

    def _format_value(self, value: Any) -> str:
        """格式化单个值"""
        if isinstance(value, bool):
            return "✅" if value else "❌"
        elif isinstance(value, (int, float)):
            # 对大数进行特殊处理
            if value > 1000000:
                if value > 1000000000:  # 十亿以上
                    return f"${value/1000000000:.2f}B"
                else:  # 百万到十亿
                    return f"${value/1000000:.2f}M"
            # 对超过5位小数的数进行格式化
            elif isinstance(value, float) and abs(value) < 0.00001:
                return f"{value:.5f}"
            elif isinstance(value, float):
                return f"{value:.4f}"
            return str(value)
        elif value is None:
            return "N/A"
        else:
            return str(value)

    def _format_dict_as_tree(self, data: Dict[str, Any], indent: int = 0, max_str_len: int = 500) -> List[str]:
        """将字典格式化为树形结构，限制字符串长度"""
        result = []
        items = list(data.items())

        for i, (key, value) in enumerate(items):
            is_last = i == len(items) - 1
            prefix = SYMBOLS["tree_last"] if is_last else SYMBOLS["tree_branch"]
            indent_str = "  " * indent
            
            # 格式化当前值
            formatted_value = self._format_value(value)

            # 特殊处理market_data和一些大型数据结构
            if key in ["market_returns", "stock_returns"] and isinstance(value, str) and len(value) > max_str_len:
                result.append(f"{indent_str}{prefix} {key}: [数据过长，已省略]")
                continue
                    
            # 添加特殊处理大数值的逻辑
            if "price" in key.lower() and isinstance(value, (int, float)) and value > 1000000:
                if value > 1000000000:  # 十亿以上
                    formatted_value = f"${value/1000000000:.2f}B"
                else:  # 百万到十亿
                    formatted_value = f"${value/1000000:.2f}M"
                result.append(f"{indent_str}{prefix} {key}: {formatted_value}")
                continue
                
            # 处理0.0值
            if isinstance(value, (int, float)) and value == 0.0:
                # 检查是否在应该显示0值的场景（如数量、计数等）
                if any(keyword in key.lower() for keyword in ['count', 'quantity', 'number', 'index']):
                    result.append(f"{indent_str}{prefix} {key}: {formatted_value}")
                else:
                    # 如果是在stress_test或其他默认为0的场景，可以选择不显示
                    if "stress_test" not in key.lower() and "potential_loss" not in key.lower():
                        result.append(f"{indent_str}{prefix} {key}: {formatted_value}")
                continue

            if isinstance(value, dict) and value:
                result.append(f"{indent_str}{prefix} {key}:")
                result.extend(self._format_dict_as_tree(value, indent + 1, max_str_len))
            elif isinstance(value, list) and value:
                result.append(f"{indent_str}{prefix} {key}:")
                for j, item in enumerate(value):
                    sub_is_last = j == len(value) - 1
                    sub_prefix = SYMBOLS["tree_last"] if sub_is_last else SYMBOLS["tree_branch"]
                    if isinstance(item, dict):
                        result.append(
                            f"{indent_str}  {sub_prefix} Agent {j+1}:")
                        result.extend(
                            ["  " + line for line in self._format_dict_as_tree(item, indent + 2, max_str_len)])
                    else:
                        # 截断过长的列表项
                        item_str = str(item)
                        if len(item_str) > max_str_len:
                            item_str = item_str[:max_str_len] + "..."
                        result.append(f"{indent_str}  {sub_prefix} {item_str}")
            else:
                # 截断过长的字符串
                if isinstance(formatted_value, str) and len(formatted_value) > max_str_len:
                    formatted_value = formatted_value[:max_str_len] + "..."
                result.append(f"{indent_str}{prefix} {key}: {formatted_value}")

        return result
    
    def _format_market_data_section(self, data: Dict[str, Any]) -> List[str]:
        """格式化市场数据部分为简洁摘要"""
        result = []
        width = 80
        
        # 创建标题
        title = "📊 市场数据摘要"
        result.append(
            f"{SYMBOLS['header_left']}{SYMBOLS['border'] * ((width - len(title) - 2) // 2)} {title} {SYMBOLS['border'] * ((width - len(title) - 2) // 2)}{SYMBOLS['header_right']}")
        
        # 添加主要数据
        if data.get("ticker"):
            result.append(f"{SYMBOLS['vertical']} 股票代码: {data.get('ticker')}")
        
        if data.get("start_date") and data.get("end_date"):
            result.append(f"{SYMBOLS['vertical']} 分析区间: {data.get('start_date')} 至 {data.get('end_date')}")
        
        # 价格摘要
        prices = data.get("prices", [])
        if prices:
            # 计算价格统计数据
            if len(prices) > 0:
                latest_price = prices[-1].get('close', 0)
                avg_price = sum(p.get('close', 0) for p in prices) / len(prices)
                max_price = max(p.get('high', 0) for p in prices)
                min_price = min(p.get('low', 0) for p in prices) if all(p.get('low', 0) > 0 for p in prices) else 0
                
                result.append(f"{SYMBOLS['vertical']} {SYMBOLS['section_prefix']}价格统计:")
                result.append(f"{SYMBOLS['vertical']}   • 最新价格: {latest_price:.2f}")
                result.append(f"{SYMBOLS['vertical']}   • 平均价格: {avg_price:.2f}")
                result.append(f"{SYMBOLS['vertical']}   • 最高价格: {max_price:.2f}")
                result.append(f"{SYMBOLS['vertical']}   • 最低价格: {min_price:.2f}")
        
        # 财务指标摘要
        fin_metrics = data.get("financial_metrics", [{}])[0] if data.get("financial_metrics") else {}
        if fin_metrics:
            result.append(f"{SYMBOLS['vertical']} {SYMBOLS['section_prefix']}主要财务指标:")
            
            # 仅展示关键指标
            key_metrics = {
                "pe_ratio": "市盈率(P/E)",
                "price_to_book": "市净率(P/B)",
                "return_on_equity": "净资产收益率(ROE)",
                "debt_to_equity": "负债/股本比",
                "earnings_growth": "盈利增长率"
            }
            
            for key, label in key_metrics.items():
                if key in fin_metrics:
                    value = fin_metrics[key]
                    result.append(f"{SYMBOLS['vertical']}   • {label}: {value}")
        
        # 添加底部
        result.append(
            f"{SYMBOLS['footer_left']}{SYMBOLS['border'] * (width - 2)}{SYMBOLS['footer_right']}")
        
        return result

    def _format_agent_section(self, agent_name: str, data: Any) -> List[str]:
        """格式化agent部分"""
        result = []

        # 获取agent信息
        agent_info = AGENT_MAP.get(
            agent_name, {"icon": "🔄", "name": agent_name})
        icon = agent_info["icon"]
        display_name = agent_info["name"]

        # 创建标题
        width = 80
        title = f"{icon} {display_name}分析"
        result.append(
            f"{SYMBOLS['header_left']}{SYMBOLS['border'] * ((width - len(title) - 2) // 2)} {title} {SYMBOLS['border'] * ((width - len(title) - 2) // 2)}{SYMBOLS['header_right']}")

        # 添加内容
        if isinstance(data, dict):
            if agent_name == "market_data_agent":
                # 使用简化的市场数据展示
                return self._format_market_data_section(data)
            
            # 特殊处理portfolio_management_agent 和 macro_analyst_agent
            if agent_name == "portfolio_management_agent":
                # 尝试提取action和confidence
                if "action" in data:
                    action = data.get("action", "")
                    action_icon = STATUS_ICONS.get(action.lower(), "")
                    result.append(
                        f"{SYMBOLS['vertical']} 交易行动: {action_icon} {action.upper() if action else ''}")

                if "quantity" in data:
                    quantity = data.get("quantity", 0)
                    result.append(f"{SYMBOLS['vertical']} 交易数量: {quantity}")

                if "confidence" in data:
                    conf = data.get("confidence", 0)
                    if isinstance(conf, (int, float)) and conf <= 1:
                        conf_str = f"{conf*100:.0f}%"
                    else:
                        conf_str = str(conf)
                    result.append(f"{SYMBOLS['vertical']} 决策信心: {conf_str}")

                # 显示各个Agent的信号
                if "agent_signals" in data:
                    result.append(
                        f"{SYMBOLS['vertical']} {SYMBOLS['section_prefix']}各分析师意见:")

                    for signal_info in data["agent_signals"]:
                        agent = signal_info.get("agent", "")
                        signal = signal_info.get("signal", "")
                        conf = signal_info.get("confidence", 1.0)

                        # 跳过空信号
                        if not agent or not signal:
                            continue

                        # 获取信号图标
                        signal_icon = STATUS_ICONS.get(signal.lower(), "")

                        # 格式化置信度
                        if isinstance(conf, (int, float)) and conf <= 1:
                            conf_str = f"{conf*100:.0f}%"
                        else:
                            conf_str = str(conf)

                        result.append(
                            f"{SYMBOLS['vertical']}   • {agent}: {signal_icon} {signal} (置信度: {conf_str})")

                # 决策理由
                if "reasoning" in data:
                    reasoning = data["reasoning"]
                    result.append(
                        f"{SYMBOLS['vertical']} {SYMBOLS['section_prefix']}决策理由:")
                    if isinstance(reasoning, str):
                        # 将长文本拆分为多行，每行不超过width-4个字符
                        for i in range(0, len(reasoning), width-4):
                            line = reasoning[i:i+width-4]
                            result.append(f"{SYMBOLS['vertical']}   {line}")
            elif agent_name == "macro_analyst_agent":
                # 处理宏观分析
                if isinstance(data, dict):
                    # 提取关键信息
                    macro_env = data.get("macro_environment", "")
                    impact = data.get("impact_on_stock", "")
                    key_factors = data.get("key_factors", [])
                    
                    # 添加高亮的宏观环境和影响
                    env_icon = "📈" if macro_env == "positive" else "📉" if macro_env == "negative" else "◽"
                    impact_icon = "📈" if impact == "positive" else "📉" if impact == "negative" else "◽"
                    
                    result.append(f"{SYMBOLS['vertical']} 宏观环境: {env_icon} {macro_env}")
                    result.append(f"{SYMBOLS['vertical']} 对股票影响: {impact_icon} {impact}")
                    
                    # 添加关键因素列表
                    if key_factors:
                        result.append(f"{SYMBOLS['vertical']} {SYMBOLS['section_prefix']}关键因素:")
                        for i, factor in enumerate(key_factors[:5]):  # 最多显示5个因素
                            result.append(f"{SYMBOLS['vertical']}   • {factor}")
                    
                    # 添加简化的理由
                    reasoning = data.get("reasoning", "")
                    if reasoning:
                        # 截取前100个字符作为摘要
                        reasoning_summary = reasoning[:100] + "..." if len(reasoning) > 100 else reasoning
                        result.append(f"{SYMBOLS['vertical']} {SYMBOLS['section_prefix']}分析摘要:")
                        result.append(f"{SYMBOLS['vertical']}   {reasoning_summary}")
            else:
                # 标准处理其他agent
                # 提取信号和置信度（如果有）
                if "signal" in data:
                    signal = data.get("signal", "")
                    # 确保signal是字符串类型
                    if isinstance(signal, (int, float)):
                        # 将数值信号转换为字符串
                        if signal > 0.2:
                            signal = "bullish"
                        elif signal < -0.2:
                            signal = "bearish"
                        else:
                            signal = "neutral"
                    signal_str = str(signal)
                    signal_icon = STATUS_ICONS.get(signal_str.lower(), "")
                    result.append(
                        f"{SYMBOLS['vertical']} 信号: {signal_icon} {signal_str}")

                if "confidence" in data:
                    conf = data.get("confidence", "")
                    if isinstance(conf, (int, float)) and conf <= 1:
                        conf_str = f"{conf*100:.0f}%"
                    else:
                        conf_str = str(conf)
                    result.append(f"{SYMBOLS['vertical']} 置信度: {conf_str}")

            # 添加其他数据
            tree_lines = self._format_dict_as_tree(data)
            for line in tree_lines:
                result.append(f"{SYMBOLS['vertical']} {line}")
        elif isinstance(data, list):
            for i, item in enumerate(data):
                prefix = SYMBOLS["tree_last"] if i == len(
                    data) - 1 else SYMBOLS["tree_branch"]
                result.append(f"{SYMBOLS['vertical']} {prefix} {item}")
        else:
            result.append(f"{SYMBOLS['vertical']} {data}")

        # 添加底部
        result.append(
            f"{SYMBOLS['footer_left']}{SYMBOLS['border'] * (width - 2)}{SYMBOLS['footer_right']}")

        return result

    def generate_output(self) -> str:
        """生成格式化输出"""
        width = 80
        result = []

        # 添加标题
        ticker = self.metadata.get("ticker", "未知")
        title = f"股票代码 {ticker} 投资分析报告"
        result.append(SYMBOLS["border"] * width)
        result.append(f"{title:^{width}}")
        result.append(SYMBOLS["border"] * width)

        # 添加日期范围（如果有）
        if "start_date" in self.metadata and "end_date" in self.metadata:
            date_range = f"分析区间: {self.metadata['start_date']} 至 {self.metadata['end_date']}"
            result.append(f"{date_range:^{width}}")
            result.append("")

        # 按顺序添加每个agent的输出
        for agent_name in AGENT_ORDER:
            if agent_name in self.data:
                result.extend(self._format_agent_section(
                    agent_name, self.data[agent_name]))
                result.append("")  # 添加空行

        # 添加结束分隔线
        result.append(SYMBOLS["border"] * width)

        return "\n".join(result)

    def print_output(self) -> None:
        """打印格式化输出"""
        output = self.generate_output()
        
        # 添加ANSI颜色代码
        colored_output = output
        colored_output = colored_output.replace("bullish", "\033[32mbullish\033[0m")  # 绿色
        colored_output = colored_output.replace("bearish", "\033[31mbearish\033[0m")  # 红色
        colored_output = colored_output.replace("neutral", "\033[33mneutral\033[0m")  # 黄色
        colored_output = colored_output.replace("positive", "\033[32mpositive\033[0m")  # 绿色
        colored_output = colored_output.replace("negative", "\033[31mnegative\033[0m")  # 红色
        colored_output = colored_output.replace("BUY", "\033[32mBUY\033[0m")  # 绿色
        colored_output = colored_output.replace("SELL", "\033[31mSELL\033[0m")  # 红色
        colored_output = colored_output.replace("HOLD", "\033[33mHOLD\033[0m")  # 黄色
        
        # 直接使用print输出，不受日志级别限制
        print("\n" + colored_output)
        logger.info("\n" + colored_output)


# 创建全局实例
terminal = StructuredTerminalOutput()


def extract_agent_data(state: Dict[str, Any], agent_name: str) -> Any:
    """
    从状态中提取指定agent的数据

    Args:
        state: 工作流状态
        agent_name: agent名称

    Returns:
        提取的agent数据
    """
    # 特殊处理portfolio_management_agent
    if agent_name == "portfolio_management_agent":
        # 尝试从最后一条消息中获取数据
        messages = state.get("messages", [])
        if messages and hasattr(messages[-1], "content"):
            content = messages[-1].content
            # 尝试解析JSON
            if isinstance(content, str):
                try:
                    # 如果是JSON字符串，尝试解析
                    if content.strip().startswith('{') and content.strip().endswith('}'):
                        return json.loads(content)
                    # 如果是JSON字符串包含在其他文本中，尝试提取并解析
                    json_start = content.find('{')
                    json_end = content.rfind('}')
                    if json_start >= 0 and json_end > json_start:
                        json_str = content[json_start:json_end+1]
                        return json.loads(json_str)
                except json.JSONDecodeError:
                    # 如果解析失败，返回原始内容
                    return {"message": content}
            return {"message": content}

    # 首先尝试从metadata中的all_agent_reasoning获取
    metadata = state.get("metadata", {})
    all_reasoning = metadata.get("all_agent_reasoning", {})

    # 查找匹配的agent数据
    for name, data in all_reasoning.items():
        if agent_name in name:
            return data

    # 如果在all_agent_reasoning中找不到，尝试从agent_reasoning获取
    if agent_name == metadata.get("current_agent_name") and "agent_reasoning" in metadata:
        return metadata["agent_reasoning"]

    # 尝试从messages中获取
    messages = state.get("messages", [])
    for message in messages:
        if hasattr(message, "name") and message.name and agent_name in message.name:
            # 尝试解析消息内容
            try:
                if hasattr(message, "content"):
                    content = message.content
                    # 尝试解析JSON
                    if isinstance(content, str) and (content.startswith('{') or content.startswith('[')):
                        try:
                            return json.loads(content)
                        except json.JSONDecodeError:
                            pass
                    return content
            except Exception:
                pass

    # 如果都找不到，返回None
    return None


def process_final_state(state: Dict[str, Any]) -> None:
    """
    处理最终状态，提取所有agent的数据

    Args:
        state: 工作流的最终状态
    """
    # 提取元数据
    data = state.get("data", {})

    # 设置元数据
    terminal.set_metadata("ticker", data.get("ticker", "未知"))
    if "start_date" in data and "end_date" in data:
        terminal.set_metadata("start_date", data["start_date"])
        terminal.set_metadata("end_date", data["end_date"])

    # 提取每个agent的数据
    for agent_name in AGENT_ORDER:
        agent_data = extract_agent_data(state, agent_name)
        if agent_data:
            terminal.add_agent_data(agent_name, agent_data)


def print_structured_output(state: Dict[str, Any]) -> None:
    """
    处理最终状态并打印结构化输出

    Args:
        state: 工作流的最终状态
    """
    try:
        # 处理最终状态
        process_final_state(state)

        # 打印输出
        terminal.print_output()
    except Exception as e:
        logger.error(f"生成结构化输出时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
