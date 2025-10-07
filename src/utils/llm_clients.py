import os
import time
import backoff
from abc import ABC, abstractmethod
from openai import OpenAI
from src.utils.logging_config import setup_logger, SUCCESS_ICON, ERROR_ICON, WAIT_ICON
from dotenv import load_dotenv

load_dotenv()
# 设置日志记录
logger = setup_logger('llm_clients')


class LLMClient(ABC):
    """LLM 客户端抽象基类"""

    @abstractmethod
    def get_completion(self, messages, **kwargs):
        """获取模型回答"""
        pass


class OpenAIClient(LLMClient):
    """OpenAI Compatible API 客户端"""

    def __init__(self, api_key=None, base_url=None, model=None):
        self.api_key = api_key or os.getenv("OPENAI_COMPATIBLE_API_KEY")
        self.base_url = base_url or os.getenv("OPENAI_COMPATIBLE_BASE_URL")
        self.model = model or os.getenv("OPENAI_COMPATIBLE_MODEL")

        # self.api_key = '15O3IPILSH'
        # self.base_url = 'https://whale-wave.alibaba-inc.com/api/v2/services/aigc/text-generation'
        # self.model ='DeepSeek-V3.2-Exp'

        # self.api_key = 'sk-93bf8a433943448bad6611ca5532a113'
        # self.base_url = 'https://dashscope.aliyuncs.com/compatible-mode/v1/'
        # self.model ='qwen3-max'

        if not self.api_key:
            logger.error(f"{ERROR_ICON} 未找到 OPENAI_COMPATIBLE_API_KEY 环境变量")
            raise ValueError(
                "OPENAI_COMPATIBLE_API_KEY not found in environment variables")

        if not self.base_url:
            logger.error(f"{ERROR_ICON} 未找到 OPENAI_COMPATIBLE_BASE_URL 环境变量")
            raise ValueError(
                "OPENAI_COMPATIBLE_BASE_URL not found in environment variables")

        if not self.model:
            logger.error(f"{ERROR_ICON} 未找到 OPENAI_COMPATIBLE_MODEL 环境变量")
            raise ValueError(
                "OPENAI_COMPATIBLE_MODEL not found in environment variables")

        # 初始化 OpenAI 客户端
        self.client = OpenAI(
            base_url=self.base_url,
            api_key=self.api_key
        )
        logger.info(f"{SUCCESS_ICON} OpenAI Compatible 客户端初始化成功")

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=5,
        max_time=300
    )
    def call_api_with_retry(self, messages, stream=False):
        """带重试机制的 API 调用函数"""
        try:
            logger.info(f"{WAIT_ICON} 正在调用 OpenAI API...")
            logger.debug(f"请求内容: {messages}")
            logger.debug(f"模型: {self.model}, 流式: {stream}")

            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                stream=stream
            )

            logger.info(f"{SUCCESS_ICON} API 调用成功")
            return response
        except Exception as e:
            error_msg = str(e)
            logger.error(f"{ERROR_ICON} API 调用失败: {error_msg}")
            raise e

    def get_completion(self, messages, max_retries=3, initial_retry_delay=1, **kwargs):
        """获取聊天完成结果，包含重试逻辑"""
        try:
            logger.info(f"{WAIT_ICON} 使用 OpenAI 模型: {self.model}")
            logger.debug(f"消息内容: {messages}")

            for attempt in range(max_retries):
                try:
                    # 调用 API
                    response = self.call_api_with_retry(messages)

                    if response is None:
                        logger.warning(
                            f"{ERROR_ICON} 尝试 {attempt + 1}/{max_retries}: API 返回空值")
                        if attempt < max_retries - 1:
                            retry_delay = initial_retry_delay * (2 ** attempt)
                            logger.info(
                                f"{WAIT_ICON} 等待 {retry_delay} 秒后重试...")
                            time.sleep(retry_delay)
                            continue
                        return None

                    # 打印调试信息
                    content = response.choices[0].message.content
                    logger.debug(f"API 原始响应: {content[:500]}...")
                    logger.info(f"{SUCCESS_ICON} 成功获取 OpenAI 响应")

                    # 直接返回文本内容
                    return content

                except Exception as e:
                    logger.error(
                        f"{ERROR_ICON} 尝试 {attempt + 1}/{max_retries} 失败: {str(e)}")
                    if attempt < max_retries - 1:
                        retry_delay = initial_retry_delay * (2 ** attempt)
                        logger.info(f"{WAIT_ICON} 等待 {retry_delay} 秒后重试...")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"{ERROR_ICON} 最终错误: {str(e)}")
                        return None

        except Exception as e:
            logger.error(f"{ERROR_ICON} get_completion 发生错误: {str(e)}")
            return None


class LLMClientFactory:
    """LLM 客户端工厂类"""

    @staticmethod
    def create_client(**kwargs):
        """
        创建 OpenAI Compatible 客户端

        Args:
            **kwargs: 客户端的配置参数，包括 api_key、base_url 和 model

        Returns:
            LLMClient: 实例化的 OpenAI Compatible 客户端
        """
        return OpenAIClient(
            api_key=kwargs.get("api_key"),
            base_url=kwargs.get("base_url"),
            model=kwargs.get("model")
        )