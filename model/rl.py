import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.distributions import Categorical
import gym
from gym import spaces
import os
from src.utils.logging_config import setup_logger
from typing import Dict, Tuple, Any
import random

# 设置日志
logger = setup_logger('reinforcement_learning')

# 设置随机种子以确保结果可重现
torch.manual_seed(42)
np.random.seed(42)
random.seed(42)

# 确保CUDA可用时使用GPU，否则使用CPU
DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
logger.info(f"使用设备: {DEVICE}")

# 只有在CUDA可用时才设置CUDA种子
if torch.cuda.is_available():
    torch.cuda.manual_seed(42)
    torch.cuda.manual_seed_all(42)
    logger.info(f"CUDA设备名称: {torch.cuda.get_device_name(0)}")
    logger.info(f"CUDA设备数量: {torch.cuda.device_count()}")
else:
    logger.info("CUDA不可用，使用CPU进行计算")


class StockTradingEnv(gym.Env):
    """股票交易环境，实现OpenAI Gym接口"""
    
    def __init__(self, df: pd.DataFrame, initial_balance=100000, 
                transaction_fee_percent=0.001, tech_indicators=None,
                reward_scaling=1.0, max_steps=252, window_size=20):
        """
        初始化交易环境
        
        Args:
            df: 价格数据DataFrame
            initial_balance: 初始资金
            transaction_fee_percent: 交易手续费比例
            tech_indicators: 技术指标列表
            reward_scaling: 奖励缩放因子
            max_steps: 每个episode的最大步数
            window_size: 观察窗口大小
        """
        super(StockTradingEnv, self).__init__()
        
        self.df = df.copy()
        self.initial_balance = initial_balance
        self.balance = initial_balance
        self.transaction_fee_percent = transaction_fee_percent
        self.reward_scaling = reward_scaling
        self.max_steps = max_steps
        self.window_size = window_size
        
        # 技术指标列表，如果为None使用默认指标
        if tech_indicators is None:
            self.tech_indicator_columns = [
                'ma5', 'ma10', 'ma20', 'rsi', 'macd', 'macd_signal', 'macd_hist',
                'volatility_5d', 'volatility_10d', 'volatility_20d'
            ]
        else:
            self.tech_indicator_columns = tech_indicators
        
        # 确保所有技术指标都在DataFrame中
        missing_columns = [col for col in self.tech_indicator_columns if col not in self.df.columns]
        if missing_columns:
            logger.warning(f"以下技术指标在DataFrame中不存在: {missing_columns}")
            # 从列表中移除不存在的指标
            self.tech_indicator_columns = [col for col in self.tech_indicator_columns if col in self.df.columns]
        
        # 状态空间：价格、技术指标、持仓、余额等
        # 特征数量 = 窗口大小 * (价格特征 + 技术指标) + 持仓和余额信息
        num_price_features = 5  # OHLCV: 开盘、最高、最低、收盘、成交量
        num_tech_indicators = len(self.tech_indicator_columns)
        observation_shape = (self.window_size * (num_price_features + num_tech_indicators) + 2,)
        
        self.observation_space = spaces.Box(
            low=-np.inf,
            high=np.inf,
            shape=observation_shape,
            dtype=np.float32
        )
        
        # 动作空间：买入、卖出、持有 (0: 持有, 1: 买入, 2: 卖出)
        self.action_space = spaces.Discrete(3)
        
        # 当前步骤和位置
        self.current_step = None
        self.current_position = 0
        self.total_reward = 0
        self.history = []
        
        # 初始化环境
        self.reset()
    
    def _next_observation(self) -> np.ndarray:
        """
        获取当前窗口的状态特征
        
        Returns:
            状态特征数组
        """
        # 确保在数据范围内
        end_idx = min(self.current_step + 1, len(self.df))
        start_idx = max(0, end_idx - self.window_size)
        
        # 不足窗口大小的部分用第一个数据填充
        padding_size = self.window_size - (end_idx - start_idx)
        
        # 提取价格数据
        price_features = ['open', 'high', 'low', 'close', 'volume']
        prices = self.df[price_features].iloc[start_idx:end_idx].values
        
        # 提取技术指标
        tech_indicators = self.df[self.tech_indicator_columns].iloc[start_idx:end_idx].values
        
        # 填充不足部分
        if padding_size > 0:
            price_padding = np.repeat(prices[:1], padding_size, axis=0)
            tech_padding = np.repeat(tech_indicators[:1], padding_size, axis=0)
            prices = np.vstack([price_padding, prices])
            tech_indicators = np.vstack([tech_padding, tech_indicators])
        
        # 归一化处理数据
        prices_mean = np.mean(prices, axis=0)
        prices_std = np.std(prices, axis=0)
        tech_mean = np.mean(tech_indicators, axis=0)
        tech_std = np.std(tech_indicators, axis=0)
        
        # 避免除以0
        prices_std = np.where(prices_std == 0, 1, prices_std)
        tech_std = np.where(tech_std == 0, 1, tech_std)
        
        normalized_prices = (prices - prices_mean) / prices_std
        normalized_tech = (tech_indicators - tech_mean) / tech_std
        
        # 拼接价格和技术指标
        features = np.column_stack([normalized_prices, normalized_tech])
        
        # 展平为一维数组
        flattened_features = features.flatten()
        
        # 添加持仓和余额信息
        current_price = self.df['close'].iloc[self.current_step]
        position_value = self.current_position * current_price / self.initial_balance
        cash_ratio = self.balance / self.initial_balance
        
        # 构建完整的状态特征
        observation = np.append(flattened_features, [position_value, cash_ratio])
        
        return observation.astype(np.float32)
    
    def _take_action(self, action: int) -> float:
        """
        执行交易操作并计算奖励
        
        Args:
            action: 交易动作 (0: 持有, 1: 买入, 2: 卖出)
            
        Returns:
            奖励值
        """
        current_price = self.df['close'].iloc[self.current_step]
        prev_portfolio_value = self.balance + self.current_position * current_price
        
        # 执行交易
        if action == 1:  # 买入
            # 计算可买入的最大数量
            max_possible_shares = self.balance // (current_price * (1 + self.transaction_fee_percent))
            # 每次使用25%的资金买入
            shares_to_buy = max(max_possible_shares // 4, 1)
            
            if shares_to_buy > 0 and self.balance >= shares_to_buy * current_price * (1 + self.transaction_fee_percent):
                cost = shares_to_buy * current_price * (1 + self.transaction_fee_percent)
                self.balance -= cost
                self.current_position += shares_to_buy
                trade_info = {
                    'step': self.current_step,
                    'action': 'buy',
                    'shares': shares_to_buy,
                    'price': current_price,
                    'cost': cost
                }
                self.history.append(trade_info)
        
        elif action == 2:  # 卖出
            if self.current_position > 0:
                # 每次卖出当前持仓的25%
                shares_to_sell = max(self.current_position // 4, 1)
                
                sell_amount = shares_to_sell * current_price * (1 - self.transaction_fee_percent)
                self.balance += sell_amount
                self.current_position -= shares_to_sell
                trade_info = {
                    'step': self.current_step,
                    'action': 'sell',
                    'shares': shares_to_sell,
                    'price': current_price,
                    'amount': sell_amount
                }
                self.history.append(trade_info)
        
        # 计算新的组合价值
        new_portfolio_value = self.balance + self.current_position * current_price
        
        # 计算收益率
        portfolio_return = (new_portfolio_value / prev_portfolio_value) - 1
        
        # 应用奖励缩放
        reward = portfolio_return * self.reward_scaling
        
        # 鼓励交易，减少持有惩罚
        if action == 0:  # 持有
            # 如果持仓比例低，给予小惩罚
            position_ratio = self.current_position * current_price / new_portfolio_value
            if position_ratio < 0.1:  # 持仓少于10%
                reward -= 0.001  # 小惩罚，鼓励建仓
        else:  # 交易(买入或卖出)
            # 轻微的交易奖励，鼓励探索
            reward += 0.0005
        
        self.total_reward += reward
        
        return reward
    
    def step(self, action: int) -> Tuple[np.ndarray, float, bool, Dict]:
        """
        执行一步环境交互
        
        Args:
            action: 交易动作
            
        Returns:
            observation: 新的状态
            reward: 奖励值
            done: 是否结束
            info: 附加信息
        """
        # 执行动作并获取奖励
        reward = self._take_action(action)
        
        # 前进一步
        self.current_step += 1
        
        # 检查是否结束
        done = (self.current_step >= len(self.df) - 1) or (self.current_step >= self.max_steps)
        
        # 获取新的状态
        observation = self._next_observation()
        
        # 准备附加信息
        current_price = self.df['close'].iloc[self.current_step] if self.current_step < len(self.df) else self.df['close'].iloc[-1]
        info = {
            'current_step': self.current_step,
            'current_price': current_price,
            'current_position': self.current_position,
            'balance': self.balance,
            'portfolio_value': self.balance + self.current_position * current_price,
            'total_reward': self.total_reward
        }
        
        return observation, reward, done, info
    
    def reset(self):
        """
        重置环境
        
        Returns:
            初始状态
        """
        max_start_idx = len(self.df) - self.max_steps
        min_start_idx = self.window_size
        
        if min_start_idx >= max_start_idx:
            logger.info(f"警告: 数据量不足。window_size={self.window_size}, max_steps={self.max_steps}, 数据长度={len(self.df)}")
            # 调整参数，确保可以选择起始点
            adjusted_max_steps = len(self.df) - min_start_idx - 5  # 添加一些缓冲
            self.max_steps = max(10, adjusted_max_steps)  # 至少10步
            max_start_idx = len(self.df) - self.max_steps
            logger.info(f"已调整 max_steps 为 {self.max_steps}")
        
        # 随机选择起始点
        self.current_step = np.random.randint(min_start_idx, max_start_idx)
        
        # 重置资金和持仓
        self.balance = self.initial_balance
        self.current_position = 0
        self.total_reward = 0
        self.history = []
        
        return self._next_observation()
    
    def render(self, mode='human'):
        """
        渲染环境
        
        Args:
            mode: 渲染模式
        """
        if mode == 'human':
            current_price = self.df['close'].iloc[self.current_step]
            portfolio_value = self.balance + self.current_position * current_price
            logger.info(f"Step: {self.current_step}")
            logger.info(f"Price: {current_price:.2f}")
            logger.info(f"Balance: {self.balance:.2f}")
            logger.info(f"Shares: {self.current_position}")
            logger.info(f"Portfolio Value: {portfolio_value:.2f}")
            logger.info(f"Total Reward: {self.total_reward:.4f}")
            logger.info("-------------------")


class ActorCritic(nn.Module):
    """PPO的Actor-Critic网络"""
    
    def __init__(self, state_dim: int, action_dim: int, hidden_dim: int = 128):
        """
        初始化Actor-Critic网络
        
        Args:
            state_dim: 状态空间维度
            action_dim: 动作空间维度
            hidden_dim: 隐藏层维度
        """
        super(ActorCritic, self).__init__()
        
        # 共享特征提取层
        self.feature_extractor = nn.Sequential(
            nn.Linear(state_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU()
        )
        
        # Actor网络 (策略网络)
        self.actor = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, action_dim),
            nn.Softmax(dim=-1)
        )
        
        # Critic网络 (价值网络)
        self.critic = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, 1)
        )
    
    def forward(self, state):
        """
        前向传播
        
        Args:
            state: 状态
            
        Returns:
            action_probs: 动作概率
            state_value: 状态价值
        """
        features = self.feature_extractor(state)
        action_probs = self.actor(features)
        state_value = self.critic(features)
        return action_probs, state_value
    
    def act(self, state):
        """
        根据策略选择动作
        
        Args:
            state: 状态
            
        Returns:
            action: 选择的动作
            action_prob: 选择动作的概率
            state_value: 状态价值
        """
        state = torch.FloatTensor(state).unsqueeze(0).to(DEVICE)  # 使用全局DEVICE
        action_probs, state_value = self.forward(state)
        action_dist = Categorical(action_probs)
        action = action_dist.sample()
        action_prob = action_probs[0, action.item()]
        return action.item(), action_prob.item(), state_value.item()


class PPOAgent:
    """PPO智能交易代理"""
    
    def __init__(self, state_dim: int, action_dim: int, hidden_dim: int = 128,
                lr: float = 0.0003, gamma: float = 0.99, eps_clip: float = 0.2,
                k_epochs: int = 4, device: str = None):
        """
        初始化PPO代理
        
        Args:
            state_dim: 状态空间维度
            action_dim: 动作空间维度
            hidden_dim: 隐藏层维度
            lr: 学习率
            gamma: 折扣因子
            eps_clip: PPO裁剪参数
            k_epochs: 策略更新轮数
            device: 运行设备 ('cuda' 或 'cpu')
        """
        self.gamma = gamma
        self.eps_clip = eps_clip
        self.k_epochs = k_epochs
        
        # 明确使用CUDA设备
        if device is None:
            self.device = DEVICE  # 使用全局设备配置
        else:
            self.device = torch.device(device)
            
        logger.info(f"PPO代理初始化完成，使用设备: {self.device}")
        
        # 策略网络
        self.policy = ActorCritic(state_dim, action_dim, hidden_dim).to(self.device)
        self.optimizer = torch.optim.Adam(self.policy.parameters(), lr=lr)
        
        # 旧策略
        self.old_policy = ActorCritic(state_dim, action_dim, hidden_dim).to(self.device)
        self.old_policy.load_state_dict(self.policy.state_dict())
        
        # 损失函数
        self.MseLoss = nn.MSELoss()
        
        # 经验缓冲区
        self.buffer = []
    
    def select_action(self, state):
        """
        根据当前策略选择动作
        
        Args:
            state: 状态
            
        Returns:
            action: 选择的动作
            action_prob: 动作概率
            state_value: 状态价值
        """
        with torch.no_grad():
            return self.old_policy.act(state)
    
    def update_old_policy(self):
        """更新旧策略"""
        self.old_policy.load_state_dict(self.policy.state_dict())
    
    def store_transition(self, transition):
        """
        存储状态转移
        
        Args:
            transition: (state, action, action_prob, reward, next_state, done)
        """
        self.buffer.append(transition)
    
    def update(self):
        """
        更新策略
        
        Returns:
            actor_loss: Actor网络损失
            critic_loss: Critic网络损失
        """
        # 从缓冲区提取数据
        old_states = torch.FloatTensor(np.array([t[0] for t in self.buffer])).to(self.device)
        old_actions = torch.LongTensor(np.array([t[1] for t in self.buffer])).to(self.device)
        old_action_probs = torch.FloatTensor(np.array([t[2] for t in self.buffer])).to(self.device)
        rewards = [t[3] for t in self.buffer]
        next_states = [t[4] for t in self.buffer]
        dones = [t[5] for t in self.buffer]
        
        # 计算累积回报
        returns = []
        discounted_reward = 0
        for reward, next_state, done in zip(reversed(rewards), reversed(next_states), reversed(dones)):
            if done:
                discounted_reward = 0
            discounted_reward = reward + (self.gamma * discounted_reward)
            returns.insert(0, discounted_reward)
        
        # 归一化回报
        returns = torch.FloatTensor(returns).to(self.device)
        returns = (returns - returns.mean()) / (returns.std() + 1e-5)
        
        # 多次优化
        actor_loss_epoch = 0
        critic_loss_epoch = 0
        
        for _ in range(self.k_epochs):
            # 获取当前策略的动作概率和状态价值
            action_probs, state_values = self.policy(old_states)
            state_values = state_values.squeeze()
            
            # 计算新旧策略的概率比
            dist = Categorical(action_probs)
            new_action_probs = dist.log_prob(old_actions).exp()
            
            # 计算比率
            ratios = new_action_probs / old_action_probs
            
            # 计算优势
            advantages = returns - state_values.detach()
            
            # PPO损失
            surr1 = ratios * advantages
            surr2 = torch.clamp(ratios, 1-self.eps_clip, 1+self.eps_clip) * advantages
            actor_loss = -torch.min(surr1, surr2).mean()
            
            # 价值损失
            critic_loss = self.MseLoss(state_values, returns)
            
            # 总损失
            loss = actor_loss + 0.5 * critic_loss
            
            # 优化
            self.optimizer.zero_grad()
            loss.backward()
            self.optimizer.step()
            
            actor_loss_epoch += actor_loss.item()
            critic_loss_epoch += critic_loss.item()
        
        # 清空缓冲区
        self.buffer = []
        
        return actor_loss_epoch / self.k_epochs, critic_loss_epoch / self.k_epochs
    
    def save_model(self, path):
        """
        保存模型
        
        Args:
            path: 保存路径
        """
        torch.save(self.policy.state_dict(), path)
    
    def load_model(self, path):
        """
        加载模型
        
        Args:
            path: 模型路径
        """
        state_dict = torch.load(path, map_location=self.device, weights_only=True)
        self.policy.load_state_dict(state_dict)
        self.old_policy.load_state_dict(self.policy.state_dict())


class RLTrader:
    """强化学习交易系统，集成PPO代理和交易环境"""
    
    def __init__(self, model_dir: str = 'models', hidden_dim: int = 128, device: str = None):
        """
        初始化RL交易系统
        
        Args:
            model_dir: 模型保存目录
            hidden_dim: 网络隐藏层维度
            device: 运行设备
        """
        self.model_dir = model_dir
        os.makedirs(model_dir, exist_ok=True)
        
        self.hidden_dim = hidden_dim
        # 明确使用CUDA设备
        if device is None:
            self.device = DEVICE  # 使用全局设备配置
        else:
            self.device = device
        
        self.env = None
        self.agent = None
        self.trained = False
        
        self.logger = setup_logger('rl_trader')
        self.logger.info(f"RLTrader初始化完成，使用设备: {self.device}")
    
    def train(self, df, initial_balance=100000, transaction_fee_percent=0.001,
          n_episodes=1000, batch_size=64, reward_scaling=100.0, max_steps=252,
          window_size=20):
        """
        训练强化学习交易模型
        
        Args:
            df: 价格数据DataFrame
            initial_balance: 初始资金
            transaction_fee_percent: 交易手续费比例
            n_episodes: 训练轮数
            batch_size: 批量大小
            reward_scaling: 奖励缩放因子
            max_steps: 每个episode的最大步数
        """
        # 创建交易环境
        self.env = StockTradingEnv(
            df=df,
            initial_balance=initial_balance,
            transaction_fee_percent=transaction_fee_percent,
            reward_scaling=reward_scaling,
            max_steps=max_steps,
            window_size=window_size,
        )
        
        # 创建PPO代理
        state_dim = self.env.observation_space.shape[0]
        action_dim = self.env.action_space.n
        
        self.agent = PPOAgent(
            state_dim=state_dim,
            action_dim=action_dim,
            hidden_dim=self.hidden_dim,
            device=self.device  # 明确传递设备参数
        )
        
        # 训练记录
        episode_rewards = []
        actor_losses = []
        critic_losses = []
        portfolio_values = []
        
        self.logger.info(f"开始训练，共{n_episodes}轮...")
        best_reward = -float('inf')
        
        for episode in range(n_episodes):
            state = self.env.reset()
            done = False
            episode_reward = 0
            
            while not done:
                # 选择动作
                action, action_prob, _ = self.agent.select_action(state)
                
                # 执行动作
                next_state, reward, done, info = self.env.step(action)
                
                # 存储转移
                self.agent.store_transition((state, action, action_prob, reward, next_state, done))
                
                # 如果缓冲区满，更新策略
                if len(self.agent.buffer) >= batch_size:
                    actor_loss, critic_loss = self.agent.update()
                    self.agent.update_old_policy()
                    actor_losses.append(actor_loss)
                    critic_losses.append(critic_loss)
                
                state = next_state
                episode_reward += reward
            
            # 记录结果
            episode_rewards.append(episode_reward)
            portfolio_values.append(info['portfolio_value'])
            
            # 打印训练进度
            if (episode + 1) % 10 == 0:
                avg_reward = np.mean(episode_rewards[-10:])
                avg_value = np.mean(portfolio_values[-10:])
                self.logger.info(f"Episode {episode+1}/{n_episodes}, Avg Reward: {avg_reward:.4f}, Avg Portfolio: {avg_value:.2f}")
            
            # 保存最佳模型
            if episode_reward > best_reward:
                best_reward = episode_reward
                self.save_model("best_model")
        
        # 最终保存模型
        self.save_model("final_model")
        self.trained = True
        
        # 返回训练记录
        training_history = {
            'episode_rewards': episode_rewards,
            'actor_losses': actor_losses,
            'critic_losses': critic_losses,
            'portfolio_values': portfolio_values
        }
        
        return training_history
    
    def test(self, df: pd.DataFrame, initial_balance: float = 100000,
            transaction_fee_percent: float = 0.001, model_name: str = "best_model"):
        """
        测试强化学习交易模型
        
        Args:
            df: 价格数据DataFrame
            initial_balance: 初始资金
            transaction_fee_percent: 交易手续费比例
            model_name: 模型名称
            
        Returns:
            测试结果字典
        """
        # 创建测试环境
        test_env = StockTradingEnv(
            df=df,
            initial_balance=initial_balance,
            transaction_fee_percent=transaction_fee_percent,
            reward_scaling=1.0,
            max_steps=len(df)
        )
        
        # 确保代理已加载
        if self.agent is None:
            state_dim = test_env.observation_space.shape[0]
            action_dim = test_env.action_space.n
            
            self.agent = PPOAgent(
                state_dim=state_dim,
                action_dim=action_dim,
                hidden_dim=self.hidden_dim,
                device=self.device  # 明确传递设备参数
            )
            self.load_model(model_name)
        
        # 测试
        state = test_env.reset()
        done = False
        total_reward = 0
        
        # 记录测试过程
        states = []
        actions = []
        rewards = []
        portfolio_values = []
        balances = []
        positions = []
        prices = []
        
        while not done:
            # 选择动作
            action, _, _ = self.agent.select_action(state)
            
            # 执行动作
            next_state, reward, done, info = test_env.step(action)
            
            # 记录
            states.append(state)
            actions.append(action)
            rewards.append(reward)
            portfolio_values.append(info['portfolio_value'])
            balances.append(info['balance'])
            positions.append(info['current_position'])
            prices.append(info['current_price'])
            
            state = next_state
            total_reward += reward
        
        # 计算性能指标
        returns = np.array(portfolio_values) / initial_balance - 1
        daily_returns = np.diff(portfolio_values) / portfolio_values[:-1]
        
        # 计算夏普比率
        avg_daily_return = np.mean(daily_returns)
        std_daily_return = np.std(daily_returns)
        sharpe_ratio = (avg_daily_return / std_daily_return) * np.sqrt(252) if std_daily_return > 0 else 0
        
        # 最大回撤
        peak = np.maximum.accumulate(portfolio_values)
        drawdown = (peak - portfolio_values) / peak
        max_drawdown = np.max(drawdown)
        
        # 总收益率
        total_return = portfolio_values[-1] / initial_balance - 1
        
        # 测试结果
        test_results = {
            'total_reward': total_reward,
            'total_return': total_return,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'portfolio_values': portfolio_values,
            'actions': actions,
            'positions': positions,
            'balances': balances,
            'prices': prices,
            'trades': test_env.history
        }
        
        self.logger.info(f"测试完成，总收益率: {total_return:.2%}, 夏普比率: {sharpe_ratio:.2f}, 最大回撤: {max_drawdown:.2%}")
        
        return test_results
    
    def save_model(self, model_name: str = "ppo_model"):
        """
        保存模型
        
        Args:
            model_name: 模型名称
        """
        if self.agent is None:
            self.logger.warning("没有模型可保存")
            return
        
        model_path = os.path.join(self.model_dir, f"{model_name}.pth")
        self.agent.save_model(model_path)
        self.logger.info(f"模型保存至 {model_path}")
    
    def load_model(self, model_name: str = "ppo_model"):
        """
        加载模型
        
        Args:
            model_name: 模型名称
        """
        model_path = os.path.join(self.model_dir, f"{model_name}.pth")
        
        if not os.path.exists(model_path):
            self.logger.warning(f"模型文件 {model_path} 不存在")
            return False
        
        if self.agent is None:
            self.logger.warning("请先初始化代理")
            return False
        
        self.agent.load_model(model_path)
        self.trained = True
        self.logger.info(f"模型从 {model_path} 加载成功")
        return True

    def generate_trading_signal(self, state):
        """
        生成交易信号
        
        Args:
            state: 当前状态
            
        Returns:
            交易信号字典
        """
        if not self.trained or self.agent is None:
            return {'signal': 'neutral', 'confidence': 0.5}
        
        # 获取动作和概率
        with torch.no_grad():
            try:
                state_tensor = torch.FloatTensor(state).unsqueeze(0).to(self.agent.device)
                action_probs, _ = self.agent.policy(state_tensor)
                action_probs = action_probs.squeeze().cpu().numpy()
                
                # 打印调试信息
                logger.info(f"动作概率: {action_probs}")
                
                # 选择动作
                action = np.argmax(action_probs)
                
                # 转换为交易信号
                signal_map = {0: 'neutral', 1: 'bullish', 2: 'bearish'}
                
                # 确保动作在有效范围内
                if action not in signal_map:
                    logger.info(f"警告: 无效的动作索引 {action}")
                    action = 0  # 默认为neutral
                
                # 计算置信度
                confidence = float(action_probs[action])
                
                # 构建动作概率字典
                action_prob_dict = {}
                for i, prob in enumerate(action_probs):
                    if i in signal_map:
                        action_prob_dict[signal_map[i]] = float(prob)
                
                # 同时使用action_probs和action_probabilities两个键，保证兼容性
                return {
                    'signal': signal_map[action],
                    'confidence': confidence,
                    'action_probs': action_prob_dict,
                    'action_probabilities': action_prob_dict
                }
            except Exception as e:
                logger.info(f"生成交易信号时出错: {e}")
                import traceback
                traceback.logger.info_exc()
                return {'signal': 'neutral', 'confidence': 0.5}


class RLTradingAgent:
    """强化学习交易Agent，集成到现有系统中"""
    
    def __init__(self, model_dir: str = 'models'):
        """
        初始化RL交易Agent
        
        Args:
            model_dir: 模型保存目录
        """
        self.rl_trader = RLTrader(model_dir=model_dir, device=DEVICE)  # 使用全局设备配置
        self.window_size = 20  # 观察窗口大小
        self.logger = setup_logger('rl_trading_agent')
        self.is_trained = False
    
    def train(self, price_data: pd.DataFrame, tech_indicators: Dict[str, pd.Series] = None):
        """
        训练RL交易模型
        
        Args:
            price_data: 价格数据
            tech_indicators: 技术指标
        """
        try:
            # 准备训练数据
            df = self._prepare_data(price_data, tech_indicators)
            
            # 训练模型
            training_history = self.rl_trader.train(
                df=df,
                n_episodes=500,  # 减少轮数以加快训练
                batch_size=32,
                reward_scaling=1.0
            )
            
            self.is_trained = True
            return training_history
            
        except Exception as e:
            self.logger.error(f"训练模型时出错: {str(e)}")
            return None
    
    def load_model(self, model_name: str = "best_model"):
        """
        加载已训练的模型
        
        Args:
            model_name: 模型名称
        """
        # 创建空环境以初始化agent
        dummy_df = pd.DataFrame({
            'open': [100] * 100,
            'high': [110] * 100,
            'low': [90] * 100,
            'close': [105] * 100,
            'volume': [1000] * 100
        })
        
        # 添加技术指标
        for indicator in ['ma5', 'ma10', 'ma20', 'rsi', 'macd', 'macd_signal', 'macd_hist',
                         'volatility_5d', 'volatility_10d', 'volatility_20d']:
            dummy_df[indicator] = 0
        
        # 创建环境
        self.rl_trader.env = StockTradingEnv(
            df=dummy_df,
            window_size=self.window_size
        )
        
        # 初始化代理
        state_dim = self.rl_trader.env.observation_space.shape[0]
        action_dim = self.rl_trader.env.action_space.n
        
        self.rl_trader.agent = PPOAgent(
            state_dim=state_dim,
            action_dim=action_dim,
            hidden_dim=self.rl_trader.hidden_dim,
            device=DEVICE  # 使用全局设备配置
        )
        
        # 加载模型
        success = self.rl_trader.load_model(model_name)
        self.is_trained = success
        return success
    
    def generate_signals(self, price_data: pd.DataFrame, 
                    tech_indicators: Dict[str, pd.Series] = None) -> Dict[str, Any]:
        """
        生成交易信号
        
        Args:
            price_data: 价格数据
            tech_indicators: 技术指标
            
        Returns:
            包含交易信号的字典
        """
        signals = {}
        
        try:
            # 加载模型(如果尚未加载)
            if not self.is_trained:
                loaded = self.load_model()
                if not loaded:
                    return {'signal': 'neutral', 'confidence': 0.5, 'error': '模型未加载成功'}
            
            # 准备数据
            df = self._prepare_data(price_data, tech_indicators)
            
            # 检查数据量是否足够
            if len(df) < self.window_size + 5:  # 确保有足够的数据
                return {
                    'signal': 'neutral', 
                    'confidence': 0.5, 
                    'error': f'数据量不足，需要至少{self.window_size + 5}个数据点，但只有{len(df)}个'
                }
            
            # 创建环境
            env = StockTradingEnv(
                df=df,
                window_size=self.window_size,
                max_steps=10  # 小步数
            )
            
            # 获取状态
            state = env.reset()

            logger.info(f"\n用于信号生成的状态shape: {state.shape}\n")
            
            # 生成信号
            signal_info = self.rl_trader.generate_trading_signal(state)
            
            # 添加策略分析
            signals['rl_signal'] = signal_info['signal']
            signals['rl_confidence'] = signal_info['confidence']
            
            # 同时尝试两种键名
            if 'action_probabilities' in signal_info:
                signals['action_probabilities'] = signal_info['action_probabilities']
            if 'action_probs' in signal_info:
                signals['action_probs'] = signal_info['action_probs']
            
            # 添加推理分析
            signals['reasoning'] = self._generate_reasoning(signal_info)
            
            # 最终信号
            signals['signal'] = signal_info['signal']
            signals['confidence'] = signal_info['confidence']
            
        except Exception as e:
            self.logger.error(f"生成交易信号时出错: {e}")
            import traceback
            traceback.logger.info_exc()
            signals['signal'] = 'neutral'
            signals['confidence'] = 0.5
            signals['error'] = str(e)
        
        return signals
    
    def _prepare_data(self, price_data: pd.DataFrame, tech_indicators: Dict[str, pd.Series] = None) -> pd.DataFrame:
        """
        准备模型输入数据
        
        Args:
            price_data: 价格数据
            tech_indicators: 技术指标
            
        Returns:
            处理后的DataFrame
        """
        # 复制数据
        df = price_data.copy()
        
        # 确保有必要的列
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"价格数据缺少必要的列: {col}")
        
        # 计算常用技术指标
        # 移动平均线
        df['ma5'] = df['close'].rolling(window=5).mean()
        df['ma10'] = df['close'].rolling(window=10).mean()
        df['ma20'] = df['close'].rolling(window=20).mean()
        
        # RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # MACD
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = df['ema12'] - df['ema26']
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']
        
        # 波动率
        df['volatility_5d'] = df['close'].pct_change().rolling(window=5).std()
        df['volatility_10d'] = df['close'].pct_change().rolling(window=10).std()
        df['volatility_20d'] = df['close'].pct_change().rolling(window=20).std()
        
        # 添加外部技术指标
        if tech_indicators:
            for name, indicator in tech_indicators.items():
                if name not in df.columns:
                    df[name] = indicator
        
        # 填充缺失值
        df = df.ffill().bfill()
        
        return df
    
    def _generate_reasoning(self, signal_info: Dict[str, Any]) -> str:
        """
        根据信号生成决策理由
        
        Args:
            signal_info: 信号信息字典
            
        Returns:
            决策理由
        """
        signal = signal_info['signal']
        confidence = signal_info['confidence']
        
        # 尝试两种可能的键名
        action_probs = {}
        if 'action_probabilities' in signal_info:
            action_probs = signal_info['action_probabilities']
        elif 'action_probs' in signal_info:
            action_probs = signal_info['action_probs']
        
        reasoning = []
        
        # 分析信号
        if signal == 'bullish':
            reasoning.append(f"强化学习模型预测看多信号，置信度: {confidence:.2%}")
            if 'bullish' in action_probs:
                reasoning.append(f"买入概率: {action_probs.get('bullish', 0):.2%}")
        elif signal == 'bearish':
            reasoning.append(f"强化学习模型预测看空信号，置信度: {confidence:.2%}")
            if 'bearish' in action_probs:
                reasoning.append(f"卖出概率: {action_probs.get('bearish', 0):.2%}")
        else:
            reasoning.append(f"强化学习模型预测中性信号，置信度: {confidence:.2%}")
            if 'neutral' in action_probs:
                reasoning.append(f"持有概率: {action_probs.get('neutral', 0):.2%}")
        
        # 分析行为概率分布
        if action_probs:
            probs_str = ", ".join([f"{k}: {v:.2%}" for k, v in action_probs.items()])
            reasoning.append(f"行为概率分布: {probs_str}")
        else:
            reasoning.append("行为概率分布: 无有效数据")
        
        return "; ".join(reasoning)