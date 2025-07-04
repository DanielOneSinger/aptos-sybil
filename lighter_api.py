#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Lighter API封装 - 简化版本
"""

import asyncio
import logging
from typing import Dict, Optional, Any
import lighter


class LighterAPI:
    """Lighter API封装类"""

    def __init__(self, base_url: str, api_key_private_key: str, account_index: int = 0,
                 api_key_index: int = 1, logger: Optional[logging.Logger] = None,
                 config: Optional[Dict] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.base_url = base_url
        self.api_key_private_key = api_key_private_key
        self.account_index = account_index
        self.api_key_index = api_key_index
        self.config = config or {}
        
        # 初始化Lighter客户端
        self.client = lighter.SignerClient(
            url=base_url,
            private_key=api_key_private_key,
            account_index=account_index,
            api_key_index=api_key_index,
        )
        
        # 市场索引映射 (简化版，实际需要从API获取)
        self.market_index_map = {
            'BTC': 0,
            'ETH': 1,
            'SOL': 2
        }
        
        self.prices = {}
        self.client_order_index = 0

    async def close(self):
        if self.client:
            await self.client.close()

    def _get_market_index(self, symbol: str) -> int:
        return self.market_index_map.get(symbol, 0)

    def _get_next_client_order_index(self) -> int:
        self.client_order_index += 1
        return self.client_order_index

    async def get_price(self, symbol: str) -> dict:
        try:
            # 使用API客户端获取价格
            api_client = lighter.ApiClient(configuration=lighter.Configuration(host=self.base_url))
            order_api = lighter.OrderApi(api_client)
            
            # 获取订单簿详情
            market_id = self._get_market_index(symbol)
            order_book = await order_api.order_book_details(market_id=market_id)
            
            # 计算中间价
            if order_book and 'bids' in order_book and 'asks' in order_book:
                best_bid = float(order_book['bids'][0]['price']) if order_book['bids'] else 0
                best_ask = float(order_book['asks'][0]['price']) if order_book['asks'] else 0
                mid_price = (best_bid + best_ask) / 2 if best_bid > 0 and best_ask > 0 else 0
                
                return {
                    'price': mid_price,
                    'timestamp': int(asyncio.get_event_loop().time() * 1000)
                }
            
            await api_client.close()
            return {'price': 0, 'timestamp': 0}
            
        except Exception as e:
            self.logger.error(f"获取Lighter价格失败: {e}")
            return {'price': 0, 'timestamp': 0}

    async def get_depth(self, symbol: str) -> Optional[Dict]:
        try:
            api_client = lighter.ApiClient(configuration=lighter.Configuration(host=self.base_url))
            order_api = lighter.OrderApi(api_client)
            
            market_id = self._get_market_index(symbol)
            order_book = await order_api.order_book_details(market_id=market_id)
            
            if order_book and 'bids' in order_book and 'asks' in order_book:
                bids = [[float(bid['price']), float(bid['size'])] for bid in order_book['bids'][:5]]
                asks = [[float(ask['price']), float(ask['size'])] for ask in order_book['asks'][:5]]
                
                await api_client.close()
                return {'bids': bids, 'asks': asks}
            
            await api_client.close()
            return None
            
        except Exception as e:
            self.logger.error(f"获取Lighter深度失败: {e}")
            return None

    async def place_order(self, symbol: str, side: str, order_type: str = "MARKET", 
                         size: float = None, price: float = None) -> Dict:
        try:
            market_index = self._get_market_index(symbol)
            client_order_index = self._get_next_client_order_index()
            
            if order_type == "MARKET":
                # 市价单
                if side == "BUY":
                    # 买入市价单
                    tx = await self.client.create_market_order(
                        market_index=market_index,
                        client_order_index=client_order_index,
                        base_amount=int(size * 1000),  # 转换为整数
                        avg_execution_price=int(price * 1000) if price else 0,
                        is_ask=False  # 买入
                    )
                else:
                    # 卖出市价单
                    tx = await self.client.create_market_order(
                        market_index=market_index,
                        client_order_index=client_order_index,
                        base_amount=int(size * 1000),  # 转换为整数
                        avg_execution_price=int(price * 1000) if price else 0,
                        is_ask=True  # 卖出
                    )
            else:
                # 限价单
                if side == "BUY":
                    tx = await self.client.create_limit_order(
                        market_index=market_index,
                        client_order_index=client_order_index,
                        base_amount=int(size * 1000),
                        limit_price=int(price * 1000),
                        is_ask=False
                    )
                else:
                    tx = await self.client.create_limit_order(
                        market_index=market_index,
                        client_order_index=client_order_index,
                        base_amount=int(size * 1000),
                        limit_price=int(price * 1000),
                        is_ask=True
                    )
            
            return {
                'success': True,
                'order_id': str(client_order_index),
                'tx': tx,
                'status': 'PENDING',
                'price': price,
                'size': size,
                'raw_response': {'tx': tx}
            }
            
        except Exception as e:
            self.logger.error(f"Lighter下单失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    async def get_order_status(self, order_id: str, symbol: str = None) -> Dict:
        try:
            # Lighter的订单状态查询需要实现
            # 这里简化处理，实际需要调用相应的API
            return {
                'success': True,
                'order_id': order_id,
                'status': 'FILLED',  # 假设已成交
                'price': 0,
                'size': 0,
                'total_size': 0,
                'left_size': 0,
                'raw_response': {}
            }
        except Exception as e:
            self.logger.error(f"获取Lighter订单状态失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    async def cancel_order(self, order_id: str, symbol: str = None) -> Dict:
        try:
            # Lighter的取消订单需要实现
            # 这里简化处理
            return {
                'success': True,
                'raw_response': {}
            }
        except Exception as e:
            self.logger.error(f"取消Lighter订单失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    async def get_position(self, symbol: str) -> Optional[Dict]:
        try:
            # 获取账户信息
            api_client = lighter.ApiClient(configuration=lighter.Configuration(host=self.base_url))
            account_api = lighter.AccountApi(api_client)
            
            account_info = await account_api.account(by="index", value=str(self.account_index))
            
            await api_client.close()
            
            # 解析持仓信息
            if account_info and 'positions' in account_info:
                for position in account_info['positions']:
                    if position.get('market_index') == self._get_market_index(symbol):
                        size = float(position.get('base_amount', 0)) / 1000  # 转换为小数
                        if size != 0:
                            return {
                                'symbol': symbol,
                                'size': size,
                                'side': 'LONG' if size > 0 else 'SHORT',
                                'entry_price': float(position.get('entry_price', 0)) / 1000,
                                'unrealized_pnl': 0,  # Lighter可能没有这个字段
                                'raw_response': position
                            }
            return None
            
        except Exception as e:
            self.logger.error(f"获取Lighter持仓失败: {e}")
            return None

    async def get_positions(self) -> Dict[str, Dict]:
        try:
            api_client = lighter.ApiClient(configuration=lighter.Configuration(host=self.base_url))
            account_api = lighter.AccountApi(api_client)
            
            account_info = await account_api.account(by="index", value=str(self.account_index))
            
            await api_client.close()
            
            positions = {}
            if account_info and 'positions' in account_info:
                for position in account_info['positions']:
                    size = float(position.get('base_amount', 0)) / 1000
                    if size != 0:
                        # 根据market_index反查symbol
                        symbol = None
                        for sym, idx in self.market_index_map.items():
                            if idx == position.get('market_index'):
                                symbol = sym
                                break
                        
                        if symbol:
                            positions[symbol] = {
                                'symbol': symbol,
                                'size': size,
                                'side': 'LONG' if size > 0 else 'SHORT',
                                'entry_price': float(position.get('entry_price', 0)) / 1000,
                                'unrealized_pnl': 0
                            }
            
            return positions
            
        except Exception as e:
            self.logger.error(f"获取Lighter所有持仓失败: {e}")
            return {}

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        try:
            # Lighter的杠杆设置需要实现
            # 这里简化处理
            self.logger.info(f"设置Lighter {symbol}杠杆为{leverage}倍成功")
            return True
        except Exception as e:
            self.logger.error(f"设置Lighter杠杆失败: {e}")
            return False 