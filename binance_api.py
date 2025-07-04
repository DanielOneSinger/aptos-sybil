#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
gate交易所API封装

提供与gate交易所交互的功能，包括获取价格、资金费率、下单、查询仓位等
"""

import time
import json
import hmac
import hashlib
import base64
import asyncio
import uuid
from typing import Dict, List, Optional, Any, Tuple, Union
from decimal import Decimal
import math
import traceback
import urllib.parse

import httpx
import websockets
import logging
import nacl.signing  # 添加PyNaCl库依赖

import hmac
import hashlib
from Crypto.Hash import SHA256, HMAC
from cryptography.hazmat.primitives.serialization import load_pem_private_key

from funding_arbitrage_bot.exchanges.base_exchange import BaseExchange
from funding_arbitrage_bot.utils import helpers


class BinanceAPI(BaseExchange):
    """binance交易所API封装类"""

    def __init__(
            self,
            api_key: str,
            api_secret: str,
            base_url: str = "https://fapi.binance.com",
            ws_url: str = "wss://fstream.binance.com/stream",
            logger: Optional[logging.Logger] = None,
            config: Optional[Dict] = None
    ):
        """
        初始化gate API

        Args:
            api_key: API密钥
            api_secret: API密钥对应的密钥
            base_url: REST API基础URL
            ws_url: WebSocket API URL
            logger: 日志记录器，如果为None则使用默认日志记录器
            config: 配置信息，包含交易对精度等设置
        """
        self.logger = logger or logging.getLogger(__name__)
        self.api_key = api_key.strip()

        # 保存原始的API密钥
        self.api_secret = api_secret.strip()

        # 尝试解码
        try:
            self.api_secret_bytes = base64.b64decode(self.api_secret)
            self.logger.info("成功解码API密钥")

            # 如果密钥长度大于32字节，仅取前32字节
            if len(self.api_secret_bytes) > 32:
                self.api_secret_bytes = self.api_secret_bytes[:32]
                self.logger.info(f"API密钥长度为{len(self.api_secret_bytes)}字节，截取前32字节")
        except Exception as e:
            self.logger.warning(f"无法解码API密钥，可能导致签名问题: {e}")
            self.api_secret_bytes = self.api_secret.encode()

        self.base_url = base_url
        self.ws_url = ws_url

        # 保存配置信息
        self.config = config or {}

        # 价格和资金费率缓存
        self.prices = {}
        self.funding_rates = {}
        # 24小时交易量缓存
        self.volumes = {}
        # 24小时成交数缓存
        self.trade_counts = {}

        # 新的数据结构，格式为 {"BTC": {"symbol": "BTCUSDT", "funding_rate": "0.111", "funding_rate_timestamp": "xxxx", "price": 1, "price_timestamp": "xxxx"}}
        self.coin_data = {}

        # HTTP客户端
        self.http_client = httpx.AsyncClient(timeout=10.0)

        # WebSocket连接
        self.ws = None
        self.ws_connected = False
        self.ws_task = None

        # 默认请求超时时间
        self.default_window = 5000  # 5秒

        # 价格日志输出控制
        self.last_price_log = {}
        self.price_log_interval = 300  # 每5分钟记录一次价格

        self.price_coins = []

        self.depth_data = {}

        self.ws_api_key = self.config.get('exchanges', {}).get('binance', {}).get('ed25519_api_key', '')

        PRIVATE_KEY_PATH = self.config.get('exchanges', {}).get('binance', {}).get('ed25519_api_secret_path', '')
        with open(PRIVATE_KEY_PATH, 'rb') as f:
            self.ws_private_key = load_pem_private_key(data=f.read(), password=None)

        self.ws_trade = None
        self.ws_trade_connected = False
        self.ws_trade_task = None
        self.ws_trade_pending_message = {}
        self.ws_trade_pending_futures = {}  # 新增：id -> Future

    def create_ed25519_sign(self, request: dict):
        # Create signature payload from params
        params = request['params']
        payload = '&'.join([f'{k}={v}' for k, v in sorted(params.items())])
        signature = base64.b64encode(self.ws_private_key.sign(payload.encode('ASCII')))
        return signature

    async def close(self):
        """关闭API连接"""
        if self.http_client:
            await self.http_client.aclose()

        if self.ws_task:
            self.ws_task.cancel()
            try:
                await self.ws_task
            except asyncio.CancelledError:
                pass

    def gen_sign(self, origin_data: dict):
        channel = origin_data.get('channel')
        event = origin_data.get('event')
        current_time = origin_data.get('current_time')
        message = 'channel=%s&event=%s&time=%d' % (channel, event, current_time)
        h = hmac.new(self.api_secret.encode("utf8"), message.encode("utf8"), hashlib.sha512)
        return h.hexdigest()

    def gen_sign_rest(self, params: dict) -> str:
        """
        使用 HMAC-SHA256 算法生成币安API签名

        Args:
            params (dict): 请求参数

        Returns:
            str: 十六进制格式的签名
        """
        # 确保所有参数值都是字符串
        params = {k: str(v) for k, v in params.items()}

        # 按字母顺序排序并生成查询字符串（不进行URL编码）
        query_string = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])

        try:
            # 创建 HMAC 对象
            hmac_obj = HMAC.new(
                key=self.api_secret.encode('utf-8'),
                msg=query_string.encode('utf-8'),
                digestmod=SHA256
            )
            # 计算签名并转换为十六进制字符串
            signature = hmac_obj.hexdigest()
            return signature
        except Exception as e:
            raise RuntimeError(f"Failed to calculate hmac-sha256: {str(e)}")

    async def _make_signed_request(
            self,
            instruction: str,
            method: str,
            endpoint: str,
            params: Optional[Dict] = None,
            data: Optional[Dict] = None
    ) -> Dict:
        """
        发送带签名的API请求
        todo  sign参数问题暂时废弃
        Args:
            instruction: API指令名称
            method: HTTP方法（GET、POST等）
            endpoint: API端点（不包含基础URL）
            params: URL参数
            data: 请求体数据

        Returns:
            API响应数据
        """
        # 准备请求参数
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

        # if method == 'GET':
        # return await self.http_client.get(url=f'{self.base_url}{endpoint}', headers=headers)

        pass

    async def get_price(self, symbol: str) -> dict:
        base_symbol = symbol
        if base_symbol in self.coin_data and self.coin_data[base_symbol]["price"] is not None:
            return {
                "price": self.coin_data[base_symbol]["price"],
                "price_timestamp": self.coin_data[base_symbol]["price_timestamp"]
            }
        else:
            return {
                "price": None,
                "price_timestamp": None
            }

    async def get_funding_rate(self, symbol: str) -> dict:
        # print(self.coin_data)
        # 从coin_data中获取资金费率
        if symbol in self.coin_data and self.coin_data[symbol]["funding_rate"] is not None:
            return {
                "funding_rate": self.coin_data[symbol]["funding_rate"],
                "funding_rate_timestamp": self.coin_data[symbol]["funding_rate_timestamp"]
            }
        else:
            return {
                "funding_rate": None,
                "funding_rate_timestamp": None
            }

    async def place_order_rest(self,
                               symbol: str,
                               side: str,
                               order_type: str = "MARKET",
                               size: float = None,
                               price: float = None,
                               tif: str = "gtc",
                               is_close=False):
        try:
            place_order_url = f'{self.base_url}/fapi/v1/order'
            bn_symbol = helpers.get_binance_symbol(symbol)
            # 处理价格格式化 (对LIMIT订单)
            formatted_price = ""
            trading_pair_config = None
            for pair in self.config.get("trading_pairs", []):
                if pair.get("symbol") == symbol:
                    trading_pair_config = pair
                    break
            if order_type == "LIMIT" and price:

                # 从配置中获取价格精度和tick_size
                price_precision = 2  # 默认精度
                tick_size = 0.01  # 默认tick_size
                if trading_pair_config:
                    price_precision = int(trading_pair_config.get("price_precision", 2))
                    tick_size = float(trading_pair_config.get("tick_size", 0.01))

                    # 确保价格是浮点数
                    price = float(price)

                    # 调整价格为tick_size的倍数
                    price = round(price / tick_size) * tick_size

                    # 控制小数位数，确保不超过配置的精度
                    price = round(price, price_precision)

                    # 格式化价格，确保精度符合要求
                    formatted_price = "{:.{}f}".format(price, price_precision)

                    # 去除尾部多余的0
                    formatted_price = formatted_price.rstrip('0').rstrip(
                        '.') if '.' in formatted_price else formatted_price

                    self.logger.info(
                        f"价格处理: 原始价格={price}, 格式化后={formatted_price}, 精度={price_precision}, tick_size={tick_size}")
                    print(
                        f"Binance 开仓：价格处理: 原始价格={price}, 格式化后={formatted_price}, 精度={price_precision}, tick_size={tick_size}")

            # 构造请求参数
            params = {
                "symbol": bn_symbol,
                "side": side,
                "type": order_type,
                "quantity": str(size),
                "timestamp": str(int(time.time() * 1000)),
                "timeInForce": tif.upper()
            }

            # 添加限价单特有参数
            if order_type == "LIMIT":
                params.update({
                    "price": str(formatted_price)
                })

            # 生成签名
            signature = self.gen_sign_rest(params)

            # 构造完整的查询字符串，包括签名
            # 对每个参数值进行URL编码
            encoded_params = {k: urllib.parse.quote(str(v)) for k, v in params.items()}
            query_string = '&'.join([f"{k}={v}" for k, v in sorted(encoded_params.items())])
            query_string = f"{query_string}&signature={signature}"

            # 构造完整URL
            url = f"{place_order_url}?{query_string}"

            # 设置请求头
            headers = {'X-MBX-APIKEY': self.api_key}

            self.logger.debug(f"Binance下单URL: {url}")
            print(f"Binance下单URL: {url}")
            order_res = await self.http_client.post(url, headers=headers)
            self.logger.info(f"Binance下单结果: {order_res.text}")
            print(f"Binance下单结果: {order_res.text}")
            if order_res.status_code == 200:
                data = order_res.json()
                if "orderId" in data:
                    return {
                        "success": True,
                        "order_id": data['orderId'],
                        "error": None,
                        "symbol": symbol,
                        "side": side,
                        "size": data['origQty'],
                        "price": data.get('price'),
                        "type": order_type,
                        "raw_response": data
                    }

            return {
                "success": False,
                "error": order_res.text,
                "symbol": symbol,
                "side": side,
                "type": order_type,
                "raw_response": order_res.text
            }

        except Exception as e:
            self.logger.error(f"Binance下单异常: {e}")
            return {
                "success": False,
                "error": str(e),
                "symbol": symbol,
                "side": side,
                "type": order_type,
                "raw_response": str(e)
            }

    async def place_order(
            self,
            symbol: str,
            side: str,
            order_type: str = "MARKET",
            size: float = None,
            price: float = None,
            tif: str = "gtc",
            is_close=False
    ) -> Dict:
        """
        币安下单接口
        """

        if self.ws_trade and self.ws_trade_connected:
            bn_symbol = helpers.get_binance_symbol(symbol)
            # 处理价格格式化 (对LIMIT订单)
            formatted_price = ""
            trading_pair_config = None
            for pair in self.config.get("trading_pairs", []):
                if pair.get("symbol") == symbol:
                    trading_pair_config = pair
                    break
            if order_type == "LIMIT" and price:

                # 从配置中获取价格精度和tick_size
                price_precision = 2  # 默认精度
                tick_size = 0.01  # 默认tick_size
                if trading_pair_config:
                    price_precision = int(trading_pair_config.get("price_precision", 2))
                    tick_size = float(trading_pair_config.get("tick_size", 0.01))

                    # 确保价格是浮点数
                    price = float(price)

                    # 调整价格为tick_size的倍数
                    price = round(price / tick_size) * tick_size

                    # 控制小数位数，确保不超过配置的精度
                    price = round(price, price_precision)

                    # 格式化价格，确保精度符合要求
                    formatted_price = "{:.{}f}".format(price, price_precision)

                    # 去除尾部多余的0
                    formatted_price = formatted_price.rstrip('0').rstrip(
                        '.') if '.' in formatted_price else formatted_price

                    self.logger.info(
                        f"价格处理: 原始价格={price}, 格式化后={formatted_price}, 精度={price_precision}, tick_size={tick_size}")
                    print(
                        f"Binance 开仓：价格处理: 原始价格={price}, 格式化后={formatted_price}, 精度={price_precision}, tick_size={tick_size}")

            params = {
                "apiKey": self.ws_api_key,
                "symbol": bn_symbol,
                "side": side,
                "type": order_type,
                "quantity": str(size)

            }
            if order_type == "LIMIT":
                params.update({
                    "price": str(formatted_price),
                    "timeInForce": tif.upper()
                })
            order_uuid = str(uuid.uuid4())
            request = {
                'id': order_uuid,
                'method': 'order.place',
                'params': params
            }
            timestamp = int(time.time() * 1000)  # 以毫秒为单位的 UNIX 时间戳
            request['params']['timestamp'] = timestamp
            signature = self.create_ed25519_sign(request)
            request['params']['signature'] = signature.decode('ASCII')
            print(f'binance trade_json = {request}')
            self.logger.info(f'binance trade_json = {request}')
            # 新增：下单前注册Future
            loop = asyncio.get_event_loop()
            fut = loop.create_future()
            self.ws_trade_pending_futures[order_uuid] = fut
            await self.ws_trade.send(json.dumps(request))
            try:
                # 等待响应，超时可自定义
                response_json = await asyncio.wait_for(fut, timeout=10)
            except asyncio.TimeoutError:
                self.ws_trade_pending_futures.pop(order_uuid, None)
                return {
                    "success": False,
                    "error": "WebSocket下单超时",
                    "symbol": symbol,
                    "side": side,
                    "type": order_type,
                    "raw_response": None
                }
            print(f'binance ws下单结果: {response_json}')
            self.logger.info(f'binance ws下单结果: {response_json}')
            data = response_json['result']
            if "orderId" in data:
                return {
                    "success": True,
                    "order_id": data['orderId'],
                    "error": None,
                    "symbol": symbol,
                    "side": side,
                    "size": data['origQty'],
                    "price": data.get('price'),
                    "type": order_type,
                    "raw_response": data
                }
            else:
                return {
                    "success": False,
                    "error": data,
                    "symbol": symbol,
                    "side": side,
                    "type": order_type,
                    "raw_response": response_json
                }

        else:
            return await self.place_order_rest(symbol, side, order_type, size, price, tif, is_close)

    async def get_order_status(self, order_id: str, symbol: str = None) -> Dict:
        """
        获取订单状态

        Args:
            symbol: 交易对，如 "BTC"
            order_id: 订单ID

        Returns:
            订单状态信息
        """
        try:
            url = f'{self.base_url}/fapi/v1/order'
            params = {
                'symbol': helpers.get_binance_symbol(symbol),
                'orderId': order_id,
                'timestamp': str(int(time.time() * 1000))
            }
            headers = {'X-MBX-APIKEY': self.api_key}
            # 生成签名
            signature = self.gen_sign_rest(params)

            # 构造完整的查询字符串，包括签名
            encoded_params = {k: urllib.parse.quote(str(v)) for k, v in params.items()}
            query_string = '&'.join([f"{k}={v}" for k, v in sorted(encoded_params.items())])
            query_string = f"{query_string}&signature={signature}"
            url = f"{url}?{query_string}"
            response = await self.http_client.get(url, headers=headers)
            print(f'binance get_order_status response: {response.text}')
            self.logger.info(f'binance get_order_status response: {response.text}')

            res_json = response.json()
            status_res = {
                'success': False,  # 请求状态
                'total_size': '',  # 订单总size
                'price': '',  # 订单价格
                'left_size': '',  # 订单剩余size
                'status': 'unknown',  # 订单状态   open/finished (转换)
                'finish_status': None,  # 订单结束状态(取消: cancelled, 完成：filled)
                'raw_response': response.json()
            }
            if response.status_code == 200:
                if 'orderId' in res_json:
                    status_res['success'] = True
                    total_size = res_json['origQty']
                    status_res['total_size'] = total_size
                    # execute_qty = res_json['executedQty']
                    execute_qty = res_json.get('executedQty', 0)
                    status_res['left_size'] = float(total_size) - float(execute_qty)
                    status_res['price'] = res_json['price']

                    if res_json['status'] == 'FILLED':
                        status_res['status'] = 'finished'
                    elif res_json['status'] == 'NEW':
                        status_res['status'] = 'open'

                    if float(total_size) - float(execute_qty) == 0:
                        status_res['finish_status'] = 'filled'

            return status_res
        except Exception as e:
            print(e)
            self.logger.error(e)
            return {
                'success': False,  # 请求状态
                'total_size': '',  # 订单总size
                'price': '',  # 订单价格
                'left_size': '',  # 订单剩余size
                'status': 'unknown',  # 订单状态   open/finished (转换)
                'finish_status': None,  # 订单结束状态(取消: cancelled, 完成：filled)
                'raw_response': e,
                'error': e
            }

    async def get_position(self, symbol: str) -> Optional[Dict]:
        """
        获取当前持仓

        Args:
            symbol: 交易对，如 "BTCUSDT"

        Returns:
            持仓信息，如果无持仓则返回None
        """
        try:
            # 构造请求参数
            params = {
                'symbol': helpers.get_binance_symbol(symbol),
                'timestamp': str(int(time.time() * 1000))
            }

            # 生成签名
            signature = self.gen_sign_rest(params)

            # 构造完整的查询字符串，包括签名
            encoded_params = {k: urllib.parse.quote(str(v)) for k, v in params.items()}
            query_string = '&'.join([f"{k}={v}" for k, v in sorted(encoded_params.items())])
            query_string = f"{query_string}&signature={signature}"

            # 构造完整URL
            url = f"{self.base_url}/fapi/v3/positionRisk?{query_string}"

            # 设置请求头
            headers = {'X-MBX-APIKEY': self.api_key}

            self.logger.debug(f"Binance请求持仓URL: {url}")
            response = await self.http_client.get(url, headers=headers)
            self.logger.debug(f"Binance请求持仓响应: {response.text}")

            # 检查响应状态
            if response.status_code == 200:
                data = response.json()

                # 解析持仓数据
                if isinstance(data, list):
                    for position in data:
                        position_symbol = position.get("symbol", "")
                        if not position_symbol or position_symbol != helpers.get_binance_symbol(symbol):
                            continue

                        position_amt = float(position.get("positionAmt", "0"))

                        # 跳过零持仓
                        if position_amt == 0:
                            self.logger.debug(f"跳过零持仓: {position_symbol}")
                            continue

                        # 确定持仓方向
                        side = "BUY" if position_amt > 0 else "SELL"
                        abs_size = abs(position_amt)

                        return {
                            "symbol": position_symbol,
                            "side": side,
                            "size": abs_size,
                            "entry_price": float(position.get("entryPrice", 0)),
                            "mark_price": float(position.get("markPrice", 0)),
                            "unrealized_pnl": float(position.get("unRealizedProfit", 0))
                        }

            self.logger.warning(f"未找到{symbol}的持仓信息")
            return None

        except Exception as e:
            self.logger.error(f"获取{symbol}持仓信息失败: {e}")
            self.logger.debug(f"异常详情: {traceback.format_exc()}")
            return None

    async def close_position_rest(self, symbol: str, size: Optional[float] = None, order_type="LIMIT", price=""):
        """
        平仓操作
        """
        try:
            bn_symbol = helpers.get_binance_symbol(symbol)
            current_position = await self.get_position(symbol)
            print(f'Binance平仓， 当前持仓:{symbol}, {current_position}')
            if not current_position:
                return {
                    "success": False,
                    "error": "No position found",
                    "symbol": symbol
                }
            trading_pair_config = None
            for pair in self.config.get("trading_pairs", []):
                if pair.get("symbol") == symbol:
                    trading_pair_config = pair
                    break

            cur_side = current_position['side']
            side = "BUY" if cur_side == "SELL" else "SELL"

            # 构造请求参数
            params = {
                "symbol": bn_symbol,
                "side": side,
                "type": order_type,
                "quantity": current_position.get('size'),
                "reduceOnly": "true",
                "timestamp": str(int(time.time() * 1000))
            }

            if order_type == "LIMIT":
                formatted_price = price
                if order_type == "LIMIT" and price:
                    # 从配置中获取价格精度和tick_size
                    price_precision = 2  # 默认精度
                    tick_size = 0.01  # 默认tick_size
                    if trading_pair_config:
                        price_precision = int(trading_pair_config.get("price_precision", 2))
                        tick_size = float(trading_pair_config.get("tick_size", 0.01))
                    # 确保价格是浮点数
                    price = float(price)

                    # 调整价格为tick_size的倍数
                    price = round(price / tick_size) * tick_size

                    # 控制小数位数，确保不超过配置的精度
                    price = round(price, price_precision)

                    # 格式化价格，确保精度符合要求
                    formatted_price = "{:.{}f}".format(price, price_precision)

                    # 去除尾部多余的0
                    formatted_price = formatted_price.rstrip('0').rstrip(
                        '.') if '.' in formatted_price else formatted_price

                    self.logger.info(
                        f"价格处理: 原始价格={price}, 格式化后={formatted_price}, 精度={price_precision}, tick_size={tick_size}")
                    print(
                        f"Binance 平仓：价格处理: 原始价格={price}, 格式化后={formatted_price}, 精度={price_precision}, tick_size={tick_size}")

                params.update({
                    "timeInForce": "GTC",
                    "price": str(formatted_price)
                })

            # 生成签名
            signature = self.gen_sign_rest(params)

            # 构造完整的查询字符串，包括签名
            encoded_params = {k: urllib.parse.quote(str(v)) for k, v in params.items()}
            query_string = '&'.join([f"{k}={v}" for k, v in sorted(encoded_params.items())])
            query_string = f"{query_string}&signature={signature}"

            # 构造完整URL
            url = f"{self.base_url}/fapi/v1/order?{query_string}"

            # 设置请求头
            headers = {'X-MBX-APIKEY': self.api_key}

            self.logger.debug(f"Binance平仓URL: {url}")
            print(f"Binance平仓param: {params}")
            print(f"Binance平仓URL: {url}")

            response = await self.http_client.post(url, headers=headers)
            self.logger.debug(f"Binance平仓响应: {response.text}")
            print(f"Binance平仓响应: {response.text}")
            if response.status_code == 200:
                data = response.json()
                if "orderId" in data:
                    return {
                        "success": True,
                        "order_id": data['orderId'],
                        "error": None,
                        "symbol": symbol,
                        "side": side,
                        "size": data['origQty'],
                        "price": data.get('price'),
                        "type": order_type,
                        "raw_response": data
                    }

            return {
                "success": False,
                "error": response.text,
                "symbol": symbol,
                "side": side,
                "type": order_type,
                "raw_response": response.text
            }

        except Exception as e:
            self.logger.error(f"Binance平仓异常: {e}")
            print(f"Binance平仓异常: {e}")
            return {
                "success": False,
                "error": str(e),
                "symbol": symbol,
                "type": order_type,
                "raw_response": str(e)
            }

    async def close_position(self, symbol: str, size: Optional[float] = None, order_type="LIMIT", price="",
                             side: str = None) -> Dict:
        if self.ws_trade and self.ws_trade_connected:
            """
           平仓操作
           side ：持仓方向 （是当前的持仓方向 long 为BUY, short 为 SELL）
           """
            bn_symbol = helpers.get_binance_symbol(symbol)
            print(f'Binance平仓， 当前持仓:{symbol}: {size}, 持仓方向: {side}')
            self.logger.info(f'Binance平仓， 当前持仓:{symbol}: {size}, 持仓方向: {side}')
            trading_pair_config = None
            for pair in self.config.get("trading_pairs", []):
                if pair.get("symbol") == symbol:
                    trading_pair_config = pair
                    break

            cur_side = side
            side = "BUY" if cur_side == "SELL" else "SELL"

            # 构造请求参数
            params = {
                "apiKey": self.ws_api_key,
                "symbol": bn_symbol,
                "side": side,
                "quantity": size,
                "type": order_type,
                "reduceOnly": "true",
            }
            if order_type == "LIMIT":
                formatted_price = price
                if order_type == "LIMIT" and price:
                    # 从配置中获取价格精度和tick_size
                    price_precision = 2  # 默认精度
                    tick_size = 0.01  # 默认tick_size
                    if trading_pair_config:
                        price_precision = int(trading_pair_config.get("price_precision", 2))
                        tick_size = float(trading_pair_config.get("tick_size", 0.01))
                    # 确保价格是浮点数
                    price = float(price)

                    # 调整价格为tick_size的倍数
                    price = round(price / tick_size) * tick_size

                    # 控制小数位数，确保不超过配置的精度
                    price = round(price, price_precision)

                    # 格式化价格，确保精度符合要求
                    formatted_price = "{:.{}f}".format(price, price_precision)

                    # 去除尾部多余的0
                    formatted_price = formatted_price.rstrip('0').rstrip(
                        '.') if '.' in formatted_price else formatted_price

                    self.logger.info(
                        f"价格处理: 原始价格={price}, 格式化后={formatted_price}, 精度={price_precision}, tick_size={tick_size}")
                    print(
                        f"Binance 平仓：价格处理: 原始价格={price}, 格式化后={formatted_price}, 精度={price_precision}, tick_size={tick_size}")

                params.update({
                    "timeInForce": "GTC",
                    "price": str(formatted_price)
                })
            order_uuid = str(uuid.uuid4())
            request = {
                'id': order_uuid,
                'method': 'order.place',
                'params': params
            }
            # === 新增：加上timestamp和signature ===
            timestamp = int(time.time() * 1000)  # 以毫秒为单位的 UNIX 时间戳
            request['params']['timestamp'] = timestamp
            signature = self.create_ed25519_sign(request)
            request['params']['signature'] = signature.decode('ASCII')
            print(f'binance trade_json = {request}')
            self.logger.info(f'binance trade_json = {request}')
            # === 新增：Future机制 ===
            loop = asyncio.get_event_loop()
            fut = loop.create_future()
            self.ws_trade_pending_futures[order_uuid] = fut
            await self.ws_trade.send(json.dumps(request))
            try:
                response_json = await asyncio.wait_for(fut, timeout=10)
            except asyncio.TimeoutError:
                self.ws_trade_pending_futures.pop(order_uuid, None)
                return {
                    "success": False,
                    "error": "WebSocket平仓超时",
                    "symbol": symbol,
                    "side": side,
                    "type": order_type,
                    "raw_response": None
                }
            print(f'binance ws下单结果: {response_json}')
            self.logger.info(f'binance ws下单结果: {response_json}')
            data = response_json['result']
            if "orderId" in data:
                return {
                    "success": True,
                    "order_id": data['orderId'],
                    "error": None,
                    "symbol": symbol,
                    "side": side,
                    "size": data['origQty'],
                    "price": data.get('price'),
                    "type": order_type,
                    "raw_response": data
                }
            else:
                return {
                    "success": False,
                    "error": data,
                    "symbol": symbol,
                    "side": side,
                    "type": order_type,
                    "raw_response": response_json
                }
        else:
            return await self.close_position_rest(symbol, size, order_type, price)

    async def start_ws_price_stream(self):
        """
        启动WebSocket价格数据流
        """
        if self.ws_task:
            return

        self.ws_task = asyncio.create_task(self._ws_price_listener())
        self.ws_trade_task = asyncio.create_task(self._ws_trade_task())

    async def _ws_price_listener(self):
        """
        WebSocket价格数据监听器
        """
        while True:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    self.ws = ws
                    self.ws_connected = True
                    self.logger.info("Binance WebSocket已连接")

                    # 获取永续合约交易对列表
                    symbols = await self._get_perp_symbols()

                    # 构建订阅参数
                    subscribe_params = []
                    for symbol in symbols:
                        # 订阅标记价格流
                        # subscribe_params.append(f"{symbol}@markPrice")
                        # subscribe_params.append(f"{symbol}@ticker")
                        # print(s)
                        subscribe_params.append(f"{symbol}@depth5@100ms")

                    subscribe_msg = {
                        "method": "SUBSCRIBE",
                        "params": subscribe_params,
                        "id": 1
                    }

                    # 发送订阅请求
                    await ws.send(json.dumps(subscribe_msg))
                    self.logger.info(f"已向Binance发送订阅请求，共 {len(symbols)} 个交易对")

                    for symbol in symbols:
                        # 订阅标记价格流
                        subscribe_params.append(f"{symbol}@markPrice")
                        # subscribe_params.append(f"{symbol}@ticker")
                    subscribe_msg = {
                        "method": "SUBSCRIBE",
                        "params": subscribe_params,
                        "id": 1
                    }
                    await ws.send(json.dumps(subscribe_msg))

                    # 接收和处理消息
                    while True:
                        message = await ws.recv()

                        data_json = json.loads(message)

                        # 处理确认消息
                        if "result" in data_json and data_json.get("result") == "success":
                            self.logger.info("Binance订阅成功")
                            continue

                        # 处理标记价格更新
                        if "stream" in data_json and "@markPrice" in data_json["stream"]:
                            stream_data = data_json.get("data", {})
                            symbol = stream_data.get("s", "")
                            base_symbol = symbol.split("USDT")[0] if "USDT" in symbol else symbol
                            current_time = time.time()

                            if symbol and "p" in stream_data:
                                try:
                                    mark_price = float(stream_data["p"])
                                    self.prices[symbol] = mark_price
                                    if base_symbol not in self.coin_data:
                                        self.coin_data[base_symbol] = {
                                            "symbol": symbol,
                                            "funding_rate": None,
                                            "funding_rate_timestamp": None,
                                            "price": None,
                                            "price_timestamp": None
                                        }

                                except (ValueError, TypeError) as e:
                                    self.logger.error(
                                        f"无法将Binance标记价格转换为浮点数: {stream_data['p']}, 错误: {e}")

                            # 处理资金费率数据
                            if 'r' in stream_data:
                                try:
                                    funding_rate = float(stream_data['r'])
                                    self.funding_rates[symbol] = funding_rate
                                    if base_symbol in self.coin_data:
                                        self.coin_data[base_symbol]["funding_rate"] = funding_rate
                                        self.coin_data[base_symbol]["funding_rate_timestamp"] = current_time
                                        if hasattr(self, "data_manager") and self.data_manager:
                                            self.data_manager.update_funding_rate(base_symbol, 'binance', funding_rate,
                                                                                  current_time)
                                    else:
                                        self.coin_data[base_symbol] = {
                                            "symbol": symbol,
                                            "funding_rate": funding_rate,
                                            "funding_rate_timestamp": current_time,
                                            "price": None,
                                            "price_timestamp": None
                                        }
                                except (ValueError, TypeError) as e:
                                    self.logger.error(
                                        f"无法将Binance资金费率转换为浮点数: {stream_data['r']}, 错误: {e}")

                            now = time.time()
                            if now - self.last_price_log.get(base_symbol, 0) > self.price_log_interval:
                                self.logger.debug(
                                    f"Binance价格更新: {base_symbol} = {stream_data.get('p', 'N/A')}, 资金费率 = {stream_data.get('r', 'N/A')}")
                                self.last_price_log[base_symbol] = now

                        if "stream" in data_json and "@depth" in data_json["stream"]:
                            stream_data = data_json.get("data", {})
                            symbol = stream_data.get("s", "")
                            base_symbol = symbol.split("USDT")[0] if "USDT" in symbol else symbol
                            bids = stream_data.get("b", [])
                            asks = stream_data.get("a", [])
                            if len(asks) > 0:
                                ask1_price = asks[0][0]
                                ask1_size = asks[0][1]
                            else:
                                ask1_price = None
                                ask1_size = None
                            if len(bids) > 0:
                                bid1_price = bids[0][0]
                                bid1_size = bids[0][1]
                            else:
                                bid1_price = None
                                bid1_size = None

                            # asks、bids统一为gate格式 {"p": "94944.2","s": 18054}
                            asks_format = []
                            for ask in asks:
                                asks_format.append({'p': ask[0], 's': ask[1]})

                            bids_format = []
                            for bid in bids:
                                bids_format.append({'p': bid[0], 's': bid[1]})

                            if hasattr(self, "data_manager") and self.data_manager:
                                timestamp = time.time()
                                self.data_manager.update_price(base_symbol, 'binance', ask1_price, ask1_size,
                                                               bid1_price, bid1_size, asks_format, bids_format,
                                                               timestamp)

            except Exception as e:
                self.ws_connected = False
                self.logger.error(f"Binance WebSocket错误: {e}")

    async def _ws_trade_task(self):
        while True:
            try:
                async with websockets.connect('wss://ws-fapi.binance.com/ws-fapi/v1') as ws:
                    self.ws_trade = ws
                    self.ws_trade_connected = True
                    request = {
                        "id": str(uuid.uuid4()),
                        "method": "session.logon",
                        "params": {
                            "apiKey": self.ws_api_key,
                            "timestamp": int(time.time() * 1000)
                        }
                    }

                    request['params']['signature'] = self.create_ed25519_sign(request).decode('ASCII')
                    print(f'ws 认证: {request}')
                    await self.ws_trade.send(json.dumps(request))
                    response = await self.ws_trade.recv()
                    print(response)
                    login_res = json.loads(response)
                    if 'status' in login_res and login_res['status'] == 200:
                        print(f'binance ws认证成功: {login_res}')
                        self.logger.info(f'binance ws认证成功: {login_res}')
                    last_check_time = time.time()
                    while True:
                        # 每20秒检查一次ws认证状态
                        if time.time() - last_check_time > 20:
                            session_status = {
                                "id": 1,
                                "method": "session.status"
                            }
                            await self.ws_trade.send(json.dumps(session_status))
                            last_check_time = time.time()
                        response = await self.ws_trade.recv()
                        message_json = json.loads(response)
                        if 'id' in message_json:
                            if message_json['id'] == 1:
                                result = message_json['result']
                                api_key = message_json['result']['apiKey']
                                if api_key is None:
                                    print('binance ws认证过期或失败，重连')
                                    self.logger.info('binance ws认证过期或失败，重连')
                                    break
                            else:
                                # 优先通知Future
                                fut = self.ws_trade_pending_futures.pop(message_json['id'], None)
                                if fut and not fut.done():
                                    fut.set_result(message_json)
                                else:
                                    self.ws_trade_pending_message[message_json['id']] = message_json

            except Exception as e:
                print(f'binance ws认证异常:{e}')
                self.logger.error(f'binance ws认证异常:{e}')

    async def _get_perp_symbols(self) -> List[str]:
        """
        获取所有永续合约交易对
        todo 是否从API获取全量

        Returns:
            交易对列表
        """
        # 默认永续合约列表
        default_symbols = []
        for symbol in self.price_coins:
            bn_symbol = helpers.get_binance_symbol(symbol)
            default_symbols.append(bn_symbol)

        perp_symbols = []
        for symbol in default_symbols:
            # 确保符号格式与get_binance_symbol函数一致
            perp_symbols.append(symbol.lower())
        print(f'Binance: 永续合约交易对: {perp_symbols}')
        # try:
        #     # 尝试从API获取交易对列表
        #     response = await self.http_client.get(f"{self.base_url}/api/v4/futures/usdt/contracts")
        #     if response.status_code == 200:
        #         tickers = response.json()
        #         symbols = []
        #         for ticker in tickers:
        #             symbol = ticker.get("name")
        #             symbols.append(symbol)
        #         if symbols:
        #             return symbols
        # except Exception as e:
        #     self.logger.error(f"获取gate交易对列表失败: {e}")
        return perp_symbols

    async def get_positions(self, url_path: str = "/fapi/v3/positionRisk") -> Dict[str, Dict[str, Any]]:
        """
        获取当前所有持仓

        Args:
            url_path: API端点路径，默认为"/fapi/v3/positionRisk"

        Returns:
            持仓信息，格式为: {symbol: position_data}
        """
        try:
            # 构造请求参数
            params = {
                'timestamp': str(int(time.time() * 1000))
            }

            # 生成签名
            signature = self.gen_sign_rest(params)

            # 构造完整的查询字符串，包括签名
            encoded_params = {k: urllib.parse.quote(str(v)) for k, v in params.items()}
            query_string = '&'.join([f"{k}={v}" for k, v in sorted(encoded_params.items())])
            query_string = f"{query_string}&signature={signature}"

            # 构造完整URL
            url = f"{self.base_url}{url_path}?{query_string}"

            # 设置请求头
            headers = {'X-MBX-APIKEY': self.api_key}

            self.logger.debug(f"Binance请求全部持仓URL: {url}")
            # print(f"Binance请求全部持仓URL: {url}")
            response = await self.http_client.get(url, headers=headers)
            self.logger.debug(f"Binance请求全部持仓响应: {response.text}")
            # print(f"Binance请求全部持仓响应: {response.text}")

            # 检查响应状态
            if response.status_code == 200:
                data = response.json()

                # 初始化持仓字典
                positions = {}

                # 解析持仓数据
                if isinstance(data, list):
                    for position in data:
                        symbol = position.get("symbol", "")
                        if not symbol:
                            continue

                        position_amt = float(position.get("positionAmt", "0"))

                        # 跳过零持仓
                        if position_amt == 0:
                            self.logger.debug(f"跳过零持仓: {symbol}")
                            continue

                        # 确定持仓方向
                        side = "BUY" if position_amt > 0 else "SELL"
                        abs_size = abs(position_amt)

                        # 构建持仓信息
                        base_symbol = symbol.split('USDT')[0]
                        positions[base_symbol] = {
                            "symbol": symbol,
                            "side": side,
                            "size": abs_size,
                            "entry_price": float(position.get("entryPrice", 0)),
                            "mark_price": float(position.get("markPrice", 0)),
                            "unrealized_pnl": float(position.get("unRealizedProfit", 0))
                        }

                        self.logger.info(f"Binance持仓: {symbol}, 方向: {side}, 数量: {abs_size}")

                    self.logger.info(f"获取到{len(positions)}个Binance持仓")
                    return positions
                else:
                    self.logger.warning(f"意外的响应格式，期望列表但收到: {type(data)}")
                    return {}
            else:
                self.logger.error(f"获取持仓信息失败，状态码: {response.status_code}, 响应: {response.text}")
                return {}

        except Exception as e:
            self.logger.error(f"获取持仓信息时出错: {e}")
            self.logger.debug(f"异常详情: {traceback.format_exc()}")
            return {}

    def _generate_auth_headers(self, method: str, url_path: str, body: str, timestamp: int) -> Dict[str, str]:
        """
        生成API认证头

        Args:
            method: HTTP方法（GET、POST等）
            url_path: URL路径部分
            body: 请求体（如果有）
            timestamp: 时间戳（毫秒）

        Returns:
            请求头字典
        """
        try:
            # 构建签名字符串
            signature_payload = f"{timestamp}{method}{url_path}{body}"
            self.logger.debug(f"签名负载: {signature_payload}")

            # 使用ED25519算法签名
            signing_key = nacl.signing.SigningKey(self.api_secret_bytes)
            signed_message = signing_key.sign(signature_payload.encode())
            signature = base64.b64encode(signed_message.signature).decode()

            # 构建请求头 - 确保头部字段与Backpack API要求一致
            headers = {
                "X-API-KEY": self.api_key,
                "X-SIGNATURE": signature,
                "X-TIMESTAMP": str(timestamp),
                "X-WINDOW": str(self.default_window),
                "Content-Type": "application/json"
            }

            self.logger.debug(f"生成认证头: {headers}")
            return headers
        except Exception as e:
            self.logger.error(f"生成认证头失败: {e}")
            raise

    async def cancel_order(self, symbol: str, order_id: str) -> Dict:
        """
        取消订单

        Args:
            symbol: 交易对，如 BTC_USDC_PERP
            order_id: 订单ID

        Returns:
            取消订单响应
        """

        if self.ws_trade and self.ws_trade_connected:
            cancel_uuid = str(uuid.uuid4())
            cancel_order_msg = {
                "id": cancel_uuid,
                "method": "order.cancel",
                "params": {
                    "apiKey": self.ws_api_key,
                    "orderId": order_id,
                    "symbol": helpers.get_binance_symbol(symbol),
                }
            }
            # 加签名和时间戳
            timestamp = int(time.time() * 1000)
            cancel_order_msg['params']['timestamp'] = timestamp
            signature = self.create_ed25519_sign(cancel_order_msg)
            cancel_order_msg['params']['signature'] = signature.decode('ASCII')
            print(f'binance cancel_json = {cancel_order_msg}')
            self.logger.info(f'binance cancel_json = {cancel_order_msg}')
            # Future机制
            loop = asyncio.get_event_loop()
            fut = loop.create_future()
            self.ws_trade_pending_futures[cancel_uuid] = fut
            await self.ws_trade.send(json.dumps(cancel_order_msg))
            try:
                response_json = await asyncio.wait_for(fut, timeout=10)
            except asyncio.TimeoutError:
                self.ws_trade_pending_futures.pop(cancel_uuid, None)
                return {
                    'success': False,
                    'order_id': order_id,
                    'raw_response': None,
                    'error': 'WebSocket取消订单超时',
                    'symbol': symbol,
                    'status': 'error'
                }
            print(f'binance ws取消订单结果: {response_json}')
            self.logger.info(f'binance ws取消订单结果: {response_json}')
            data = response_json.get('result', {})
            if response_json.get('status', 200) == 200:
                return {
                    'success': True,
                    'order_id': order_id,
                    'raw_response': response_json,
                    'error': None,
                    'symbol': symbol,
                    'status': data.get('status', 'success')
                }
            else:
                return {
                    'success': False,
                    'order_id': order_id,
                    'raw_response': response_json,
                    'error': data,
                    'symbol': symbol,
                    'status': 'error'
                }
        else:
            return await self.cancel_order_rest(symbol, order_id)

    async def cancel_order_rest(self, symbol: str, order_id: str) -> Dict:
        """
        取消订单（REST方式）

        Args:
            symbol: 交易对，如 BTC_USDC_PERP
            order_id: 订单ID

        Returns:
            取消订单响应
        """
        try:
            cancel_url = f'{self.base_url}/fapi/v1/order'
            bn_symbol = helpers.get_binance_symbol(symbol)
            cancel_payload = {
                "symbol": bn_symbol,
                "orderId": order_id,
                "timestamp": str(int(time.time() * 1000))
            }

            signature = self.gen_sign_rest(cancel_payload)
            encoded_params = {k: urllib.parse.quote(str(v)) for k, v in cancel_payload.items()}
            query_string = '&'.join([f"{k}={v}" for k, v in sorted(encoded_params.items())])
            query_string = f"{query_string}&signature={signature}"
            url = f"{cancel_url}?{query_string}"
            # 设置请求头
            headers = {'X-MBX-APIKEY': self.api_key}
            self.logger.debug(f"Binance取消订单URL: {url}")
            print(f"Binance取消订单URL: {url}")
            response = await self.http_client.delete(url, headers=headers)
            self.logger.info(f"Binance取消订单结果: {response.text}")
            print(f"Binance取消订单结果: {response.text}")
            cancel_res = {
                'success': False,
                'order_id': order_id,
                'raw_response': None,
                'error': None,
                'symbol': symbol,
                'status': 'error'
            }
            if response.status_code != 200:
                cancel_res['raw_response'] = response.json()
                return cancel_res
            elif response.status_code == 200:
                response_json = response.json()
                cancel_res['raw_response'] = response_json
                cancel_res['success'] = True
                cancel_res['status'] = response_json.get('status', 'success')
                return cancel_res
            else:
                cancel_res['raw_response'] = response.json()
                return cancel_res

        except Exception as e:
            self.logger.error(f"binance取消订单时出错: {e}")
            return {
                'success': False,
                'order_id': order_id,
                'raw_response': None,
                'error': e,
                'symbol': symbol,
                'status': 'error'
            }

    async def get_volume(self, symbol: str) -> Optional[float]:
        """
        获取指定交易对的24小时交易量

        Args:
            symbol: 交易对，如 "BTCUSDT"

        Returns:
            24小时交易量，如果无法获取则返回None
        """
        # 如果已经通过WebSocket获取了交易量，直接返回
        if symbol in self.volumes:
            return self.volumes.get(symbol)

        # 否则通过REST API获取
        try:
            response = await self.http_client.get(f"{self.base_url}/fapi/v1/ticker/24hr?symbol={symbol}")
            if response.status_code == 200:
                data = response.json()
                if "volume" in data:
                    volume = float(data["volume"])
                    self.volumes[symbol] = volume
                    return volume
            return None
        except Exception as e:
            self.logger.error(f"获取{symbol} 24小时交易量失败: {e}")
            return None

    async def get_trade_count(self, symbol: str) -> Optional[int]:
        """
        获取指定交易对的24小时成交数

        Args:
            symbol: 交易对，如 "BTCUSDT"

        Returns:
            24小时成交数，如果无法获取则返回None
        """
        # 如果已经通过WebSocket获取了成交数，直接返回
        if symbol in self.trade_counts:
            return self.trade_counts.get(symbol)

        # 否则通过REST API获取
        try:
            response = await self.http_client.get(f"{self.base_url}/fapi/v1/ticker/24hr?symbol={symbol}")
            if response.status_code == 200:
                data = response.json()
                if "count" in data:
                    count = int(data["count"])
                    self.trade_counts[symbol] = count
                    return count
            return None
        except Exception as e:
            self.logger.error(f"获取{symbol} 24小时成交数失败: {e}")
            return None

    async def get_all_funding_rates(self) -> Dict[str, float]:
        """
        批量获取所有交易对的资金费率

        Returns:
            资金费率字典，格式为 {symbol: funding_rate}
        """
        try:
            # 获取所有永续合约交易对
            symbols = await self._get_perp_symbols()

            # 构建请求URL
            url = f"{self.base_url}/fapi/v1/premiumIndex"
            self.logger.debug(f"批量获取资金费率请求URL: {url}")

            # 发送请求
            response = await self.http_client.get(url)

            # 检查响应状态
            if response.status_code != 200:
                self.logger.error(f"批量获取资金费率失败，状态码: {response.status_code}")
                return {}

            # 解析响应
            data = response.json()

            # 检查数据有效性
            if not data or not isinstance(data, list):
                self.logger.warning("批量获取资金费率数据格式异常")
                return {}

            # 提取资金费率
            funding_rates = {}
            for item in data:
                symbol = item.get("symbol", "")
                if not symbol:
                    continue

                if "lastFundingRate" in item:
                    try:
                        funding_rate = float(item["lastFundingRate"])
                        funding_rates[symbol] = funding_rate
                        # 更新缓存
                        self.funding_rates[symbol] = funding_rate
                        self.logger.debug(f"获取到{symbol}资金费率: {funding_rate}")
                    except (ValueError, TypeError) as e:
                        self.logger.error(f"无法将{symbol}资金费率转换为浮点数: {item['lastFundingRate']}, 错误: {e}")

            self.logger.info(f"批量获取资金费率成功，共{len(funding_rates)}个交易对")
            return funding_rates

        except Exception as e:
            self.logger.error(f"批量获取资金费率时出错: {e}")
            return {}

    async def get_depth(self, symbol) -> Optional[Dict]:
        """获取指定币种的深度数据"""
        if symbol in self.depth_data:
            depth_data = self.depth_data[symbol]
            # 检查深度数据是否在1分钟内
            current_time = time.time()
            last_timestamp = depth_data.get("timestamp", 0)
            if current_time - last_timestamp < 60:  # 60秒 = 1分钟
                return depth_data

        await self.get_depths_rest(symbol)
        if symbol in self.depth_data:
            return self.depth_data[symbol]
        return {}

    async def get_depths_rest(self, symbol: str):
        """
        通过REST API获取指定交易对的深度数据

        Args:
            symbol: 交易对，如 "BTC"
        """
        """
        通过REST API获取深度数据

        Args:
            symbol: 交易对，如 "BTC"

        Returns:
            深度数据，包含asks和bids
        """
        bn_symbol = helpers.get_binance_symbol(symbol)
        depth_url = f'https://fapi.binance.com/fapi/v1/depth?symbol={bn_symbol}&limit=100'
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

        try:
            async with httpx.AsyncClient(timeout=60, headers=headers) as client:
                res = await client.get(depth_url)
                depth_data = res.json()

                if 'asks' in depth_data and 'bids' in depth_data:
                    bids = depth_data.get("bids", [])
                    asks = depth_data.get("asks", [])

                    # 转换为统一格式 - Binance返回的是[price, quantity]格式的数组
                    formatted_bids = [[float(bid[0]), float(bid[1])] for bid in bids]
                    formatted_asks = [[float(ask[0]), float(ask[1])] for ask in asks]

                    # 存储深度数据
                    self.depth_data[symbol] = {
                        "bids": formatted_bids,
                        "asks": formatted_asks,
                        "timestamp": time.time()
                    }
                    self.logger.debug(f"已获取{symbol}深度数据: {len(formatted_bids)}买单, {len(formatted_asks)}卖单")
                else:
                    self.logger.warning(f"获取{symbol}深度数据失败，返回格式异常: {depth_data}")
        except Exception as e:
            self.logger.error(f"REST获取{symbol}深度数据时出错: {str(e)}")
            print(f"REST获取{symbol}深度数据时出错: {str(e)}")

    async def get_depths(self):
        """获取所有已订阅币种的深度数据"""
        tasks = [self.get_depths_rest(symbol) for symbol in self.price_coins]
        await asyncio.gather(*tasks)
        return self.depth_data

    async def update_depths(self):
        """获取所有已订阅币种的深度数据"""
        tasks = [self.get_depths_rest(symbol) for symbol in self.price_coins]
        await asyncio.gather(*tasks)
        return self.depth_data

    async def get_klines(self, symbol, interval='1m', limit=50):
        try:
            res = await self.http_client.get(
                f'{self.base_url}/fapi/v1/klines?symbol={helpers.get_binance_symbol(symbol)}&interval={interval}&limit={limit}')
            # print('binance-klines', res.text)
            if 'Invalid symbol' in res.text:
                return None
            raw_klines = res.json()
            klines = []
            for k in raw_klines:
                klines.append({
                    "timestamp": int(k[0]),  # 开盘时间
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5])
                })
            return klines
        except Exception as e:
            print(f'binance获取k线数据失败:{e}')
            self.logger.error(f'binance获取k线数据失败:{e}')
            return None
