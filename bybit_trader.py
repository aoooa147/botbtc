import asyncio
import logging
from typing import Dict, List, Optional, Union
from datetime import datetime
from pybit.unified_trading import HTTP
import json # Import json for pretty printing

from utils import SecureConfig, safe_api_call # Assuming bybit_rate_limiter is handled within safe_api_call or not globally needed
from trading_bot import BaseTrader

logger = logging.getLogger(__name__)

class BybitTrader(BaseTrader):
    def __init__(self, config):
        super().__init__(config, exchange_name='Bybit')
        self.config = config
        self._validate_config()
        
        self.api_key = self.config.get('BYBIT', 'api_key')
        self.api_secret = self.config.get('BYBIT', 'api_secret')
        self.testnet = self.config.getboolean('BYBIT', 'testnet', fallback=False)
        self.default_symbol = self.config.get('BYBIT', 'default_symbol', fallback='BTCUSDT')
        self.default_leverage = self.config.getint('BYBIT', 'default_leverage', fallback=10) 
        self.default_coin = self.config.get('BYBIT', 'default_coin', fallback='USDT')
        self.trading_category = self.config.get('BYBIT', 'category', fallback='linear').lower() 
        self.account_type = self.config.get('BYBIT', 'account_type', fallback='UNIFIED').upper()

        self.session = HTTP(
            testnet=self.testnet,
            api_key=self.api_key,
            api_secret=self.api_secret,
            recv_window=self.config.getint('BYBIT', 'recv_window', fallback=10000)
        )
        
        self._last_balance_update = 0
        self._last_orders_update = 0
        self._last_positions_update = 0
        self._cache_duration = self.config.getint('BYBIT', 'cache_duration_seconds', fallback=5) 
        self._cached_orders: List[Dict] = []
        self._cached_positions: List[Dict] = []
        
        logger.info(f"BybitTrader initialized. Testnet: {self.testnet}, Default Symbol: {self.default_symbol}, Category: {self.trading_category}, AccountType: {self.account_type}")

    def _validate_config(self):
        # Ensure all necessary BYBIT configurations are present
        required_bybit_fields = [
            'api_key', 'api_secret', 'default_symbol', 'default_leverage', 
            'default_coin', 'category', 'account_type',
            'position_size_mode' 
        ]
        if not self.config.has_section('BYBIT'):
            error_msg = "Missing required configuration section: [BYBIT] in config.ini"
            logger.critical(error_msg)
            raise ValueError(error_msg)
        for field in required_bybit_fields:
            if not self.config.get('BYBIT', field): 
                error_msg = f"Missing or empty required configuration field: [BYBIT].{field} in config.ini"
                logger.critical(error_msg)
                raise ValueError(error_msg)
        
        if self.config.get('BYBIT', 'position_size_mode').lower() == 'risk_percentage':
            if not self.config.getfloat('BYBIT', 'risk_per_trade_percentage'):
                error_msg = "Missing [BYBIT].risk_per_trade_percentage for 'risk_percentage' mode."
                logger.critical(error_msg)
                raise ValueError(error_msg)
        elif self.config.get('BYBIT', 'position_size_mode').lower() == 'fixed':
             if not self.config.get('BYBIT', 'max_position_size'): 
                default_symbol_max_pos_key = f"{self.config.get('BYBIT', 'default_symbol', fallback='BTCUSDT').upper()}_max_position_size"
                if not self.config.get('BYBIT', default_symbol_max_pos_key):
                    error_msg = f"Missing [BYBIT].max_position_size or [BYBIT].{default_symbol_max_pos_key} for 'fixed' mode."
                    logger.critical(error_msg)
                    raise ValueError(error_msg)

        logger.info("BybitTrader configuration validated.")

    async def test_connection(self) -> bool:
        try:
            response_data, error = await safe_api_call(self.session.get_server_time)
            if error:
                logger.error(f"Bybit API connection test failed. Error: {error.get('msg', 'Unknown error')}")
                return False
            if response_data and response_data.get('retCode') == 0:
                logger.debug(f"Bybit API connection test successful. Server time (ns): {response_data.get('result', {}).get('timeNano')}")
                return True
            logger.error(f"Bybit API connection test failed. Response: {response_data}")
            return False
        except Exception as e:
            logger.error(f"Exception during Bybit API connection test: {e}", exc_info=True)
            return False

    async def get_wallet_balance(self, accountType: Optional[str] = None, coin_to_filter: Optional[str] = None) -> Dict[str, float]:
        acc_type_to_use = accountType if accountType else self.account_type
        target_coin_to_use = coin_to_filter if coin_to_filter else self.default_coin
        
        coin_available_balance = 0.0 
        final_total_balance = 0.0
        coin_used_margin = 0.0

        try:
            params_for_api = {"accountType": acc_type_to_use}
            if target_coin_to_use and acc_type_to_use != "UNIFIED": 
                 params_for_api["coin"] = target_coin_to_use

            logger.debug(f"Fetching wallet balance with API parameters: {params_for_api}")
            resp_data, error = await safe_api_call(self.session.get_wallet_balance, **params_for_api)

            if resp_data: logger.debug(f"Raw Wallet Balance API Response (accountType: {acc_type_to_use}):\n{json.dumps(resp_data, indent=2)}")
            elif error: logger.error(f"Raw Wallet Balance API Error (accountType: {acc_type_to_use}): {error}")

            if error:
                logger.error(f"Bybit wallet balance API call failed (pre-check): {error.get('msg', 'Unknown error')}")
                return {"total_balance": 0.0, "available_balance": 0.0, "used_margin": 0.0}

            if not resp_data or resp_data.get('retCode') != 0:
                ret_code = resp_data.get('retCode') if resp_data else 'N/A'
                ret_msg = resp_data.get('retMsg') if resp_data else 'No response data'
                logger.error(f"Failed to get wallet balance (Bybit API error). Code: {ret_code}, Msg: {ret_msg}.")
                return {"total_balance": 0.0, "available_balance": 0.0, "used_margin": 0.0}

            result_list = resp_data.get('result', {}).get('list', [])
            if not result_list:
                logger.error(f"Wallet balance response 'result.list' is empty.")
                return {"total_balance": 0.0, "available_balance": 0.0, "used_margin": 0.0}

            account_data = result_list[0] 
            
            total_equity_str = account_data.get('totalEquity', '0')
            total_wallet_balance_str = account_data.get('totalWalletBalance', '0')
            
            final_total_balance = float(total_equity_str or '0') if acc_type_to_use == "UNIFIED" else float(total_wallet_balance_str or '0')
            coin_used_margin = float(account_data.get('totalInitialMargin', '0') or '0') 

            if acc_type_to_use == "UNIFIED":
                total_available_balance_str = account_data.get('totalAvailableBalance', '0')
                coin_available_balance = float(total_available_balance_str or '0')
                logger.debug(f"UNIFIED Account: Using account-level totalAvailableBalance ('{total_available_balance_str}') as coin_available_balance for {target_coin_to_use}.")
            elif acc_type_to_use != "UNIFIED": 
                 coins_array_in_response = account_data.get('coin', [])
                 specific_coin_info = next((c for c in coins_array_in_response if c.get('coin') == target_coin_to_use), None)
                 
                 if specific_coin_info:
                     ab_string = specific_coin_info.get('availableBalance', specific_coin_info.get('availableToWithdraw', '0'))
                     coin_available_balance = float(ab_string or '0')
                     logger.debug(f"{acc_type_to_use} Account, Coin {target_coin_to_use}: Using coin-specific availableBalance/availableToWithdraw ('{ab_string}').")
                 else: 
                     logger.warning(f"Coin '{target_coin_to_use}' not found in {acc_type_to_use} wallet's 'coin' array. Using account-level figures if possible (may be inaccurate).")
                     coin_available_balance = 0.0 
            
            logging.debug(f"Parsed wallet balance for Account Type {acc_type_to_use} (Target Coin: {target_coin_to_use}): "
                         f"Total Balance (Equity/Wallet): {final_total_balance:.4f}, Coin Available: {coin_available_balance:.4f}, Used Margin (Account Initial): {coin_used_margin:.4f}")
            
            return {
                "total_balance": final_total_balance,
                "available_balance": coin_available_balance, 
                "used_margin": coin_used_margin
            }

        except Exception as e:
            logger.error(f"Exception in get_wallet_balance: {str(e)}", exc_info=True)
            return {"total_balance": 0.0, "available_balance": 0.0, "used_margin": 0.0}

    async def get_instruments_info(self, category: Optional[str] = None, symbol: Optional[str] = None) -> Optional[List[Dict]]:
        """Fetches instrument information (tick size, lot size, etc.)."""
        try:
            params = {'category': category if category else self.trading_category}
            if symbol:
                params['symbol'] = symbol
            
            logger.debug(f"Fetching instruments info with params: {params}")
            response_data, error = await safe_api_call(self.session.get_instruments_info, **params)

            if error:
                logger.warning(f"Failed to get instruments info: {error.get('msg', 'Unknown error')}")
                return None
            
            if response_data and response_data.get('retCode') == 0:
                instrument_list = response_data.get('result', {}).get('list', [])
                logger.debug(f"Fetched {len(instrument_list)} instruments info entries.")
                return instrument_list 
            
            ret_msg = response_data.get('retMsg') if response_data else 'No response data'
            logger.warning(f"Failed to get instruments info: {ret_msg}. Full: {response_data}")
            return None
        except Exception as e:
            logger.error(f"Exception in get_instruments_info: {e}", exc_info=True)
            return None

    async def get_tickers(self, category: Optional[str] = None, symbol: Optional[str] = None) -> Optional[List[Dict]]:
        """Fetches ticker information (last price, etc.)."""
        try:
            params = {'category': category if category else self.trading_category}
            if symbol:
                params['symbol'] = symbol
            
            logger.debug(f"Fetching tickers with params: {params}")
            response_data, error = await safe_api_call(self.session.get_tickers, **params)

            if error:
                logger.warning(f"Failed to get tickers: {error.get('msg', 'Unknown error')}")
                return None # Return None on error
            
            if response_data and response_data.get('retCode') == 0:
                ticker_list = response_data.get('result', {}).get('list', [])
                logger.debug(f"Fetched {len(ticker_list)} ticker entries for params {params}.")
                # For a single symbol query, the list usually contains one item.
                return ticker_list 
            
            ret_msg = response_data.get('retMsg') if response_data else 'No response data'
            logger.warning(f"Failed to get tickers (API error {response_data.get('retCode')}): {ret_msg}. Full: {response_data}")
            return None # Return None if API call was not successful (retCode != 0)
        except Exception as e:
            logger.error(f"Exception in get_tickers: {e}", exc_info=True)
            return None


    async def get_open_orders(self, symbol: Optional[str] = None, settleCoin: Optional[str] = None) -> List[Dict]:
        current_time = datetime.now().timestamp()
        if current_time - self._last_orders_update < self._cache_duration and self._cached_orders:
            logger.debug("Returning cached open orders.")
            return self._cached_orders
        try:
            params = {'category': self.trading_category}
            if symbol: params['symbol'] = symbol
            if settleCoin: params['settleCoin'] = settleCoin
            elif not symbol and self.trading_category != 'spot': params['settleCoin'] = self.default_coin 
            
            logger.debug(f"Fetching open orders with params: {params}")
            response_data, error = await safe_api_call(self.session.get_open_orders, **params)
            if error:
                logging.warning(f"Failed to get open orders: {error.get('msg', 'Unknown error')}")
                return [] 
            if response_data and response_data.get('retCode') == 0:
                orders = response_data.get('result', {}).get('list', [])
                self._cached_orders = orders; self._last_orders_update = current_time
                logging.debug(f"Fetched {len(orders)} open orders.")
                return orders
            ret_msg = response_data.get('retMsg') if response_data else 'No response data'
            logging.warning(f"Failed to get open orders: {ret_msg}. Full: {response_data}")
            return []
        except Exception as e:
            logger.error(f"Exception in get_open_orders: {e}", exc_info=True)
            return []


    async def get_open_positions(self, symbol: Optional[str] = None, settleCoin: Optional[str] = None) -> List[Dict]:
        current_time = datetime.now().timestamp()
        if current_time - self._last_positions_update < self._cache_duration and self._cached_positions:
            logger.debug("Returning cached open positions.")
            return self._cached_positions
        try:
            params = {'category': self.trading_category}
            if symbol: params['symbol'] = symbol
            if settleCoin: params['settleCoin'] = settleCoin
            elif not symbol and self.trading_category != 'spot': params['settleCoin'] = self.default_coin

            logger.debug(f"Fetching open positions with params: {params}")
            response_data, error = await safe_api_call(self.session.get_positions, **params)
            if error:
                logging.warning(f"Failed to get open positions: {error.get('msg', 'Unknown error')}")
                return []
            if response_data and response_data.get('retCode') == 0:
                positions = [p for p in response_data.get('result', {}).get('list', []) if float(p.get('size', '0') or '0') > 0]
                self._cached_positions = positions; self._last_positions_update = current_time
                logging.debug(f"Fetched {len(positions)} open positions.")
                return positions
            ret_msg = response_data.get('retMsg') if response_data else 'No response data'
            logging.warning(f"Failed to get open positions: {ret_msg}. Full: {response_data}")
            return []
        except Exception as e:
            logger.error(f"Exception in get_open_positions: {e}", exc_info=True)
            return []

    async def place_order(self, symbol: str, side: str, qty: Union[float, str], 
                         order_type: str = "Market", price: Optional[float] = None,
                         time_in_force: str = "GTC", reduce_only: bool = False,
                         take_profit: Optional[float] = None, stop_loss: Optional[float] = None,
                         tpsl_mode: Optional[str] = "Partial", 
                         tp_trigger_by: Optional[str] = "LastPrice", 
                         sl_trigger_by: Optional[str] = "LastPrice",
                         position_idx: Optional[int] = None 
                         ) -> Optional[Dict]:
        try:
            if order_type.lower() == "limit" and price is None:
                raise ValueError("Price is required for Limit orders.")
            if side not in ["Buy", "Sell"]:
                raise ValueError("Side must be either 'Buy' or 'Sell'.")
            
            qty_str = str(qty) 
            
            params = {
                'category': self.trading_category,
                'symbol': symbol,
                'side': side,
                'orderType': order_type,
                'qty': qty_str,
                'timeInForce': time_in_force,
                'reduceOnly': reduce_only,
            }
            
            if position_idx is not None: 
                params['positionIdx'] = position_idx

            if order_type.lower() == "limit" and price is not None: 
                params['price'] = str(price) 

            if take_profit is not None: 
                params['takeProfit'] = str(take_profit) 
                params['tpTriggerBy'] = tp_trigger_by
            if stop_loss is not None: 
                params['stopLoss'] = str(stop_loss) 
                params['slTriggerBy'] = sl_trigger_by
            
            if take_profit is not None or stop_loss is not None: 
                params['tpslMode'] = tpsl_mode 

            logger.debug(f"Placing order with params: {params}")
            response_data, error = await safe_api_call(self.session.place_order, **params)

            if error:
                logger.error(f"Failed to place order for {symbol}: {error.get('msg', 'Unknown error')}. Params: {params}")
                return {'retCode': error.get('code', -1), 'retMsg': error.get('msg', 'Unknown error'), 'error_details': error} 

            if response_data and response_data.get('retCode') == 0:
                order_result = response_data.get('result', {})
                logger.info(f"Order placed successfully for {symbol}: ID {order_result.get('orderId')}")
                self._last_orders_update = 0 
                self._last_positions_update = 0 
                return response_data 
            
            ret_msg = response_data.get('retMsg', 'No response data') if response_data else 'No response data'
            ret_code = response_data.get('retCode', -1) if response_data else -1
            if "insufficient" in ret_msg.lower() and "balance" in ret_msg.lower():
                 logger.error(f"Failed to place order for {symbol} due to INSUFFICIENT BALANCE: {ret_msg}. Full response: {response_data}")
            else:
                 logger.error(f"Failed to place order for {symbol}: {ret_msg}. Full response: {response_data}")
            return response_data 

        except ValueError as ve: 
            logger.error(f"Order placement validation error for {symbol}: {ve}")
            return {'retCode': -1, 'retMsg': str(ve)}
        except Exception as e:
            logger.error(f"Exception placing order for {symbol}: {e}", exc_info=True)
            return {'retCode': -1, 'retMsg': f"Unexpected exception: {e}"}

    async def cancel_order(self, order_id: Optional[str] = None, order_link_id: Optional[str] = None, symbol: Optional[str] = None) -> bool:
        if not order_id and not order_link_id:
            logger.error("Either order_id or order_link_id is required to cancel an order.")
            return False
        if order_link_id and not symbol: 
            logger.error("Symbol is required when cancelling by order_link_id.")
            return False
        try:
            params = {'category': self.trading_category}
            if symbol: params['symbol'] = symbol 
            else: params['symbol'] = self.default_symbol 
            if order_id: params['orderId'] = order_id
            if order_link_id: params['orderLinkId'] = order_link_id
            
            logger.debug(f"Cancelling order with params: {params}")
            response_data, error = await safe_api_call(self.session.cancel_order, **params)
            
            if error:
                logger.warning(f"Failed to cancel order ({order_id or order_link_id}): {error.get('msg', 'Unknown error')}")
                return False
            if response_data and response_data.get('retCode') == 0:
                logging.info(f"Order ({order_id or order_link_id}) cancelled successfully. Result: {response_data.get('result')}")
                self._last_orders_update = 0 
                return True
            ret_msg = response_data.get('retMsg') if response_data else 'No response data'
            logging.warning(f"Failed to cancel order ({order_id or order_link_id}): {ret_msg}. Details: {response_data}")
            return False
        except Exception as e:
            logger.error(f"Exception cancelling order {order_id or order_link_id}: {e}", exc_info=True)
            return False

    async def cancel_all_orders(self, symbol: Optional[str] = None, settleCoin: Optional[str] = None) -> bool:
        try:
            params = {'category': self.trading_category}
            if symbol: params['symbol'] = symbol
            if settleCoin and self.trading_category != 'spot': params['settleCoin'] = settleCoin
            if not symbol and not settleCoin and self.trading_category != 'spot': params['settleCoin'] = self.default_coin

            logger.debug(f"Cancelling all orders with params: {params}")
            response_data, error = await safe_api_call(self.session.cancel_all_orders, **params)
            if error:
                logging.warning(f"Failed to cancel all orders: {error.get('msg', 'Unknown error')}")
                return False
            if response_data and response_data.get('retCode') == 0:
                cancelled_list = response_data.get('result', {}).get('list', [])
                logging.info(f"All orders for params {params} cancel request submitted. {len(cancelled_list)} orders in response.")
                self._last_orders_update = 0 
                return True
            ret_msg = response_data.get('retMsg') if response_data else 'No response data'
            logging.warning(f"Failed to cancel all orders: {ret_msg}. Details: {response_data}")
            return False
        except Exception as e:
            logger.error(f"Exception cancelling all orders: {e}", exc_info=True)
            return False

    async def close_position(self, symbol: str, side_of_position_to_close: str, qty_to_close: Optional[Union[float, str]] = None, position_idx: Optional[int] = 0) -> Optional[Dict]:
        """Closes a position by placing a reduce-only market order."""
        try:
            open_positions = await self.get_open_positions(symbol=symbol)
            target_position = None
            for pos in open_positions:
                current_pos_idx = pos.get('positionIdx', 0) 
                if pos.get('symbol') == symbol and pos.get('side', '').lower() == side_of_position_to_close.lower() and current_pos_idx == position_idx:
                    target_position = pos
                    break
            
            if not target_position:
                logging.info(f"No open position found for {symbol} with side {side_of_position_to_close} (Idx: {position_idx}) to close.")
                return None

            current_size_str = target_position.get('size', '0') or '0'
            current_size = float(current_size_str)
            if current_size <= 0:
                logging.info(f"Position size for {symbol} {side_of_position_to_close} (Idx: {position_idx}) is already zero or less.")
                return {"message": "Position already zero or closed."} 

            qty_val_to_close = current_size if qty_to_close is None else float(str(qty_to_close)) 
            
            if qty_val_to_close > current_size:
                logging.warning(f"Attempting to close {qty_val_to_close} of {symbol} {side_of_position_to_close} (Idx: {position_idx}), but position size is {current_size}. Closing entire position.")
                qty_val_to_close = current_size
            
            if qty_val_to_close <= 0:
                logging.info(f"Quantity to close for {symbol} {side_of_position_to_close} (Idx: {position_idx}) is zero or negative.")
                return None

            closing_order_side = "Sell" if side_of_position_to_close.lower() == "buy" else "Buy"

            logging.info(f"Attempting to close {qty_val_to_close} of {symbol} (position side {side_of_position_to_close}, Idx: {position_idx}) by placing a {closing_order_side} Market order with reduceOnly=True.")

            return await self.place_order(
                symbol=symbol,
                side=closing_order_side,
                qty=str(qty_val_to_close), 
                order_type="Market",
                reduce_only=True,
                position_idx=position_idx 
            )
        except Exception as e:
            logger.error(f"Exception closing position for {symbol} {side_of_position_to_close}: {e}", exc_info=True)
            return None

    async def close_all_positions(self, settleCoin: Optional[str] = None) -> bool:
        try:
            target_settle_coin = settleCoin if settleCoin else self.default_coin
            logging.info(f"Attempting to close all open positions for settleCoin: {target_settle_coin}")
            all_positions = await self.get_open_positions(settleCoin=target_settle_coin)
            
            if not all_positions:
                logging.info(f"No open positions found for settleCoin {target_settle_coin}.")
                return True

            all_closed_successfully = True
            for position in all_positions:
                symbol = position.get('symbol')
                side = position.get('side') 
                size_str = position.get('size', '0') or '0'
                size = float(size_str)
                pos_idx = position.get('positionIdx', 0) 

                if symbol and side and size > 0:
                    logging.info(f"Closing position: {symbol}, Side: {side}, Size: {size}, Idx: {pos_idx}")
                    close_result = await self.close_position(symbol=symbol, side_of_position_to_close=side, qty_to_close=size, position_idx=pos_idx)
                    if not close_result or close_result.get('retCode', -1) != 0: 
                        all_closed_successfully = False
                        logger.error(f"Failed to initiate close for position {symbol} {side} (Idx: {pos_idx}). Result: {close_result}")
                else:
                    logging.debug(f"Skipping position due to missing data or zero size: {position}")
            
            if all_closed_successfully:
                logging.info(f"Successfully initiated closure for all identified positions for settleCoin {target_settle_coin}.")
            else:
                logging.warning(f"One or more positions failed to close or confirm closure for settleCoin {target_settle_coin}.")
            return all_closed_successfully
        except Exception as e:
            logger.error(f"Error closing all positions for {settleCoin}: {e}", exc_info=True)
            return False

    async def set_leverage(self, symbol: str, buy_leverage: str, sell_leverage: str) -> bool: 
        try:
            logging.debug(f"Setting leverage for {symbol}: Buy={buy_leverage}, Sell={sell_leverage}, Category={self.trading_category}")
            response_data, error = await safe_api_call(
                self.session.set_leverage,
                category=self.trading_category, 
                symbol=symbol,
                buyLeverage=buy_leverage, 
                sellLeverage=sell_leverage 
            )
            if error:
                err_msg_lower = str(error.get('msg', '')).lower()
                if "110043" in err_msg_lower or "leverage not modified" in err_msg_lower:
                    logger.info(f"Leverage for {symbol} already set to Buy: {buy_leverage}, Sell: {sell_leverage}. (Code 110043: Leverage not modified)")
                    return True 
                logger.warning(f"Failed to set leverage for {symbol}: {error.get('msg', 'Unknown error')}")
                return False
            if response_data and response_data.get('retCode') == 0:
                logger.info(f"Leverage set successfully for {symbol} to Buy: {buy_leverage}, Sell: {sell_leverage}")
                return True
            ret_msg = response_data.get('retMsg') if response_data else 'No response data (set_leverage)'
            logging.warning(f"Failed to set leverage for {symbol} (Bybit API non-zero retCode): {ret_msg}. Details: {response_data}")
            return False
        except Exception as e:
            logger.error(f"Exception setting leverage for {symbol}: {e}", exc_info=True)
            return False

    async def get_kline(self, symbol: str, interval: str, limit: int = 200, 
                        start_time: Optional[int] = None, end_time: Optional[int] = None) -> Optional[List[List]]:
        try:
            params = {
                'category': self.trading_category if self.trading_category != 'spot' else 'spot', 
                'symbol': symbol,
                'interval': interval,
                'limit': limit
            }
            if start_time: params['start'] = start_time 
            if end_time: params['end'] = end_time 

            logging.debug(f"Fetching kline data for {symbol} ({interval}) with params: {params}")
            response_data, error = await safe_api_call(self.session.get_kline, **params)
            if error:
                logging.warning(f"Failed to get kline data for {symbol} ({interval}): {error.get('msg', 'Unknown error')}")
                return None
            if response_data and response_data.get('retCode') == 0:
                kline_list = response_data.get('result', {}).get('list', [])
                logging.debug(f"Fetched {len(kline_list)} kline data points for {symbol} ({interval}).")
                return kline_list
            ret_msg = response_data.get('retMsg') if response_data else 'No response data'
            logging.warning(f"Failed to get kline data for {symbol} ({interval}): {ret_msg}")
            return None
        except Exception as e:
            logger.error(f"Exception getting kline data for {symbol} ({interval}): {e}", exc_info=True)
            return None

    async def set_trading_stop(self, symbol: str, 
                               take_profit: Optional[Union[float, str]] = None, 
                               stop_loss: Optional[Union[float, str]] = None, 
                               position_idx: int = 0, 
                               tpsl_mode: str = "Partial", 
                               tp_trigger_by: str = "LastPrice", 
                               sl_trigger_by: str = "LastPrice",
                               tp_order_type: str = "Market", 
                               sl_order_type: str = "Market", 
                               tp_limit_price: Optional[Union[float, str]] = None, 
                               sl_limit_price: Optional[Union[float, str]] = None
                              ) -> bool:
        if not take_profit and not stop_loss:
            # Allow clearing TP/SL by passing "0"
            if str(take_profit) != "0" and str(stop_loss) != "0":
                 logger.warning(f"set_trading_stop for {symbol} called without take_profit or stop_loss (and not clearing with '0').")
                 return False 
        
        params = {
            'category': self.trading_category,
            'symbol': symbol,
            'positionIdx': position_idx, 
        }
        
        if take_profit is not None or stop_loss is not None:
            params['tpslMode'] = tpsl_mode

        if take_profit is not None:
            params['takeProfit'] = str(take_profit) # "0" to cancel TP
            if str(take_profit) != "0": # Only set trigger if actually setting a TP value
                params['tpTriggerBy'] = tp_trigger_by
                # params['tpOrderType'] = tp_order_type # tpOrderType is for conditional orders, not this endpoint
                # if tp_order_type == "Limit" and tp_limit_price is not None:
                #     params['tpLimitPrice'] = str(tp_limit_price)


        if stop_loss is not None:
            params['stopLoss'] = str(stop_loss) # "0" to cancel SL
            if str(stop_loss) != "0": # Only set trigger if actually setting an SL value
                params['slTriggerBy'] = sl_trigger_by
                # params['slOrderType'] = sl_order_type # slOrderType is for conditional orders
                # if sl_order_type == "Limit" and sl_limit_price is not None:
                #     params['slLimitPrice'] = str(sl_limit_price)
        
        try:
            logging.debug(f"Setting trading stop for {symbol} with params: {params}")
            response_data, error = await safe_api_call(self.session.set_trading_stop, **params)

            if error:
                err_msg_lower = str(error.get('msg', '')).lower()
                err_code = error.get('code')

                # Bybit error code 110043: position tpsl identical modify
                # Bybit error code 34036 (old API?): "Set cancel condition is not modified"
                if err_code == 110043 or "tpsl identical modify" in err_msg_lower or "not modified" in err_msg_lower:
                    logger.info(f"TP/SL for {symbol} not modified (already set to this value or no change needed). Params: {params}")
                    return True 
                logger.error(f"Failed to set TP/SL for {symbol}: {error.get('msg', 'Unknown error')} (Code: {err_code}). Params: {params}")
                return False
            
            if response_data and response_data.get('retCode') == 0:
                logging.info(f"Successfully set/modified TP/SL for {symbol}. Response: {response_data.get('result')}")
                return True
            
            ret_msg = response_data.get('retMsg') if response_data else 'No response data'
            ret_code = response_data.get('retCode') if response_data else -1
            
            if ret_code == 110043: # position tpsl identical modify
                 logging.info(f"TP/SL for {symbol} not modified (already set to this value). Bybit Msg: {ret_msg}")
                 return True

            logger.error(f"Failed to set TP/SL for {symbol}: {ret_msg} (Code: {ret_code}). Details: {response_data}. Params: {params}")
            return False
        except Exception as e:
            logger.error(f"Exception setting TP/SL for {symbol}: {e}", exc_info=True)
            return False

    async def get_funding_rate_history(self, symbol: str, limit: int = 10) -> Optional[list]:
        try:
            params = {
                'category': self.trading_category,
                'symbol': symbol,
                'limit': limit
            }
            response_data, error = await safe_api_call(self.session.get_funding_rate_history, **params)
            if error:
                logger.error(f"Failed to get funding rate history: {error.get('msg', 'Unknown error')}")
                return None
            if response_data and response_data.get('retCode') == 0:
                return response_data.get('result', {}).get('list', [])
            return None
        except Exception as e:
            logger.error(f"Exception in get_funding_rate_history: {e}", exc_info=True)
            return None

    async def close(self):
        logging.info("BybitTrader close called. No explicit async resources to release for pybit HTTP session, but good practice for future.")
        pass

