import logging
import asyncio
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN, ROUND_UP, InvalidOperation
from typing import Optional, Dict, List, Any, Tuple, Union
import configparser
import pytz
import json # Import json for state saving
import os   # Import os for path manipulation

from bybit_trader import BybitTrader 
from telegram_bot import TelegramBot
from signal_parser import TradingSignal
# เพิ่ม import สำหรับ ui_status_log
try:
    from main import ui_status_log
except ImportError:
    def ui_status_log(msg):
        pass

logger = logging.getLogger(__name__)
THAILAND_TZ = pytz.timezone('Asia/Bangkok')

MIN_ORDER_QTY_MAP = {
    "BTCUSDT": Decimal('0.001'),
    "ETHUSDT": Decimal('0.01'),
}
DEFAULT_MIN_ORDER_QTY = Decimal('0.001')

TPSL_VERIFICATION_DELAY_SECONDS = 5
TPSL_VERIFICATION_RETRIES = 3
TPSL_RETRY_DELAY_SECONDS = 10
TPSL_PERIODIC_CHECK_INTERVAL_SECONDS = 30 
POSITION_CLOSE_CHECK_INTERVAL_SECONDS = 15 

# --- State Saving Constants ---
STATE_DIR = "state" # Directory to store state files
ACTIVE_POSITIONS_STATE_FILE = os.path.join(STATE_DIR, "active_positions_state.json")
TRADE_HISTORY_FILE = os.path.join(STATE_DIR, "trade_history.json")

class BaseTrader:
    def __init__(self, config, exchange_name):
        self.exchange_name = exchange_name
        self.config = config
        # ... existing code ...
    # ... ฟังก์ชันหลักๆ เดิม ...

class TradingBot:
    def __init__(self, config: configparser.ConfigParser, telegram_code_callback=None, ui_master=None, app_instance=None):
        self.config = config
        self._ui_master = ui_master
        self._app_instance = app_instance

        self.bybit_trader = BybitTrader(self.config)
        self.telegram_bot = TelegramBot(
            self.config,
            self.bybit_trader,
            code_request_callback=telegram_code_callback,
            app_instance=app_instance,
            trading_bot_instance=self
        )

        self.current_balance: float = 0.0
        self.current_available_balance: float = 0.0
        self.current_margin: float = 0.0
        self.total_orders_count: int = 0
        self.total_positions_count: int = 0
        self.current_open_orders: List[Dict] = []
        self.current_open_positions: List[Dict] = []
        self.instrument_info: Dict[str, Dict[str, Any]] = {}

        self.bybit_connected: bool = False
        self.telegram_connected: bool = False

        self.running: bool = False
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.start_time: Optional[datetime] = None

        self._periodic_tasks: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        self.is_license_valid: bool = True
        self.license_days_remaining: int = 365

        self.default_qty_precision = self.config.getint('BYBIT', 'default_qty_precision', fallback=3)
        self.default_price_precision = self.config.getint('BYBIT', 'default_price_precision', fallback=1)
        self.position_size_mode = self.config.get('BYBIT', 'position_size_mode', fallback='fixed').lower()
        self.risk_per_trade_percentage = self.config.getfloat('BYBIT', 'risk_per_trade_percentage', fallback=1.0)
        self.enable_breakeven_on_tp1 = self.config.getboolean('BYBIT', 'enable_breakeven_on_tp1', fallback=False)
        self.cancel_orders_on_new_signal = self.config.getboolean('BYBIT', 'cancel_orders_on_new_signal', fallback=True)
        self.closed_pnl_fetch_limit = self.config.getint('BYBIT', 'closed_pnl_fetch_limit', fallback=5)
        self.enable_breakeven_on_partial_tp = True


        self.target_trading_symbol = self.config.get('BYBIT', 'default_symbol', fallback='BTCUSDT').upper()
        logger.info(f"TradingBot initialized. Will exclusively trade: {self.target_trading_symbol}")
        logger.info(f"Position size mode: {self.position_size_mode}")
        if self.position_size_mode == 'risk_percentage':
            logger.info(f"Risk per trade: {self.risk_per_trade_percentage}%")
        logger.info(f"Break-even on TP1 enabled: {self.enable_breakeven_on_tp1}")
        logger.info(f"Cancel existing orders on new signal for same symbol: {self.cancel_orders_on_new_signal}")

        # Initialize state variables (will be loaded from disk if files exist)
        self.trade_history: List[Dict[str, Any]] = []
        self.active_positions_details: Dict[Tuple[str, int], Dict[str, Any]] = {}
        
        self._ensure_state_dir_exists() # Create state directory if it doesn't exist
        self._load_trade_history_from_disk()
        self._load_active_positions_from_disk()

        # Structure of active_positions_details[(symbol, pos_idx)]:
        # {
        #     'symbol': str,
        #     'position_idx': int,
        #     'side': str ("BUY" or "SELL"),
        #     'entry_price': Optional[Decimal], 
        #     'signal_entry_price': Optional[Decimal], 
        #     'intended_sl': Optional[Decimal],
        #     'intended_tp1': Optional[Decimal], 
        #     'breakeven_applied': bool,
        #     'main_order_id': Optional[str],
        #     'main_order_status': Optional[str], 
        #     'tp_order_ids': List[str], 
        #     'last_update_time': datetime, # Will be stored as ISO string
        #     'last_known_size': Optional[Decimal] 
        # }

    # --- State Saving and Loading Methods ---
    def _ensure_state_dir_exists(self):
        """Ensures the state directory exists."""
        if not os.path.exists(STATE_DIR):
            try:
                os.makedirs(STATE_DIR)
                logger.info(f"Created state directory: {STATE_DIR}")
            except OSError as e:
                logger.error(f"Could not create state directory {STATE_DIR}: {e}", exc_info=True)

    def _serialize_value(self, value: Any) -> Any:
        """Converts Decimal to str and datetime to ISO str for JSON serialization."""
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, list):
            return [self._serialize_value(item) for item in value]
        if isinstance(value, dict):
            return {k: self._serialize_value(v) for k, v in value.items()}
        return value

    def _deserialize_value(self, value: Any, target_type: Optional[type] = None) -> Any:
        """
        Converts str back to Decimal or datetime based on context or target_type.
        This is a simplified version; more robust deserialization might need schema/hints.
        """
        if isinstance(value, str):
            try: # Attempt to parse as Decimal
                return Decimal(value)
            except InvalidOperation:
                try: # Attempt to parse as datetime
                    return datetime.fromisoformat(value.replace("Z", "+00:00")) # Handle Z for UTC
                except (ValueError, TypeError):
                    pass # Not a Decimal or datetime string
        if isinstance(value, list): # Recursively deserialize list items
            return [self._deserialize_value(item) for item in value]
        if isinstance(value, dict): # Recursively deserialize dict values
            return {k: self._deserialize_value(v) for k, v in value.items()}
        return value
        
    def _save_active_positions_to_disk(self):
        """Saves the active_positions_details to a JSON file."""
        if not self.active_positions_details:
            logger.debug("No active positions to save.")
            if os.path.exists(ACTIVE_POSITIONS_STATE_FILE):
                try:
                    os.remove(ACTIVE_POSITIONS_STATE_FILE)
                    logger.info(f"Removed active positions state file as it's now empty: {ACTIVE_POSITIONS_STATE_FILE}")
                except OSError as e:
                    logger.error(f"Error removing empty state file {ACTIVE_POSITIONS_STATE_FILE}: {e}")
            return

        logger.info(f"Saving active positions to {ACTIVE_POSITIONS_STATE_FILE}...")
        data_to_save = {}
        for (symbol, pos_idx), details in self.active_positions_details.items():
            key_str = f"{symbol}_{pos_idx}"
            data_to_save[key_str] = self._serialize_value(details)
        
        try:
            with open(ACTIVE_POSITIONS_STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, indent=4)
            logger.info(f"Successfully saved {len(data_to_save)} active position(s) to disk.")
        except IOError as e:
            logger.error(f"Failed to save active positions state: {e}", exc_info=True)
        except TypeError as e:
            logger.error(f"TypeError during active positions serialization: {e}. Data: {data_to_save}", exc_info=True)


    def _load_active_positions_from_disk(self):
        """Loads the active_positions_details from a JSON file."""
        if not os.path.exists(ACTIVE_POSITIONS_STATE_FILE):
            logger.info("No active positions state file found. Starting with empty details.")
            self.active_positions_details = {}
            return

        logger.info(f"Loading active positions from {ACTIVE_POSITIONS_STATE_FILE}...")
        try:
            with open(ACTIVE_POSITIONS_STATE_FILE, 'r', encoding='utf-8') as f:
                loaded_data_str_keys = json.load(f)
            
            self.active_positions_details = {}
            for key_str, details_json in loaded_data_str_keys.items():
                try:
                    symbol, pos_idx_str = key_str.rsplit('_', 1)
                    pos_idx = int(pos_idx_str)
                    
                    deserialized_details = {}
                    for field_key, field_value in details_json.items():
                        if field_key in ['entry_price', 'signal_entry_price', 'intended_sl', 'intended_tp1', 'last_known_size', 'tp1_price']: # Added tp1_price
                            deserialized_details[field_key] = Decimal(str(field_value)) if field_value is not None else None
                        elif field_key == 'last_update_time' and field_value:
                            deserialized_details[field_key] = datetime.fromisoformat(str(field_value)) if field_value else None
                        elif field_key == 'tp_order_ids' and isinstance(field_value, list):
                             deserialized_details[field_key] = [str(item) for item in field_value] 
                        else:
                            deserialized_details[field_key] = field_value 

                    self.active_positions_details[(symbol, pos_idx)] = deserialized_details
                except ValueError as ve:
                    logger.error(f"Error parsing key '{key_str}' or pos_idx from state file: {ve}")
                except Exception as e_detail:
                    logger.error(f"Error deserializing details for key '{key_str}': {e_detail}. Details: {details_json}", exc_info=True)

            logger.info(f"Successfully loaded {len(self.active_positions_details)} active position(s) from disk.")
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Failed to load or parse active positions state file {ACTIVE_POSITIONS_STATE_FILE}: {e}", exc_info=True)
            self.active_positions_details = {} 
        except Exception as e_outer:
            logger.error(f"Unexpected error loading active positions state: {e_outer}", exc_info=True)
            self.active_positions_details = {}


    def _save_trade_history_to_disk(self):
        """Saves the trade_history to a JSON file."""
        if not self.trade_history:
            logger.debug("No trade history to save.")
            return

        logger.info(f"Saving trade history to {TRADE_HISTORY_FILE}...")
        data_to_save = [self._serialize_value(item) for item in self.trade_history]
        try:
            with open(TRADE_HISTORY_FILE, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, indent=4)
            logger.info(f"Successfully saved {len(data_to_save)} trade history entries to disk.")
        except IOError as e:
            logger.error(f"Failed to save trade history: {e}", exc_info=True)
        except TypeError as e:
            logger.error(f"TypeError during trade history serialization: {e}. Data sample: {data_to_save[:2] if data_to_save else 'N/A'}", exc_info=True)


    def _load_trade_history_from_disk(self):
        """Loads the trade_history from a JSON file."""
        if not os.path.exists(TRADE_HISTORY_FILE):
            logger.info("No trade history file found. Starting with empty history.")
            self.trade_history = []
            return

        logger.info(f"Loading trade history from {TRADE_HISTORY_FILE}...")
        try:
            with open(TRADE_HISTORY_FILE, 'r', encoding='utf-8') as f:
                loaded_history = json.load(f)
            self.trade_history = [self._deserialize_value(item) for item in loaded_history]
            logger.info(f"Successfully loaded {len(self.trade_history)} trade history entries from disk.")
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Failed to load or parse trade history file {TRADE_HISTORY_FILE}: {e}", exc_info=True)
            self.trade_history = [] 
        except Exception as e_outer:
            logger.error(f"Unexpected error loading trade history: {e_outer}", exc_info=True)
            self.trade_history = []

    # --- End of State Saving and Loading Methods ---

    def _add_trade_to_history(self, symbol: str, order_type: str, side: str, qty: str, price: Optional[str], order_id: Optional[str], status: str, pnl: Optional[str] = "0.00", notes: Optional[str] = None):
        now_utc = datetime.utcnow()
        now_thailand = now_utc.astimezone(THAILAND_TZ)
        trade_event = {
            "time": now_thailand.strftime("%Y-%m-%d %H:%M:%S"), 
            "symbol": symbol,
            "type": f"{side.capitalize()} {order_type.capitalize()}",
            "amount": qty,
            "price": price if price else "Market",
            "order_id": order_id if order_id else "N/A",
            "status": status,
            "pnl": pnl,
            "notes": notes if notes else ""
        }
        self.trade_history.append(trade_event)
        logger.info(f"Trade history updated: {trade_event}")
        self._save_trade_history_to_disk() 
        if self._app_instance and hasattr(self._app_instance, '_update_orders_positions_table'):
            if self._app_instance.master and hasattr(self._app_instance.master, 'winfo_exists') and self._app_instance.master.winfo_exists():
                 self._app_instance.master.after(0, self._app_instance._update_orders_positions_table)

    def get_formatted_trade_history(self) -> List[Tuple[str, str, str, str, str, str]]:
        logger.debug(f"get_formatted_trade_history: Current self.trade_history length: {len(self.trade_history)}. List ID: {id(self.trade_history)}")
        formatted_history = []
        history_to_iterate = list(self.trade_history) 
        logger.debug(f"get_formatted_trade_history: Copied list length: {len(history_to_iterate)}")
        for trade in reversed(history_to_iterate): 
            result_display = f"{trade['status']}"
            if trade.get('order_id') and trade['order_id'] != "N/A":
                result_display += f" (ID: ...{str(trade['order_id'])[-6:]})"
            if trade.get('price') and trade['price'] != "Market":
                 result_display += f" @{trade['price']}"
            if trade.get('notes'): 
                result_display += f" ({trade['notes']})"
            
            formatted_history.append((
                str(trade.get("time", "N/A")), 
                str(trade.get("symbol", "N/A")), 
                str(trade.get("type", "N/A")),
                str(trade.get("amount", "N/A")), 
                result_display, 
                str(trade.get("pnl", "N/A"))
            ))
        logger.debug(f"get_formatted_trade_history: Returning {len(formatted_history)} formatted items.")
        return formatted_history[:50] 

    def _get_instrument_detail(self, symbol: str, detail_key: str, fallback: Any = None) -> Any:
        return self.instrument_info.get(symbol, {}).get(detail_key, fallback)

    def _format_quantity(self, quantity: Union[float, Decimal, str], symbol: str) -> str:
        try:
            num_quantity = Decimal(str(quantity))
            qty_step_str = self._get_instrument_detail(symbol, 'qtyStep')
            if qty_step_str:
                qty_step = Decimal(qty_step_str)
                if qty_step > Decimal(0):
                    formatted_qty_decimal = (num_quantity / qty_step).quantize(Decimal('1'), rounding=ROUND_DOWN) * qty_step
                    return str(formatted_qty_decimal.quantize(qty_step, rounding=ROUND_DOWN))
            precision = self.config.getint('BYBIT', f"{symbol.upper()}_qty_precision", fallback=self.default_qty_precision)
            quantizer = Decimal('1e-' + str(precision))
            return str(num_quantity.quantize(quantizer, rounding=ROUND_DOWN))
        except (InvalidOperation, TypeError, ValueError) as e:
            logger.error(f"Invalid quantity value for formatting: {quantity} for symbol {symbol}. Error: {e}.")
            precision = self.config.getint('BYBIT', f"{symbol.upper()}_qty_precision", fallback=self.default_qty_precision)
            try:
                return f"{float(quantity):.{precision}f}"
            except: return str(quantity) 

    def _format_price(self, price: Union[float, Decimal, str], symbol: str) -> str:
        try:
            num_price = Decimal(str(price))
            price_step_str = self._get_instrument_detail(symbol, 'tickSize') 
            if price_step_str:
                price_step = Decimal(price_step_str)
                if price_step > Decimal(0):
                    formatted_price_decimal = (num_price / price_step).quantize(Decimal('1'), rounding=ROUND_DOWN) * price_step
                    return str(formatted_price_decimal.quantize(price_step, rounding=ROUND_DOWN)) 
            precision = self.config.getint('BYBIT', f"{symbol.upper()}_price_precision", fallback=self.default_price_precision)
            quantizer = Decimal('1e-' + str(precision))
            return str(num_price.quantize(quantizer, rounding=ROUND_DOWN))
        except (InvalidOperation, TypeError, ValueError) as e:
            logger.error(f"Invalid price value for formatting: {price} for symbol {symbol}. Error: {e}.")
            precision = self.config.getint('BYBIT', f"{symbol.upper()}_price_precision", fallback=self.default_price_precision)
            try:
                return f"{float(price):.{precision}f}"
            except: return str(price)

    async def _fetch_and_store_instrument_info(self, symbol: str):
        if symbol in self.instrument_info and self.instrument_info[symbol].get('tickSize') and self.instrument_info[symbol].get('qtyStep'):
            logger.debug(f"Instrument info for {symbol} already exists and seems complete. Skipping fetch.")
            return
        try:
            logger.info(f"Fetching instrument info for {symbol}...")
            info_list = await self.bybit_trader.get_instruments_info(symbol=symbol, category=self.bybit_trader.trading_category) 
            if info_list and isinstance(info_list, list) and len(info_list) > 0:
                instrument = info_list[0] 
                self.instrument_info[symbol] = {
                    'minOrderQty': instrument.get('lotSizeFilter', {}).get('minOrderQty'),
                    'qtyStep': instrument.get('lotSizeFilter', {}).get('qtyStep'),
                    'tickSize': instrument.get('priceFilter', {}).get('tickSize'),
                }
                logger.info(f"Stored instrument info for {symbol}: {self.instrument_info[symbol]}")
            else: 
                logger.warning(f"Could not fetch or parse instrument info for {symbol}. Response: {info_list}")
                self.instrument_info[symbol] = {
                    'minOrderQty': self.config.get('BYBIT', f"{symbol.upper()}_min_order_qty", fallback=str(DEFAULT_MIN_ORDER_QTY)),
                    'qtyStep': self.config.get('BYBIT', f"{symbol.upper()}_qty_step", fallback=str(Decimal('1e-' + str(self.default_qty_precision)))),
                    'tickSize': self.config.get('BYBIT', f"{symbol.upper()}_price_step", fallback=str(Decimal('1e-' + str(self.default_price_precision)))),
                }
                logger.info(f"Using fallback instrument info for {symbol} from config: {self.instrument_info[symbol]}")
        except Exception as e: 
            logger.error(f"Error fetching instrument info for {symbol}: {e}", exc_info=True)
            if symbol not in self.instrument_info:
                 self.instrument_info[symbol] = {
                    'minOrderQty': self.config.get('BYBIT', f"{symbol.upper()}_min_order_qty", fallback=str(DEFAULT_MIN_ORDER_QTY)),
                    'qtyStep': self.config.get('BYBIT', f"{symbol.upper()}_qty_step", fallback=str(Decimal('1e-' + str(self.default_qty_precision)))),
                    'tickSize': self.config.get('BYBIT', f"{symbol.upper()}_price_step", fallback=str(Decimal('1e-' + str(self.default_price_precision)))),
                }
                 logger.info(f"Using fallback instrument info for {symbol} due to exception: {self.instrument_info[symbol]}")


    async def _check_license(self) -> bool: 
        self.is_license_valid = True 
        self.license_days_remaining = 30 
        if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists():
            self._app_instance.master.after(0, lambda: self._app_instance.update_license_info("Valid (Demo)", "30")) 
        return True

    async def _get_closed_pnl_for_symbol(self, symbol: str, limit: int = 1) -> List[Dict]:
        if not self.bybit_trader or not self.bybit_connected:
            return []
        try:
            if hasattr(self.bybit_trader.session, 'get_closed_pnl'): 
                response_data, error = await safe_api_call(
                    self.bybit_trader.session.get_closed_pnl,
                    category=self.bybit_trader.trading_category, 
                    symbol=symbol,
                    limit=limit
                )
                if error:
                    logger.error(f"API Error fetching closed PnL for {symbol}: {error.get('msg')}")
                    return []
                if response_data and response_data.get('retCode') == 0:
                    closed_pnl_list = response_data.get('result', {}).get('list', [])
                    logger.debug(f"Fetched {len(closed_pnl_list)} closed PnL entries for {symbol}.")
                    return closed_pnl_list
                logger.warning(f"Could not fetch or parse closed PnL for {symbol}. Response: {response_data}")
            else:
                logger.warning(f"BybitTrader's session does not have get_closed_pnl method. Cannot fetch PnL for {symbol}.")
        except Exception as e:
            logger.error(f"Error fetching closed PnL for {symbol}: {e}", exc_info=True)
        return []

    async def _handle_closed_position(self, pos_key: Tuple[str, int], closed_position_data: Optional[Dict] = None, reason_override: Optional[str] = None):
        symbol, position_idx = pos_key
        
        tracked_info = self.active_positions_details.pop(pos_key, None) 
        if not tracked_info:
            logger.warning(f"No tracking information found for closed position {symbol} (PosIdx: {position_idx}) when trying to handle closure.")
            self._save_active_positions_to_disk() 
            return

        logger.info(f"Handling closed position for {symbol} (PosIdx: {position_idx}). Tracked info: {tracked_info}")
        self._save_active_positions_to_disk() 

        reason_note = reason_override or "Position Closed (Reason Undetermined)"
        pnl_value = "N/A" 
        exit_price_str = "N/A" 
        
        pos_side = tracked_info.get('side', 'N/A')
        qty_closed = closed_position_data.get('size', tracked_info.get('last_known_size', 'N/A')) if closed_position_data else tracked_info.get('last_known_size', 'N/A')

        logger.info(f"Position {symbol} (PosIdx: {position_idx}) determined closed. Reason: {reason_note}. PnL: {pnl_value}, Exit: {exit_price_str}")
        self._add_trade_to_history(symbol, "System Close", pos_side, str(qty_closed),
                                   exit_price_str, None, f"Position Closed ({reason_note})", pnl_value,
                                   notes=f"PosIdx {position_idx}")

        if self.telegram_bot and hasattr(self.telegram_bot, 'send_message_async'):
            try:
                message = (f"🔔 Position Closed: {symbol} ({pos_side}, Idx: {position_idx})\n"
                           f"Reason: {reason_note}\n"
                           f"PnL: {pnl_value} USDT (approx.)") 
                await self.telegram_bot.send_message_async(message)
            except Exception as e_tg_send:
                logger.error(f"Failed to send Telegram notification for closed position {symbol}: {e_tg_send}")
        
        if pos_key in self.active_positions_details: 
            del self.active_positions_details[pos_key]
            logger.info(f"Re-confirmed removal of details for closed position: {symbol} (PosIdx: {position_idx})")
            self._save_active_positions_to_disk() 

        # --- หลังปิดออเดอร์ เรียกอัปเดต UI ---
        if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists():
            self._app_instance.master.after(0, self._app_instance.update_trading_info_ui)


    async def update_initial_trading_data(self):
        logger.info("TradingBot: Updating trading data (with improved reconciliation)...")
        if not self.bybit_trader:
            logger.error("TradingBot: BybitTrader not initialized."); self.bybit_connected = False
            if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists():
                 self._app_instance.master.after(0, lambda: self._app_instance.update_bybit_status_ui(False))
            return

        self.bybit_connected = await self.bybit_trader.test_connection()
        if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists():
            self._app_instance.master.after(0, lambda: self._app_instance.update_bybit_status_ui(self.bybit_connected))

        if self.bybit_connected:
            if not self.instrument_info.get(self.target_trading_symbol): 
                await self._fetch_and_store_instrument_info(self.target_trading_symbol)

            balance_task = self.bybit_trader.get_wallet_balance(self.bybit_trader.account_type, self.bybit_trader.default_coin)
            orders_task = self.bybit_trader.get_open_orders(settleCoin=self.bybit_trader.default_coin) # Fetch all open orders for the settle coin
            positions_task = self.bybit_trader.get_open_positions(settleCoin=self.bybit_trader.default_coin) # Fetch all open positions

            results = await asyncio.gather(balance_task, orders_task, positions_task, return_exceptions=True)

            balance_data = results[0] if not isinstance(results[0], Exception) else None
            self.current_open_orders = results[1] if not isinstance(results[1], Exception) else [] # Store all open orders
            new_positions_list_from_api = results[2] if not isinstance(results[2], Exception) else [] # Store all open positions
            
            if isinstance(results[0], Exception): logger.error(f"Error fetching balance: {results[0]}")
            if isinstance(results[1], Exception): logger.error(f"Error fetching orders: {results[1]}")
            if isinstance(results[2], Exception): logger.error(f"Error fetching positions: {results[2]}")

            if balance_data:
                self.current_balance = float(balance_data.get('total_balance', 0.0))
                self.current_available_balance = float(balance_data.get('available_balance', 0.0))
                self.current_margin = float(balance_data.get('used_margin', 0.0))
            else: self.current_balance = self.current_available_balance = self.current_margin = 0.0
            logger.info(f"TradingBot: Balance - Total: {self.current_balance:.2f}, Avail: {self.current_available_balance:.2f}, Margin: {self.current_margin:.2f}")

            self.total_orders_count = len(self.current_open_orders)
            
            # --- Improved Reconciliation Logic ---
            previous_tracked_pos_keys = set(self.active_positions_details.keys())
            current_api_pos_keys = set() # Positions currently active on Bybit

            # 1. Update status of tracked main orders and sync existing positions from API
            for pos_key_tracked, details_tracked in list(self.active_positions_details.items()):
                symbol_tracked, pos_idx_tracked = pos_key_tracked
                main_order_id = details_tracked.get('main_order_id')
                current_tracked_status = details_tracked.get('main_order_status')

                # Check if this tracked position is active on the API
                pos_on_api = next((p for p in new_positions_list_from_api if p.get('symbol') == symbol_tracked and p.get('positionIdx') == pos_idx_tracked and float(p.get('size','0')) > 0), None)

                if pos_on_api: # Position is active on API
                    current_api_pos_keys.add(pos_key_tracked) # Mark as active on API
                    live_avg_price_str = pos_on_api.get('avgPrice')
                    live_avg_price = Decimal(live_avg_price_str) if live_avg_price_str and live_avg_price_str != "0" else None
                    live_side = pos_on_api.get('side','').upper()
                    live_size_str = pos_on_api.get('size', '0')
                    live_size = Decimal(live_size_str) if live_size_str else Decimal(0)
                    live_sl = Decimal(pos_on_api.get('stopLoss', '0')) if pos_on_api.get('stopLoss') and pos_on_api.get('stopLoss') != "0" else None
                    live_tp = Decimal(pos_on_api.get('takeProfit', '0')) if pos_on_api.get('takeProfit') and pos_on_api.get('takeProfit') != "0" else None

                    if details_tracked.get('entry_price') != live_avg_price and live_avg_price:
                        logger.info(f"Updating entry price for {symbol_tracked} (Idx {pos_idx_tracked}) from {details_tracked.get('entry_price')} to {live_avg_price}")
                        details_tracked['entry_price'] = live_avg_price
                    if details_tracked.get('side') != live_side and live_side:
                        logger.warning(f"Side mismatch for {symbol_tracked} (Idx {pos_idx_tracked}): Tracked={details_tracked.get('side')}, API={live_side}. Updating.")
                        details_tracked['side'] = live_side
                    if details_tracked.get('last_known_size') != live_size:
                         logger.info(f"Updating size for {symbol_tracked} (Idx {pos_idx_tracked}) from {details_tracked.get('last_known_size')} to {live_size}")
                         details_tracked['last_known_size'] = live_size
                    
                    # Sync SL/TP if they were not intentionally set by bot or if BE was applied
                    if details_tracked.get('intended_sl') is None or details_tracked.get('breakeven_applied'):
                        if details_tracked.get('intended_sl') != live_sl:
                             logger.info(f"Syncing SL for {symbol_tracked} (Idx {pos_idx_tracked}) from exchange: {live_sl} (was {details_tracked.get('intended_sl')})")
                             details_tracked['intended_sl'] = live_sl
                    if details_tracked.get('intended_tp1') is None:
                         if details_tracked.get('intended_tp1') != live_tp:
                             logger.info(f"Syncing TP1 for {symbol_tracked} (Idx {pos_idx_tracked}) from exchange: {live_tp} (was {details_tracked.get('intended_tp1')})")
                             details_tracked['intended_tp1'] = live_tp

                    if current_tracked_status != 'Filled': # If position is active, main order must have filled
                        logger.info(f"Confirming main order status as 'Filled' for {symbol_tracked} (Idx {pos_idx_tracked}) due to active position on API.")
                        details_tracked['main_order_status'] = 'Filled'
                    details_tracked['last_update_time'] = datetime.utcnow()

                else: # Tracked position is NOT active on API
                    if main_order_id and current_tracked_status not in ['Filled', 'Cancelled', 'Rejected', 'Deactivated']:
                        # Order was pending, check if the order itself is still open
                        order_on_exchange = next((o for o in self.current_open_orders if o.get('orderId') == main_order_id), None)
                        if order_on_exchange:
                            new_status_from_api = order_on_exchange.get('orderStatus')
                            if current_tracked_status != new_status_from_api:
                                logger.info(f"Tracked position {symbol_tracked} (Idx {pos_idx_tracked}) not active on API, but main order {main_order_id} is still '{new_status_from_api}'. Updating status.")
                                details_tracked['main_order_status'] = new_status_from_api
                                details_tracked['last_update_time'] = datetime.utcnow()
                            # If order is still open (e.g. New, PartiallyFilled), we keep tracking it. It's not "closed" yet.
                        else:
                            # Order was pending, no active position, and order is no longer in open orders.
                            # This implies the entry order failed (Cancelled/Rejected by exchange or user).
                            logger.info(f"Tracked position {symbol_tracked} (Idx {pos_idx_tracked}) not active on API, and main order {main_order_id} (was: {current_tracked_status}) no longer open. Handling as entry failed/cancelled.")
                            await self._handle_closed_position(pos_key_tracked, reason_override="Entry Order Failed/Cancelled")
                    
                    elif current_tracked_status == 'Filled':
                        # Position was 'Filled' but now absent from API's active positions. This means it's genuinely closed.
                        logger.info(f"Position {symbol_tracked} (Idx {pos_idx_tracked}) was 'Filled' but no longer in API's open positions. Handling as closed on exchange.")
                        await self._handle_closed_position(pos_key_tracked, reason_override="Closed on Exchange (was Filled)")
                    
                    else: # e.g. status was already Cancelled/Rejected, or other unhandled states
                        logger.info(f"Tracked details for {symbol_tracked} (Idx {pos_idx_tracked}) (status: {current_tracked_status}) indicate it's not an active API position. Finalizing cleanup if still tracked.")
                        if pos_key_tracked in self.active_positions_details: # Check if not already removed by a call above
                            await self._handle_closed_position(pos_key_tracked, reason_override=f"Cleanup (Status: {current_tracked_status})")


            # 2. Add any new positions found on API that were not previously tracked (e.g. external positions)
            for pos_data_api in new_positions_list_from_api:
                symbol_api = pos_data_api.get('symbol')
                pos_idx_api = pos_data_api.get('positionIdx', 0)
                if not symbol_api or float(pos_data_api.get('size','0')) <= 0: continue # Skip if no symbol or zero size

                api_pos_key = (symbol_api, pos_idx_api)
                if api_pos_key not in self.active_positions_details: # New, untracked position found on API
                    current_api_pos_keys.add(api_pos_key) # Mark as active
                    live_avg_price_str = pos_data_api.get('avgPrice')
                    live_avg_price = Decimal(live_avg_price_str) if live_avg_price_str and live_avg_price_str != "0" else None
                    live_side = pos_data_api.get('side','').upper()
                    live_size_str = pos_data_api.get('size', '0')
                    live_size = Decimal(live_size_str) if live_size_str else Decimal(0)
                    live_sl = Decimal(pos_data_api.get('stopLoss', '0')) if pos_data_api.get('stopLoss') and pos_data_api.get('stopLoss') != "0" else None
                    live_tp = Decimal(pos_data_api.get('takeProfit', '0')) if pos_data_api.get('takeProfit') and pos_data_api.get('takeProfit') != "0" else None
                    
                    logger.warning(f"Untracked active position found on exchange: {symbol_api} (Idx {pos_idx_api}, Size {live_size}). Syncing.")
                    self.active_positions_details[api_pos_key] = {
                        'symbol': symbol_api, 'position_idx': pos_idx_api, 'side': live_side,
                        'entry_price': live_avg_price, 'last_known_size': live_size,
                        'signal_entry_price': None, 
                        'intended_sl': live_sl, 
                        'intended_tp1': live_tp, 
                        'tp1_price': live_tp, # Assuming exchange TP is TP1 for external
                        'breakeven_applied': False, 
                        'main_order_id': None, 'main_order_status': 'Filled (External/Synced)', # Assume filled if active
                        'tp_order_ids': [], 'last_update_time': datetime.utcnow()
                    }
            
            self.current_open_positions = new_positions_list_from_api # Update bot's view of current positions
            self.total_positions_count = len([p for p in self.current_open_positions if float(p.get('size','0')) > 0]) # Count only active ones

            if previous_tracked_pos_keys != set(self.active_positions_details.keys()):
                self._save_active_positions_to_disk()
            # --- End of Improved Reconciliation Logic ---

        else: 
            self.current_balance = self.current_available_balance = self.current_margin = 0.0
            self.total_orders_count = self.total_positions_count = 0
            logger.warning("Bybit not connected. Cannot update live trading data. Retaining last known state for active positions.")

        if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists():
            self._app_instance.master.after(0, self._app_instance.update_trading_info_ui)


    async def start(self):
        if self.running: logger.warning("TradingBot is already running."); return
        self._shutdown_event.clear(); self.running = True; self.start_time = datetime.now()
        if self.loop is None or self.loop.is_closed(): self.loop = asyncio.get_running_loop()
        logger.info("TradingBot starting sequence...")

        if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists():
            self._app_instance.master.after(0, lambda: self._app_instance.update_status_bar("Bot Starting...", "blue"))
            self._app_instance.master.after(0, lambda: self._app_instance.update_license_info("Validating...", "-"))
        if not await self._check_license(): 
            logger.critical("License invalid. Bot will not start."); self.running = False
            if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists(): self._app_instance.master.after(0, lambda: self._app_instance.update_status_bar("Bot Stopped: License Invalid", "red"))
            return
        
        await self.update_initial_trading_data() 
        
        if not self.bybit_connected:
            logger.error("Bybit connection failed. Bot will not start."); self.running = False
            if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists(): self._app_instance.master.after(0, lambda: self._app_instance.update_status_bar("Bot Stopped: Bybit Connection Failed", "red"))
            return
        
        if self.telegram_bot:
            logger.info("TradingBot: Starting Telegram bot component..."); tg_task = self.loop.create_task(self.telegram_bot.run()); self._periodic_tasks.append(tg_task)
        else: logger.warning("TradingBot: TelegramBot not initialized.")
        
        self._periodic_tasks.append(self.loop.create_task(self._periodic_fetch_bybit_metrics()))
        self._periodic_tasks.append(self.loop.create_task(self._periodic_update_runtime()))
        self._periodic_tasks.append(self.loop.create_task(self._periodic_verify_and_maintain_tpsl()))
        
        if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists(): self._app_instance.master.after(0, lambda: self._app_instance.update_status_bar("Bot Running", "green"))
        logger.info("TradingBot main loop starting...")
        try:
            while self.running and not self._shutdown_event.is_set(): await asyncio.sleep(1)
        except asyncio.CancelledError: logger.info("TradingBot main loop cancelled.")
        except Exception as e: logger.error(f"TradingBot main loop error: {e}", exc_info=True)
        finally: 
            logger.info("TradingBot main loop finishing.")
            await self._cleanup_tasks() 
            self._save_active_positions_to_disk() 
            self._save_trade_history_to_disk()   
            self.running = False
            logger.info("TradingBot successfully set running=False in main loop finally.")


    async def _periodic_fetch_bybit_metrics(self):
        interval = self.config.getint('BYBIT', 'balance_check_interval_seconds', fallback=POSITION_CLOSE_CHECK_INTERVAL_SECONDS) 
        try:
            while self.running and not self._shutdown_event.is_set():
                logger.debug("TradingBot: Periodic metrics fetch starting.")
                await self.update_initial_trading_data() 
                await asyncio.sleep(interval)
        except asyncio.CancelledError: logger.info("Periodic metrics task cancelled.")
        except Exception as e: logger.error(f"Error in periodic metrics fetch: {e}", exc_info=True)

    async def _periodic_update_runtime(self):
        try:
            while self.running and self.start_time and not self._shutdown_event.is_set():
                if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists(): self._app_instance.master.after(0, lambda: self._app_instance.update_runtime_ui(self.get_runtime_str()))
                await asyncio.sleep(1)
        except asyncio.CancelledError: logger.info("Runtime update task cancelled.")
        except Exception as e: logger.error(f"Error in runtime update task: {e}", exc_info=True)

    async def _verify_and_set_tpsl_for_position(self, symbol: str, intended_tp: Optional[Decimal], intended_sl: Optional[Decimal], position_idx: int) -> bool:
        logger.info(f"Verifying TP/SL for {symbol} (PosIdx: {position_idx}). Intended TP: {intended_tp}, SL: {intended_sl}")
        pos_key = (symbol, position_idx)
        
        for attempt in range(TPSL_VERIFICATION_RETRIES + 1): 
            tracked_details = self.active_positions_details.get(pos_key)
            if not tracked_details:
                logger.warning(f"Position {symbol} (PosIdx {position_idx}) no longer tracked. Skipping TP/SL verification.")
                return True 

            try:
                current_positions_on_exchange = await self.bybit_trader.get_open_positions(symbol=symbol, settleCoin=self.bybit_trader.default_coin)
                target_position_data = next((p for p in current_positions_on_exchange if p.get('symbol') == symbol and p.get('positionIdx') == position_idx), None)
                
                if not target_position_data or float(target_position_data.get('size', '0')) == 0:
                    main_order_status = tracked_details.get('main_order_status')
                    if main_order_status in ['New', 'Submitted', 'PartiallyFilled'] and not tracked_details.get('entry_price'):
                        logger.info(f"Entry order for {symbol} (PosIdx: {position_idx}) is still '{main_order_status}' and no position formed. TP/SL verification skipped.")
                        return True 
                    else: 
                        logger.info(f"Position {symbol} (PosIdx: {position_idx}) no longer found or size is zero during TP/SL set. Handling as closed.")
                        # Check if still tracked before handling close, to avoid race condition if another task already handled it
                        if pos_key in self.active_positions_details:
                            await self._handle_closed_position(pos_key, closed_position_data=tracked_details)
                        return True 

                current_tp_str = target_position_data.get('takeProfit')
                current_sl_str = target_position_data.get('stopLoss')
                logger.debug(f"{symbol} PosIdx {position_idx}: Current TP on exchange: '{current_tp_str}', SL: '{current_sl_str}'")

                current_tp_decimal = Decimal(current_tp_str) if current_tp_str and current_tp_str != "0" else None
                current_sl_decimal = Decimal(current_sl_str) if current_sl_str and current_sl_str != "0" else None

                effective_intended_tp = tracked_details.get('intended_tp1') 
                effective_intended_sl = tracked_details.get('intended_sl')

                formatted_intended_tp = Decimal(self._format_price(effective_intended_tp, symbol)) if effective_intended_tp else None
                formatted_intended_sl = Decimal(self._format_price(effective_intended_sl, symbol)) if effective_intended_sl else None
                
                tp_matches = (formatted_intended_tp == current_tp_decimal) or (not formatted_intended_tp and not current_tp_decimal)
                sl_matches = (formatted_intended_sl == current_sl_decimal) or (not formatted_intended_sl and not current_sl_decimal)

                if tp_matches and sl_matches:
                    logger.info(f"TP/SL for {symbol} (PosIdx: {position_idx}) are correctly set. Formatted Intended TP: {formatted_intended_tp}, SL: {formatted_intended_sl}.")
                    return True

                logger.warning(f"TP/SL mismatch for {symbol} (PosIdx: {position_idx}). Attempt {attempt + 1}. "
                               f"Intended (Formatted) TP: {formatted_intended_tp}, SL: {formatted_intended_sl}. Exchange TP: {current_tp_decimal}, SL: {current_sl_decimal}. Attempting to set.")
                
                tp_to_set_str = self._format_price(effective_intended_tp, symbol) if effective_intended_tp else "0" 
                sl_to_set_str = self._format_price(effective_intended_sl, symbol) if effective_intended_sl else "0" 

                success = await self.bybit_trader.set_trading_stop(
                    symbol=symbol, take_profit=tp_to_set_str, stop_loss=sl_to_set_str,   
                    position_idx=position_idx, tpsl_mode="Partial" 
                )
                if success:
                    logger.info(f"Successfully re-applied TP/SL for {symbol} (PosIdx: {position_idx}) on attempt {attempt + 1}. TP: {tp_to_set_str}, SL: {sl_to_set_str}")
                    if pos_key in self.active_positions_details: 
                        self.active_positions_details[pos_key]['intended_tp1'] = effective_intended_tp 
                        self.active_positions_details[pos_key]['intended_sl'] = effective_intended_sl   
                        self.active_positions_details[pos_key]['last_update_time'] = datetime.utcnow()
                        self._save_active_positions_to_disk() 
                    await asyncio.sleep(1) 
                    return True 
                else:
                    logger.error(f"Failed to set TP/SL for {symbol} (PosIdx: {position_idx}) on attempt {attempt + 1}.")
                    if attempt < TPSL_VERIFICATION_RETRIES:
                        logger.info(f"Retrying TP/SL set for {symbol} in {TPSL_RETRY_DELAY_SECONDS}s...")
                        await asyncio.sleep(TPSL_RETRY_DELAY_SECONDS)
                    else:
                        logger.critical(f"Failed to set TP/SL for {symbol} (PosIdx: {position_idx}) after {TPSL_VERIFICATION_RETRIES + 1} attempts. MANUAL INTERVENTION MAY BE REQUIRED.")
                        self._add_trade_to_history(symbol, "System", "Alert", "N/A", None, None, "CRITICAL: TP/SL Set Fail", notes=f"PosIdx {position_idx}")
                        return False 

            except Exception as e:
                logger.error(f"Exception during TP/SL verification/setting for {symbol} (PosIdx: {position_idx}), attempt {attempt + 1}: {e}", exc_info=True)
                if attempt < TPSL_VERIFICATION_RETRIES:
                    await asyncio.sleep(TPSL_RETRY_DELAY_SECONDS)
                else: 
                    logger.critical(f"Exception on final TP/SL verification attempt for {symbol} (PosIdx: {position_idx}).")
                    return False
        return False 

    async def _periodic_verify_and_maintain_tpsl(self):
        logger.info("Starting periodic TP/SL verification and maintenance task.")
        await asyncio.sleep(15) 
        
        while self.running and not self._shutdown_event.is_set():
            try:
                if not self.bybit_connected: 
                    await asyncio.sleep(TPSL_PERIODIC_CHECK_INTERVAL_SECONDS); continue

                tracked_position_keys = list(self.active_positions_details.keys()) 
                if not tracked_position_keys:
                    logger.debug("No positions currently tracked for TP/SL maintenance.")
                    await asyncio.sleep(TPSL_PERIODIC_CHECK_INTERVAL_SECONDS); continue

                logger.debug(f"Periodic TP/SL Check: Processing {len(tracked_position_keys)} tracked positions.")
                
                for pos_key in tracked_position_keys:
                    if pos_key not in self.active_positions_details: 
                        logger.debug(f"Position {pos_key} no longer in active_positions_details. Skipping maintenance.")
                        continue 

                    symbol, position_idx = pos_key
                    pos_details = self.active_positions_details[pos_key] 
                    
                    main_order_status = pos_details.get('main_order_status')
                    # Changed: Also allow 'N/A (External/Synced)' for externally managed positions that might need BE
                    if main_order_status not in ['Filled', 'N/A (External/Synced)']: 
                        logger.debug(f"Main entry order for {symbol} (PosIdx {position_idx}) is still '{main_order_status}'. Skipping TP/SL maintenance for now.")
                        continue 

                    current_positions_on_exchange = await self.bybit_trader.get_open_positions(symbol=symbol, settleCoin=self.bybit_trader.default_coin)
                    pos_data_from_api = next((p for p in current_positions_on_exchange if p.get('symbol') == symbol and p.get('positionIdx') == position_idx), None)

                    if not pos_data_from_api or float(pos_data_from_api.get('size', '0')) == 0:
                        logger.info(f"Position {symbol} (PosIdx {position_idx}) no longer active on exchange (periodic check). Handling as closed.")
                        await self._handle_closed_position(pos_key, closed_position_data=pos_details) 
                        continue 

                    live_avg_price_str = pos_data_from_api.get('avgPrice')
                    if live_avg_price_str and live_avg_price_str != "0":
                        live_avg_price = Decimal(live_avg_price_str)
                        if pos_details.get('entry_price') != live_avg_price: 
                            logger.info(f"Updating tracked entry price for {symbol} (PosIdx {position_idx}) from {pos_details.get('entry_price')} to {live_avg_price}")
                            pos_details['entry_price'] = live_avg_price
                            self._save_active_positions_to_disk() 
                    
                    actual_entry_price = pos_details.get('entry_price') or pos_details.get('signal_entry_price') 
                    tp1_target_price_for_be = pos_details.get('tp1_price')
                    breakeven_already_applied = pos_details.get('breakeven_applied', False)
                    pos_side = pos_details.get('side', '').upper()
                    # --- ฟีเจอร์ใหม่: Partial TP แล้วย้าย SL ไป BE ---
                    if self.enable_breakeven_on_partial_tp and not breakeven_already_applied and actual_entry_price:
                        live_size = Decimal(pos_data_from_api.get('size', '0'))
                        last_known_size = pos_details.get('last_known_size', live_size)
                        if live_size < last_known_size:
                            pos_details['intended_sl'] = actual_entry_price
                            pos_details['breakeven_applied'] = True
                            self._save_active_positions_to_disk()
                            self._add_trade_to_history(symbol, "System", pos_side, str(live_size), str(actual_entry_price), None, "SL to Break-Even (Partial TP)", notes=f"PosIdx {position_idx}, Partial TP")
                            ui_status_log(f"ขนาด position {symbol} ลดลง (TP บางส่วน) ย้าย SL ไป BE อัตโนมัติ")
                            if self.telegram_bot and hasattr(self.telegram_bot, 'send_message_async'):
                                await self.telegram_bot.send_message_async(f"ขนาด position {symbol} ลดลง (TP บางส่วน) ย้าย SL ไป BE อัตโนมัติ")
                        pos_details['last_known_size'] = live_size
                    # --- END ฟีเจอร์ใหม่ ---
                    # ... logic BE เดิม ...
                    if self.enable_breakeven_on_tp1 and not breakeven_already_applied and \
                       tp1_target_price_for_be and actual_entry_price and actual_entry_price > 0:
                        tp1_hit_condition_met = False
                        current_market_price = None
                        try:
                            if self.bybit_trader and hasattr(self.bybit_trader, 'get_tickers'):
                                tickers_data = await self.bybit_trader.get_tickers(category=self.bybit_trader.trading_category, symbol=symbol)
                                if tickers_data and isinstance(tickers_data, list) and len(tickers_data) > 0:
                                    market_price_str = tickers_data[0].get('lastPrice')
                                    if market_price_str:
                                        current_market_price = Decimal(market_price_str)
                                        logger.debug(f"BE Check for {symbol} (PosIdx {position_idx}): Market Price={current_market_price}, TP1 Target={tp1_target_price_for_be}")
                            if current_market_price:
                                if pos_side == 'BUY' and current_market_price >= tp1_target_price_for_be:
                                    tp1_hit_condition_met = True
                                    logger.info(f"BE Condition MET (BUY) for {symbol}: Market Price {current_market_price} >= TP1 {tp1_target_price_for_be}")
                                elif pos_side == 'SELL' and current_market_price <= tp1_target_price_for_be:
                                    tp1_hit_condition_met = True
                                    logger.info(f"BE Condition MET (SELL) for {symbol}: Market Price {current_market_price} <= TP1 {tp1_target_price_for_be}")
                            else:
                                logger.warning(f"Could not get current market price for {symbol} to check break-even condition for PosIdx {position_idx}.")
                            if tp1_hit_condition_met:
                                new_sl_target_for_be = actual_entry_price 
                                formatted_new_sl = Decimal(self._format_price(new_sl_target_for_be, symbol)) 
                                current_formatted_intended_sl = Decimal(self._format_price(pos_details.get('intended_sl'), symbol)) if pos_details.get('intended_sl') else None
                                if current_formatted_intended_sl != formatted_new_sl : 
                                    logger.info(f"TP1 condition met for {symbol} (PosIdx {position_idx}). Moving SL to Break-Even: {formatted_new_sl} (from actual entry: {actual_entry_price})")
                                    pos_details['intended_sl'] = new_sl_target_for_be 
                                    pos_details['breakeven_applied'] = True
                                    self._save_active_positions_to_disk() 
                                    self._add_trade_to_history(symbol, "System", pos_side, pos_data_from_api.get('size', 'N/A'), 
                                                               str(actual_entry_price), None, "SL to Break-Even", 
                                                               notes=f"PosIdx {position_idx}, TP1 hit")
                                else:
                                    logger.info(f"Break-even SL for {symbol} (PosIdx {position_idx}) already at or effectively at entry price {formatted_new_sl}. No change needed, marking BE as applied.")
                                    pos_details['breakeven_applied'] = True 
                                    self._save_active_positions_to_disk() 
                        except Exception as e_be:
                            logger.error(f"Error during Break-Even logic for {symbol} (PosIdx {position_idx}): {e_be}", exc_info=True)
                    await self._verify_and_set_tpsl_for_position(symbol, pos_details.get('intended_tp1'), pos_details.get('intended_sl'), position_idx)
                    await asyncio.sleep(0.2) 

            except asyncio.CancelledError:
                logger.info("Periodic TP/SL verification task cancelled.")
                break 
            except Exception as e:
                logger.error(f"Error in periodic TP/SL verification task: {e}", exc_info=True)
            
            await asyncio.sleep(TPSL_PERIODIC_CHECK_INTERVAL_SECONDS) 
        logger.info("Periodic TP/SL verification and maintenance task stopped.")


    async def _cleanup_tasks(self):
        logger.info(f"Cleaning up {len(self._periodic_tasks)} tasks...")
        for task in self._periodic_tasks:
            if task and not task.done(): 
                task.cancel()
        if self._periodic_tasks: 
            results = await asyncio.gather(*self._periodic_tasks, return_exceptions=True)
            for i, res in enumerate(results):
                if isinstance(res, Exception) and not isinstance(res, asyncio.CancelledError):
                    task_name = "Unknown Task"
                    if i < len(self._periodic_tasks) and hasattr(self._periodic_tasks[i], 'get_name'):
                        task_name = self._periodic_tasks[i].get_name()
                    logger.error(f"Error cleaning task {task_name} (index {i}): {res}")
        self._periodic_tasks.clear()
        logger.info("Tasks cleanup complete.")

    async def stop(self):
        logger.info("TradingBot stopping sequence...");
        if not self.running: logger.info("TradingBot was not running."); return
        
        self.running = False 
        self._shutdown_event.set() 
        
        if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists(): 
            self._app_instance.master.after(0, lambda: self._app_instance.update_status_bar("Bot Stopping...", "orange"))
        
        await self._cleanup_tasks() 
        
        if self.telegram_bot and hasattr(self.telegram_bot, 'stop'):
            logger.info("Stopping TelegramBot...");
            try: await self.telegram_bot.stop()
            except Exception as e_tg_stop: logger.error(f"Error stopping TelegramBot: {e_tg_stop}", exc_info=True)
        self.telegram_connected = False
        
        if self.bybit_trader and hasattr(self.bybit_trader, 'close'): 
            logger.info("Closing BybitTrader (if applicable)...");
            try: await self.bybit_trader.close() 
            except Exception as e_bybit_close: logger.error(f"Error closing BybitTrader: {e_bybit_close}", exc_info=True)
        self.bybit_connected = False
        
        self._save_active_positions_to_disk()
        self._save_trade_history_to_disk()
        logger.info("Final state saved to disk.")

        self.current_balance=self.current_available_balance=self.current_margin=0.0; self.total_orders_count=self.total_positions_count=0
        if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists():
            self._app_instance.master.after(0, self._app_instance.update_trading_info_ui) 
            self._app_instance.master.after(0, lambda: self._app_instance.update_bybit_status_ui(False))
            self._app_instance.master.after(0, lambda: self._app_instance.update_telegram_status_ui(False))
            self._app_instance.master.after(0, lambda: self._app_instance.update_status_bar("Bot Stopped", "gray"))

        logger.info("TradingBot stopped successfully.")


    def get_runtime_str(self) -> str:
        if not self.running or self.start_time is None: return "00:00:00"
        s = int((datetime.now() - self.start_time).total_seconds())
        return f"{s//3600:02}:{(s%3600)//60:02}:{s%60:02}"

    async def _calculate_position_size_fixed(self, signal: TradingSignal) -> Decimal:
        await self._fetch_and_store_instrument_info(signal.symbol)
        
        max_size_config_key = f"{signal.symbol.upper()}_max_position_size"
        order_qty_asset_str = self.config.get('BYBIT', max_size_config_key, 
                                           fallback=self.config.get('BYBIT', 'max_position_size', fallback='0.001'))
        try:
            order_qty_asset = Decimal(order_qty_asset_str)
            logger.info(f"Fixed position size for {signal.symbol}: {order_qty_asset} (from config: '{max_size_config_key}' or 'max_position_size')")
            return order_qty_asset
        except InvalidOperation:
            logger.error(f"Invalid fixed position size in config for {signal.symbol}: '{order_qty_asset_str}'. Defaulting to 0.")
            return Decimal('0')

    async def _calculate_position_size_risk_percentage(self, signal: TradingSignal) -> Decimal:
        await self._fetch_and_store_instrument_info(signal.symbol)

        if self.current_available_balance <= 0: 
            logger.error(f"Available balance is {self.current_available_balance:.2f}. Cannot calculate risk-based position size for {signal.symbol}.")
            return Decimal('0')

        if not signal.entry_price or signal.entry_price <= Decimal(0):
            logger.error(f"Valid entry price required for risk-based position size calculation for {signal.symbol}. Signal entry: {signal.entry_price}")
            return Decimal('0')
        
        if not signal.stop_loss or signal.stop_loss <= Decimal(0):
            logger.error(f"Valid stop loss required for risk-based position size calculation for {signal.symbol}. Signal SL: {signal.stop_loss}")
            return Decimal('0')

        entry_price = signal.entry_price
        stop_loss_price = signal.stop_loss

        if signal.position.upper() == "LONG" and entry_price <= stop_loss_price:
            logger.error(f"Invalid LONG signal for risk calculation: Entry {entry_price} <= SL {stop_loss_price}")
            return Decimal('0')
        if signal.position.upper() == "SHORT" and entry_price >= stop_loss_price:
            logger.error(f"Invalid SHORT signal for risk calculation: Entry {entry_price} >= SL {stop_loss_price}")
            return Decimal('0')

        price_diff_per_unit = abs(entry_price - stop_loss_price)
        if price_diff_per_unit <= Decimal(0):
            logger.error(f"Price difference for risk calculation is zero or negative for {signal.symbol}. Entry: {entry_price}, SL: {stop_loss_price}")
            return Decimal('0')

        risk_amount_usdt = Decimal(str(self.current_available_balance)) * (Decimal(str(self.risk_per_trade_percentage)) / Decimal('100'))
        position_size_asset = risk_amount_usdt / price_diff_per_unit
        
        logger.info(f"Risk-based calculation for {signal.symbol}: AvailBal={self.current_available_balance:.2f}, Risk%={self.risk_per_trade_percentage}, "
                    f"RiskAmt={risk_amount_usdt:.2f}, Entry={entry_price}, SL={stop_loss_price}, PriceDiff={price_diff_per_unit}, CalcQty={position_size_asset:.8f}")
        return position_size_asset


    async def _determine_total_entry_qty(self, signal: TradingSignal) -> Decimal:
        logger.debug(f"Determining entry quantity for {signal.symbol} using mode: {self.position_size_mode}")
        calculated_qty_asset = Decimal('0')

        if self.position_size_mode == 'fixed':
            calculated_qty_asset = await self._calculate_position_size_fixed(signal)
        elif self.position_size_mode == 'risk_percentage':
            calculated_qty_asset = await self._calculate_position_size_risk_percentage(signal)
        else:
            logger.warning(f"Unknown position_size_mode: '{self.position_size_mode}'. Defaulting to fixed calculation.")
            calculated_qty_asset = await self._calculate_position_size_fixed(signal)
        
        if calculated_qty_asset <= Decimal(0):
            logger.error(f"Calculated quantity for {signal.symbol} is zero or negative ({calculated_qty_asset}). Cannot proceed.")
            return Decimal('0')

        min_order_qty_str = self._get_instrument_detail(signal.symbol, 'minOrderQty', str(DEFAULT_MIN_ORDER_QTY))
        try:
            min_order_qty = Decimal(min_order_qty_str)
        except InvalidOperation:
            logger.error(f"Invalid min_order_qty_str '{min_order_qty_str}' for {signal.symbol}. Using default.")
            min_order_qty = DEFAULT_MIN_ORDER_QTY
        
        if calculated_qty_asset < min_order_qty:
            logger.error(f"Calculated position size {calculated_qty_asset} for {signal.symbol} is below minimum order quantity {min_order_qty}. Aborting trade.")
            self._add_trade_to_history(
                symbol=signal.symbol, order_type="System", side=signal.position,
                qty=str(calculated_qty_asset), price=str(signal.entry_price) if signal.entry_price else "Market",
                order_id="N/A", status="Fail: Qty < Min"
            )
            return Decimal('0')
            
        return calculated_qty_asset


    async def execute_trade_from_signal(self, signal: TradingSignal):
        if not self.running or not self.bybit_connected:
            logger.warning("Bot/Bybit not ready for trade execution based on signal.")
            if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists():
                self._app_instance.master.after(0, lambda: self._app_instance.update_status_bar("Trade: Bot not ready.", "red"))
            return False

        if signal.symbol.upper() != self.target_trading_symbol:
            logger.info(f"Signal received for {signal.symbol}, but bot is configured to trade only {self.target_trading_symbol}. Ignoring signal.")
            self._add_trade_to_history(
                symbol=signal.symbol, order_type="Signal", side=signal.position,
                qty="0", price="N/A", order_id="N/A", status=f"Ignored (Not {self.target_trading_symbol})"
            )
            return False

        # --- ปิด position เดิมก่อนเข้าไม้ใหม่ ---
        open_positions = await self.bybit_trader.get_open_positions(symbol=signal.symbol)
        for pos in open_positions:
            size = float(pos.get('size', 0))
            if size > 0:
                side = pos.get('side')
                pos_idx = pos.get('positionIdx', 0)
                await self.bybit_trader.close_position(symbol=signal.symbol, side_of_position_to_close=side, qty_to_close=size, position_idx=pos_idx)
                ui_status_log(f"ปิด position เดิม {signal.symbol} ขนาด {size} ฝั่ง {side} ก่อนเข้าไม้ใหม่")
                if self.telegram_bot and hasattr(self.telegram_bot, 'send_message_async'):
                    await self.telegram_bot.send_message_async(f"ปิด position เดิม {signal.symbol} ขนาด {size} ฝั่ง {side} ก่อนเข้าไม้ใหม่")
                await asyncio.sleep(1)  # รอให้ปิด position สำเร็จ
        # --- END ปิด position เดิม ---

        # --- LOG BLOCK แบบในรูป ---
        order_msg = (
            f"🚩 กำลังเข้าออเดอร์: {signal.symbol}\n"
            f"รายละเอียดสัญญาณ:\n"
            f"- คู่เงิน: {signal.symbol}\n"
            f"- ทิศทาง: {signal.position}\n"
            f"- เลเวอเรจ: {signal.leverage if signal.leverage else self.config.getint('BYBIT', 'default_leverage', fallback=10)}\n"
            f"- จำนวน: {str(await self._determine_total_entry_qty(signal))}\n"
            f"- จำนวน TP: {len(signal.take_profits)}\n"
            f"- ราคาเปิด: {signal.entry_price if signal.entry_price else 'Market'}\n"
            f"- Stop Loss: {signal.stop_loss if signal.stop_loss else '-'}\n"
        )
        if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists():
            self._app_instance.master.after(0, lambda: ui_status_log(order_msg))
        else:
            ui_status_log(order_msg)
        # --- END LOG BLOCK ---

        if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists():
            self._app_instance.master.after(0, lambda: self._app_instance.update_status_bar(f"Trade: Processing {signal.position} {signal.symbol}...", "orange"))

        await self._fetch_and_store_instrument_info(signal.symbol) 

        try:
            total_entry_qty_decimal = await self._determine_total_entry_qty(signal)
            if total_entry_qty_decimal <= Decimal(0):
                logger.error(f"Total entry quantity determined to be zero or negative for {signal.symbol}. Aborting trade.")
                return False

            total_entry_qty_str = self._format_quantity(total_entry_qty_decimal, signal.symbol)
            if Decimal(total_entry_qty_str) <= Decimal(0): 
                logger.error(f"Formatted entry quantity '{total_entry_qty_str}' is zero or negative for {signal.symbol}. Aborting trade.")
                self._add_trade_to_history(
                     symbol=signal.symbol, order_type="System", side=signal.position,
                     qty=total_entry_qty_str, price=str(signal.entry_price) if signal.entry_price else "Market",
                     order_id="N/A", status="Fail: Qty Format Error"
                )
                if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists():
                    self._app_instance.master.after(0, lambda: self._app_instance.update_status_bar(f"Trade: Qty Format Error for {signal.symbol}", "red"))
                return False
            logger.info(f"Formatted total entry quantity for {signal.symbol}: {total_entry_qty_str} (Decimal: {total_entry_qty_decimal})")

            position_idx_for_trade = 0 
            
            existing_pos_details = self.active_positions_details.get((signal.symbol, position_idx_for_trade))
            if existing_pos_details and existing_pos_details.get('side','').upper() != signal.position.upper():
                logger.warning(f"Signal for {signal.symbol} ({signal.position}) is opposite to existing tracked position ({existing_pos_details.get('side')}). Manual check advised. Proceeding with new signal.")

            leverage_to_set = str(signal.leverage if signal.leverage else self.config.getint('BYBIT', 'default_leverage', fallback=10))
            leverage_set_successfully = await self.bybit_trader.set_leverage(signal.symbol, leverage_to_set, leverage_to_set) 

            if not leverage_set_successfully:
                logger.error(f"Failed to set leverage for {signal.symbol} to x{leverage_to_set}. Aborting trade.")
                self._add_trade_to_history(
                    symbol=signal.symbol, order_type="System", side=signal.position,
                    qty=total_entry_qty_str, price=str(signal.entry_price) if signal.entry_price else "Market",
                    order_id="N/A", status="Fail: Leverage Set"
                )
                if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists():
                    self._app_instance.master.after(0, lambda: self._app_instance.update_status_bar(f"Trade: Leverage Set Fail {signal.symbol}", "red"))
                return False
            logger.info(f"Leverage for {signal.symbol} confirmed/set to x{leverage_to_set}.")

            entry_order_side = "Buy" if signal.position.upper() == "LONG" else "Sell"
            entry_order_type = "Limit" if signal.entry_price and signal.entry_price > Decimal(0) else "Market"
            entry_price_for_order_str = self._format_price(signal.entry_price, signal.symbol) if entry_order_type == "Limit" and signal.entry_price else None

            intended_sl_price = signal.stop_loss
            intended_tp1_price = next((tp for tp in signal.take_profits if tp and tp > Decimal(0)), None)

            entry_price_for_calc = signal.entry_price 
            if not entry_price_for_calc or entry_price_for_calc <= Decimal(0): 
                logger.info(f"Signal for {signal.symbol} is Market or has no entry price. Fetching current price for TP/SL calculation.")
                tickers_data = await self.bybit_trader.get_tickers(category=self.bybit_trader.trading_category, symbol=signal.symbol)
                if tickers_data and isinstance(tickers_data, list) and len(tickers_data) > 0:
                    market_price_str = tickers_data[0].get('lastPrice')
                    if market_price_str: entry_price_for_calc = Decimal(market_price_str)
                    logger.info(f"Using current market price {entry_price_for_calc} for TP/SL calculation of {signal.symbol} market order.")
                else:
                    logger.warning(f"Could not fetch current market price for {signal.symbol} market order. TP/SL from % might be inaccurate or skipped.")
            
            if entry_price_for_calc and entry_price_for_calc > Decimal(0):
                if not intended_sl_price: 
                    sl_percentage_config = self.config.getfloat('BYBIT', 'stop_loss_percentage', fallback=0)
                    if sl_percentage_config > 0:
                        if signal.position.upper() == "LONG": intended_sl_price = entry_price_for_calc * (Decimal(1) - (Decimal(str(sl_percentage_config)) / Decimal(100)))
                        else: intended_sl_price = entry_price_for_calc * (Decimal(1) + (Decimal(str(sl_percentage_config)) / Decimal(100)))
                        logger.info(f"Calculated SL for {signal.symbol} based on {sl_percentage_config}%: {intended_sl_price} (from entry {entry_price_for_calc})")
                
                if not intended_tp1_price: 
                    tp_percentage_config = self.config.getfloat('BYBIT', 'take_profit_percentage', fallback=0)
                    if tp_percentage_config > 0:
                        if signal.position.upper() == "LONG": intended_tp1_price = entry_price_for_calc * (Decimal(1) + (Decimal(str(tp_percentage_config)) / Decimal(100)))
                        else: intended_tp1_price = entry_price_for_calc * (Decimal(1) - (Decimal(str(tp_percentage_config)) / Decimal(100)))
                        logger.info(f"Calculated TP1 for {signal.symbol} based on {tp_percentage_config}%: {intended_tp1_price} (from entry {entry_price_for_calc})")

            sl_price_for_main_order_str = self._format_price(intended_sl_price, signal.symbol) if intended_sl_price and intended_sl_price > Decimal(0) else None
            first_tp_price_for_main_order_str = self._format_price(intended_tp1_price, signal.symbol) if intended_tp1_price and intended_tp1_price > Decimal(0) else None
            
            logger.info(f"Placing main entry order for {signal.symbol} (PosIdx {position_idx_for_trade}): {entry_order_side} {total_entry_qty_str} "
                        f"@ {entry_price_for_order_str or 'Market'}, "
                        f"Attempting TP: {first_tp_price_for_main_order_str or 'N/A'}, SL: {sl_price_for_main_order_str or 'N/A'}")

            entry_order_result = await self.bybit_trader.place_order(
                symbol=signal.symbol, side=entry_order_side, qty=total_entry_qty_str,
                order_type=entry_order_type, 
                price=float(entry_price_for_order_str) if entry_price_for_order_str else None, 
                take_profit=float(first_tp_price_for_main_order_str) if first_tp_price_for_main_order_str else None,
                stop_loss=float(sl_price_for_main_order_str) if sl_price_for_main_order_str else None,
                position_idx=position_idx_for_trade, 
                tpsl_mode="Partial", 
                tp_trigger_by=self.config.get('BYBIT', 'tp_trigger_by', fallback="LastPrice"),
                sl_trigger_by=self.config.get('BYBIT', 'sl_trigger_by', fallback="LastPrice")
            )

            if not entry_order_result or entry_order_result.get('retCode') != 0 or not entry_order_result.get('result', {}).get('orderId'):
                error_msg = entry_order_result.get('retMsg', 'Unknown error') if entry_order_result else 'No response or malformed response'
                logger.error(f"Failed to place main entry order for {signal.symbol}. API Response: {entry_order_result}")
                self._add_trade_to_history(
                    symbol=signal.symbol, order_type=entry_order_type, side=entry_order_side,
                    qty=total_entry_qty_str, price=entry_price_for_order_str or "Market",
                    order_id="N/A", status=f"Fail: Entry - {error_msg}"
                )
                if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists():
                    self._app_instance.master.after(0, lambda: self._app_instance.update_status_bar(f"Trade: Entry Fail {signal.symbol} ({error_msg})", "red"))
                return False
            
            main_order_id = entry_order_result['result']['orderId']
            entry_price_for_tracking = entry_price_for_calc if entry_price_for_calc and entry_price_for_calc > Decimal(0) else None 

            self._add_trade_to_history(
                symbol=signal.symbol, order_type=entry_order_type, side=entry_order_side,
                qty=total_entry_qty_str, price=entry_price_for_order_str or "Market", 
                order_id=main_order_id, status="Entry Placed",
                notes=f"PosIdx {position_idx_for_trade}, Initial TP@{first_tp_price_for_main_order_str or 'N/A'}, SL@{sl_price_for_main_order_str or 'N/A'}"
            )
            logger.info(f"Main entry order for {signal.symbol} (PosIdx {position_idx_for_trade}) placed. ID: {main_order_id}")
            if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists():
                 self._app_instance.master.after(0, lambda: self._app_instance.update_status_bar(f"Trade: {signal.symbol} Entry Placed", "green"))

            current_pos_key = (signal.symbol, position_idx_for_trade)
            self.active_positions_details[current_pos_key] = {
                'symbol': signal.symbol,
                'position_idx': position_idx_for_trade,
                'side': signal.position.upper(),
                'entry_price': None, 
                'signal_entry_price': entry_price_for_tracking, 
                'intended_sl': intended_sl_price, 
                'intended_tp1': intended_tp1_price, 
                'tp1_price': intended_tp1_price, 
                'breakeven_applied': False,
                'main_order_id': main_order_id,
                'main_order_status': 'New', 
                'tp_order_ids': [], 
                'last_update_time': datetime.utcnow(),
                'last_known_size': total_entry_qty_decimal 
            }
            logger.info(f"Stored initial details for {signal.symbol} (PosIdx {position_idx_for_trade}): TP1={intended_tp1_price}, SL={intended_sl_price}, SignalEntry={entry_price_for_tracking}")
            self._save_active_positions_to_disk() 

            if entry_order_type == "Market" or (first_tp_price_for_main_order_str or sl_price_for_main_order_str):
                await asyncio.sleep(TPSL_VERIFICATION_DELAY_SECONDS) 
                await self._verify_and_set_tpsl_for_position(signal.symbol, intended_tp1_price, intended_sl_price, position_idx_for_trade)
            
            await self.update_initial_trading_data() 
            return True
    # ก่อนเข้า order ใหม่
        except Exception as e:
            logger.error(f"Exception during trade execution for {signal.symbol}: {e}", exc_info=True)
            self._add_trade_to_history(
                symbol=signal.symbol, order_type="System", side=signal.position,
                qty="N/A", price="N/A", order_id="N/A", status=f"ERROR: {str(e)[:30]}"
            )
            if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master.winfo_exists():
                 self._app_instance.master.after(0, lambda: self._app_instance.update_status_bar(f"Trade Error: {str(e)[:50]}", "red"))
            return False

    def get_active_positions_display_data(self) -> List[Dict[str, Any]]:
        display_data = []
        active_details_copy = list(self.active_positions_details.items())

        for key, details in active_details_copy:
            symbol, pos_idx = key
            current_pos_on_exchange = next((p for p in self.current_open_positions 
                                            if p.get('symbol') == symbol and p.get('positionIdx') == pos_idx), None)
            
            size_display = details.get('last_known_size', 'N/A') 
            if size_display is not None and not isinstance(size_display, str): size_display = str(size_display)
            
            entry_price_display = str(details.get('entry_price', 'N/A')) 
            unrealised_pnl_display = '0.00' 

            if current_pos_on_exchange: 
                size_display = current_pos_on_exchange.get('size', str(details.get('last_known_size', 'N/A')))
                if float(current_pos_on_exchange.get('size','0')) > 0:
                    entry_price_display = current_pos_on_exchange.get('avgPrice', str(details.get('entry_price', 'N/A')))
                
                pnl_val_str = current_pos_on_exchange.get('unrealisedPnl', '0')
                try: 
                    unrealised_pnl_display = f"{Decimal(pnl_val_str):.2f}"
                except (InvalidOperation, TypeError):
                    unrealised_pnl_display = pnl_val_str 
            
            data_item = {
                "symbol_pidx": f"{symbol} ({pos_idx})",
                "side_size": f"{details.get('side', 'N/A')} | {size_display}",
                "entry": entry_price_display,
                "pnl": unrealised_pnl_display,
                "sl": str(details.get('intended_sl', 'N/A')),
                "tp1": str(details.get('intended_tp1', 'N/A')),
                "be_applied": "Yes" if details.get('breakeven_applied') else "No",
                "main_order_status": str(details.get('main_order_status', 'N/A')),
                "tp_orders_left": str(len(details.get('tp_order_ids', []))), 
            }
            display_data.append(data_item)
        return display_data

