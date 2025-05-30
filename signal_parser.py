import re
from dataclasses import dataclass, field
from typing import List, Optional
from decimal import Decimal, InvalidOperation
import logging

logger = logging.getLogger(__name__)

@dataclass
class TradingSignal:
    symbol: str
    position: str  # "LONG" or "SHORT"
    entry_price: Optional[Decimal] = None
    take_profits: List[Decimal] = field(default_factory=list)
    stop_loss: Optional[Decimal] = None
    leverage: Optional[int] = None
    # Fields from RICHI Crypto signal example (backtest_days_on, winrate_percentage, etc.) REMOVED


class SignalParser:
    @staticmethod
    def _clean_price_str(price_str: str) -> str:
        # Remove commas, leading/trailing whitespace
        cleaned = price_str.replace(',', '').strip()
        # Attempt to remove any non-numeric characters except for the decimal point
        cleaned = re.sub(r'[^\d.]', '', cleaned) # More robust cleaning
        return cleaned

    @staticmethod
    def parse_signal(message: str) -> Optional[TradingSignal]:
        """
        Parse a trading signal message into a TradingSignal object.
        Focuses on core trading information and ignores statistical/backtest data.
        """
        logger.debug(f"Attempting to parse signal message:\n{message[:500]}...")
        try:
            symbol = None
            position = None
            entry_price = None
            take_profits = []
            stop_loss = None
            leverage = None

            # Enhanced Symbol Parsing: Handles #, .P, and common variations more robustly
            # Example signal: #BTCUSDT.P
            symbol_match = re.search(r"Coin\s*:\s*#?([A-Z0-9]+(?:USDT\.P|USDT|\.P)?)", message, re.IGNORECASE)
            if symbol_match:
                symbol_text = symbol_match.group(1).upper()
                # Normalize symbol: Remove .P, ensure USDT is the quote asset if not specified for common pairs
                if symbol_text.endswith(".P"):
                    symbol_text = symbol_text[:-2] # Remove .P -> BTCUSDT or BTC

                # If it's just like "BTC", assume "BTCUSDT". If "BTCUSDT", it's fine.
                # Avoid adding USDT if it looks like a specific contract name with numbers (e.g., BTC1000)
                if "USDT" not in symbol_text and not any(char.isdigit() for char in symbol_text.replace("BTC","").replace("ETH","")): # Basic check to avoid altering e.g. BTC1000
                    if not symbol_text.endswith("USDT"): # Ensure we don't double-add USDT
                        symbol = f"{symbol_text}USDT"
                    else:
                        symbol = symbol_text # Already ends with USDT
                else:
                    symbol = symbol_text # Contains USDT or is a numeric contract
                logger.debug(f"Parsed Symbol: {symbol}")


            # Enhanced Position Parsing: More flexible to handle surrounding characters/emojis
            # Looks for "Position", then colon, then optional non-alphanumeric chars (including newlines if not careful), then LONG or SHORT
            # Using [^A-Z\n] to avoid matching across newlines if LONG/SHORT is on a different line without "Position:"
            position_match = re.search(r"Position\s*:\s*[^A-Z\n]*(LONG|SHORT)[^A-Z\n]*", message, re.IGNORECASE | re.UNICODE)
            if position_match:
                position = position_match.group(1).upper()
                logger.debug(f"Parsed Position: {position}")
            else: # Fallback if the above is too broad or fails
                # More careful fallback: search for LONG/SHORT and check if "position" is in the same line
                position_match_simple = re.search(r"(LONG|SHORT)", message, re.IGNORECASE | re.UNICODE)
                if position_match_simple:
                    line_with_position_keyword = ""
                    for line in message.splitlines():
                        if "position" in line.lower() and position_match_simple.group(1).lower() in line.lower():
                            line_with_position_keyword = line
                            break
                    if line_with_position_keyword:
                        position = position_match_simple.group(1).upper()
                        logger.debug(f"Parsed Position (fallback from line context): {position}")
                    else:
                        logger.warning(f"Found '{position_match_simple.group(1)}' but not clearly in a 'Position:' line context.")
                else:
                     logger.warning("Could not parse Position from message.")


            entry_match = re.search(r"(?:Open Price|Entry)\s*:\s*([\d,]+\.?\d*)", message, re.IGNORECASE)
            if entry_match:
                try:
                    entry_price = Decimal(SignalParser._clean_price_str(entry_match.group(1)))
                    logger.debug(f"Parsed Entry Price: {entry_price}")
                except InvalidOperation:
                    logger.warning(f"Could not parse entry price from: {entry_match.group(1)}")

            tp_pattern = re.compile(r"Take Profit \d+\s*:\s*([\d,]+\.?\d*)", re.IGNORECASE)
            for tp_match in tp_pattern.finditer(message):
                try:
                    tp_val = Decimal(SignalParser._clean_price_str(tp_match.group(1)))
                    take_profits.append(tp_val)
                except InvalidOperation:
                    logger.warning(f"Could not parse TP value from: {tp_match.group(1)}")
            if take_profits:
                logger.debug(f"Parsed Take Profits: {take_profits}")

            sl_match = re.search(r"Stoploss\s*:\s*([\d,]+\.?\d*)", message, re.IGNORECASE)
            if sl_match:
                try:
                    stop_loss = Decimal(SignalParser._clean_price_str(sl_match.group(1)))
                    logger.debug(f"Parsed Stop Loss: {stop_loss}")
                except InvalidOperation:
                    logger.warning(f"Could not parse SL value from: {sl_match.group(1)}")
            
            leverage_match = re.search(r"Leverage\s*:\s*x?(\d+)", message, re.IGNORECASE)
            if leverage_match:
                try:
                    leverage = int(leverage_match.group(1))
                    logger.debug(f"Parsed Leverage: x{leverage}")
                except ValueError:
                    logger.warning(f"Could not parse leverage from: {leverage_match.group(1)}")

            if not symbol or not position:
                missing_parts = []
                if not symbol: missing_parts.append("Symbol")
                if not position: missing_parts.append("Position")
                logger.warning(f"Signal parsing failed: {', '.join(missing_parts)} not found or parsed correctly in the message.")
                return None

            return TradingSignal(
                symbol=symbol,
                position=position,
                entry_price=entry_price,
                take_profits=take_profits,
                stop_loss=stop_loss,
                leverage=leverage
            )

        except Exception as e:
            logger.error(f"Unexpected error parsing signal message: {str(e)}", exc_info=True)
            return None

    @staticmethod
    def validate_signal(signal: Optional[TradingSignal]) -> bool:
        if not signal:
            logger.warning("Signal validation failed: Signal object is None.")
            return False
        if not all([signal.symbol, signal.position]): 
            logger.warning(f"Signal validation failed: Missing symbol or position. Signal: {signal}")
            return False

        if signal.entry_price is None or signal.entry_price <= Decimal(0):
             if signal.entry_price is not None: 
                logger.warning(f"Signal validation failed: Invalid entry price for {signal.symbol}. Entry: {signal.entry_price}")
                return False
             logger.info(f"Signal for {signal.symbol} has no entry price; assuming market order if other fields are valid.")

        if not signal.take_profits and not signal.stop_loss:
            logger.warning(f"Signal validation warning: {signal.symbol} has no Take Profit and no Stop Loss defined.")

        if signal.stop_loss is not None and signal.stop_loss <= Decimal(0):
            logger.warning(f"Signal validation failed: Invalid Stop Loss price for {signal.symbol}. SL: {signal.stop_loss}")
            return False

        for tp in signal.take_profits:
            if tp <= Decimal(0):
                logger.warning(f"Signal validation failed: Invalid Take Profit price for {signal.symbol}. TP: {tp}")
                return False

        if signal.entry_price and signal.entry_price > Decimal(0): 
            if signal.stop_loss:
                if signal.position == "LONG" and signal.entry_price <= signal.stop_loss:
                    logger.warning(f"Invalid LONG signal: Entry Price {signal.entry_price} <= Stop Loss {signal.stop_loss}")
                    return False
                if signal.position == "SHORT" and signal.entry_price >= signal.stop_loss:
                    logger.warning(f"Invalid SHORT signal: Entry Price {signal.entry_price} >= Stop Loss {signal.stop_loss}")
                    return False

            if signal.take_profits:
                for tp in signal.take_profits:
                    if signal.position == "LONG" and tp <= signal.entry_price:
                        logger.warning(f"Invalid LONG signal: TP {tp} <= Entry Price {signal.entry_price}")
                        return False
                    if signal.position == "SHORT" and tp >= signal.entry_price:
                        logger.warning(f"Invalid SHORT signal: TP {tp} >= Entry Price {signal.entry_price}")
                        return False
        
        if signal.leverage is not None and (signal.leverage <=0 or signal.leverage > 125): 
            logger.warning(f"Signal validation: Leverage x{signal.leverage} for {signal.symbol} is out of typical range.")

        logger.info(f"Signal for {signal.symbol} passed basic validation.")
        return True
