import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, simpledialog
import configparser
import logging
from logging import handlers
import queue
import threading
import asyncio
import os
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple
import re
import pandas as pd
import subprocess
import requests, zipfile, io, shutil

from bybit_trader import BybitTrader
from telegram_bot import TelegramBot
from trading_bot import TradingBot
from utils import SecureConfig

# --- เพิ่ม import สำหรับ Multi-Exchange ---
try:
    from binance_trader import BinanceTrader
except ImportError:
    class BinanceTrader: pass
try:
    from okx_trader import OKXTrader
except ImportError:
    class OKXTrader: pass

log_queue = queue.Queue()
logger: Optional[logging.Logger] = None

class QueueHandler(logging.Handler):
    def __init__(self, log_queue_obj):
        super().__init__()
        self.log_queue_obj = log_queue_obj
    def emit(self, record):
        self.log_queue_obj.put(self.format(record))

def setup_logger_for_app(log_file='logs/trading_app.log', level=logging.INFO, log_queue_obj_param=None) -> logging.Logger:
    global logger
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        try: os.makedirs(log_dir)
        except OSError as e:
            # Fallback basic config if directory creation fails
            logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
            logging.error(f"CRITICAL: Could not create logs directory '{log_dir}': {e}. Logging to console only.")
            logger = logging.getLogger("FallbackLogger")
            return logger

    current_logger = logging.getLogger() # Get root logger
    # Clear existing handlers from the root logger to avoid duplication if this function is called multiple times
    if current_logger.hasHandlers():
        for handler in current_logger.handlers[:]:
            current_logger.removeHandler(handler)
            handler.close()

    current_logger.setLevel(level)

    # Updated formatter to match the desired style
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(name)s: %(message)s', datefmt='%H:%M:%S')

    # File Handler
    try:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        current_logger.addHandler(file_handler)
    except Exception as e_fh:
        # If file handler fails, log to console about it
        current_logger.error(f"Failed to create file handler for '{log_file}': {e_fh}")

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    current_logger.addHandler(console_handler)

    # Queue Handler (for UI)
    if log_queue_obj_param:
        queue_handler = QueueHandler(log_queue_obj_param)
        queue_handler.setFormatter(formatter)
        current_logger.addHandler(queue_handler)

    # Configure log levels for specific noisy libraries
    logging.getLogger("pybit").setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("pyrogram").setLevel(logging.WARNING) # Adjusted from INFO to WARNING

    logger = current_logger # Assign the configured root logger to the global variable
    if logger:
        logger.info(f"Application Logger setup complete. Level: {logging.getLevelName(logger.getEffectiveLevel())}. Log file: {log_file}")
    else:
        # This case should ideally not be reached if setup is correct
        print("ERROR: Global logger assignment failed in setup_logger_for_app.")
    return logger

class TradingApp:
    def __init__(self, master):
        global logger, log_queue # Ensure global logger is used
        self.master = master
        self.master.title("Bybit Copy Bot | v2.6.1 (ภาษาไทย)") # Updated version
        self.master.geometry("1280x760")
        self.master.protocol("WM_DELETE_WINDOW", self._on_closing)
        self.master.attributes('-topmost', False) # Allow other windows on top

        # Initialize config first
        try:
            if not os.path.exists('config.ini'):
                messagebox.showerror("ข้อผิดพลาดการตั้งค่า", "ไม่พบไฟล์ config.ini! กรุณาสร้างไฟล์")
                if master.winfo_exists(): master.destroy()
                raise SystemExit("config.ini not found")
            self.config = SecureConfig('config.ini')
            print("TradingApp: config.ini loaded (or SecureConfig initialized).") # Early print for debug
        except SystemExit: # If SecureConfig itself raises SystemExit (e.g. file not found)
            raise
        except Exception as e_cfg_load:
            # Basic logging if full logger not set up yet
            logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
            logging.error(f"TradingApp: CRITICAL - Failed to load config.ini: {e_cfg_load}", exc_info=True)
            messagebox.showerror("ข้อผิดพลาดการตั้งค่า", f"ไม่สามารถโหลด config.ini: {e_cfg_load}")
            if master.winfo_exists(): master.destroy()
            raise SystemExit(f"Config load error: {e_cfg_load}")

        # Setup logger AFTER config is loaded (to get log_level and log_file from config)
        try:
            log_level_str = self.config.get('General', 'log_level', fallback='INFO').upper()
            log_level = getattr(logging, log_level_str, logging.INFO)
            log_file_path = self.config.get('General', 'log_file', fallback='logs/trading_app.log')
            # Call the setup function, which now configures the root logger
            setup_logger_for_app(log_file=log_file_path, level=log_level, log_queue_obj_param=log_queue)
            if not logger: # Check if the global logger was successfully assigned
                raise RuntimeError("Global logger was not initialized by setup_logger_for_app.")
            logger.info("TradingApp: Logger setup complete after config load.")
        except Exception as e_log_setup:
            # Fallback logging if logger setup failed
            logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
            logging.error(f"TradingApp: CRITICAL - Error during logger setup: {e_log_setup}", exc_info=True)
            messagebox.showerror("ข้อผิดพลาดการตั้งค่า Logger", f"ไม่สามารถตั้งค่า Logger: {e_log_setup}")
            if master.winfo_exists(): master.destroy()
            raise SystemExit(f"Logger setup error: {e_log_setup}")

        # Validate config AFTER logger is confirmed to be set up
        try:
            self.validate_config()
            logger.info("TradingApp: config.ini validated successfully.")
        except SystemExit: # If validate_config raises SystemExit
            logger.critical("TradingApp: Config validation failed, exiting.")
            raise
        except Exception as e_val:
            logger.error(f"TradingApp: Error during config validation: {e_val}", exc_info=True)
            messagebox.showerror("ข้อผิดพลาดการตั้งค่า", f"เกิดข้อผิดพลาดในการตรวจสอบ config.ini: {e_val}")
            if master.winfo_exists(): master.destroy()
            raise SystemExit(f"Config validation error: {e_val}")


        self.trading_bot: Optional[TradingBot] = None
        self.bot_thread: Optional[threading.Thread] = None
        self.telegram_code_dialog_future: Optional[asyncio.Future] = None # For Telegram code
        self.loop: Optional[asyncio.AbstractEventLoop] = None # Event loop for the bot thread
        self._is_shutting_down: bool = False
        self.settings_window: Optional[tk.Toplevel] = None # To manage settings window

        self._create_widgets()
        self._check_log_queue() # Start polling the log queue
        self.update_status_bar("กำลังเริ่มต้น...", "gray")
        self.update_trading_info_ui() # Initial UI update
        self.update_license_info_on_startup()
        logger.info("TradingApp: UI initialized and ready.")


    def validate_config(self):
        # This method relies on self.config being loaded and logger being available
        if not logger:
            # This is a critical state, should not happen if __init__ order is correct
            print("CRITICAL ERROR in validate_config: Global logger is None!")
            logging.basicConfig(level=logging.CRITICAL, format='[%(asctime)s] [%(levelname)s] %(name)s: %(message)s', datefmt='%H:%M:%S') # Use new format
            logging.critical("Global logger is None during validate_config. This indicates an initialization order problem.")
            messagebox.showerror("ข้อผิดพลาดภายใน", "Logger ไม่ได้เริ่มต้นก่อนการตรวจสอบ Config โปรแกรมไม่สามารถทำงานต่อได้")
            if self.master and self.master.winfo_exists(): self.master.destroy()
            raise SystemExit("Logger not ready for config validation.")

        logger.debug("Validating configuration from config.ini...")
        required = {
            'BYBIT': ['api_key', 'api_secret', 'default_symbol', 'default_leverage',
                      'default_coin', 'category', 'account_type',
                      'default_qty_precision', 'default_price_precision',
                      'max_position_size', # Required even for risk mode as a fallback or reference
                      'position_size_mode', # 'fixed' or 'risk_percentage'
                      'risk_per_trade_percentage', # Required if mode is 'risk_percentage'
                      'take_profit_percentage', 'stop_loss_percentage', 'limit_tp',
                      'enable_breakeven_on_tp1', # True/False
                      'cancel_orders_on_new_signal' # True/False
                      ],
            'Telegram': ['api_id', 'api_hash', 'phone', 'target_chat', 'notification_chat_id'], # notification_chat_id can be same as target_chat
            'LICENSE': ['key', 'server_url'],
            'General': ['log_level', 'log_file', 'trader_mode']
        }
        config_has_section = getattr(self.config, 'has_section', None)

        for section, fields in required.items():
            section_exists_in_config = False
            if callable(config_has_section):
                section_exists_in_config = config_has_section(section)
            elif isinstance(self.config, configparser.ConfigParser): # Fallback for raw ConfigParser
                 section_exists_in_config = self.config.has_section(section)
            else: # Should not happen with SecureConfig
                logger.warning(f"Cannot determine if config object of type {type(self.config)} has section '{section}'. Proceeding with caution.")
                section_exists_in_config = True # Assume exists to check fields

            if not section_exists_in_config:
                msg = f"ไม่พบส่วนที่จำเป็นใน Config: [{section}] ใน config.ini"
                logger.critical(msg)
                messagebox.showerror("ข้อผิดพลาด Config", msg)
                if self.master and self.master.winfo_exists(): self.master.destroy()
                raise SystemExit(f"Missing config section: {section}")

            for field in fields:
                value = self.config.get(section, field, fallback="___MISSING___") # Use a unique fallback
                if value == "___MISSING___" or (isinstance(value, str) and str(value).strip() == ''):
                    # Specific handling for conditionally required fields
                    if field == 'risk_per_trade_percentage' and section == 'BYBIT':
                        mode = self.config.get('BYBIT', 'position_size_mode', fallback='fixed').lower()
                        if mode != 'risk_percentage':
                            logger.debug(f"Optional field [{section}] {field} not present, but not required for mode '{mode}'.")
                            continue # Skip if not required for current mode
                    if field == 'notification_chat_id' and section == 'Telegram' and (value == "___MISSING___" or str(value).strip() == ''):
                        logger.info(f"Optional field [{section}] {field} not present or empty. Notifications will use target_chat if configured, or be disabled.")
                        continue


                    msg = f"ค่า Config ที่จำเป็นหายไปหรือว่างเปล่า: [{section}] {field} ใน config.ini"
                    logger.critical(msg)
                    messagebox.showerror("ข้อผิดพลาด Config", msg)
                    if self.master and self.master.winfo_exists(): self.master.destroy()
                    raise SystemExit(f"Missing config value: {section}.{field}")

                # Type checks (example)
                if field in ['default_leverage', 'api_id', 'limit_tp', 'recv_window',
                             'cache_duration_seconds', 'balance_check_interval_seconds',
                             'default_qty_precision', 'default_price_precision']:
                    try: int(str(value))
                    except ValueError:
                        msg = f"ค่าไม่ถูกต้อง (ต้องเป็นตัวเลขจำนวนเต็ม) สำหรับ [{section}] {field}: '{value}'"
                        logger.critical(msg); messagebox.showerror("ข้อผิดพลาด Config", msg)
                        if self.master and self.master.winfo_exists(): self.master.destroy()
                        raise SystemExit(msg)
                if field in ['max_position_size', 'take_profit_percentage',
                             'stop_loss_percentage', 'risk_per_trade_percentage']: # risk_per_trade_percentage can be float
                     try: float(str(value))
                     except ValueError:
                        # Conditionally ignore if not in risk_percentage mode for risk_per_trade_percentage
                        if field == 'risk_per_trade_percentage' and section == 'BYBIT':
                            mode = self.config.get('BYBIT', 'position_size_mode', fallback='fixed').lower()
                            if mode != 'risk_percentage':
                                continue
                        msg = f"ค่าไม่ถูกต้อง (ต้องเป็นตัวเลขทศนิยม) สำหรับ [{section}] {field}: '{value}'"
                        logger.critical(msg); messagebox.showerror("ข้อผิดพลาด Config", msg)
                        if self.master and self.master.winfo_exists(): self.master.destroy()
                        raise SystemExit(msg)
                if field in ['testnet', 'enable_breakeven_on_tp1', 'cancel_orders_on_new_signal'] and str(value).lower() not in ['true', 'false', 'yes', 'no', '1', '0']:
                    msg = f"ค่าไม่ถูกต้อง (ต้องเป็น True/False) สำหรับ [{section}] {field}: '{value}'"
                    logger.critical(msg); messagebox.showerror("ข้อผิดพลาด Config", msg)
                    if self.master and self.master.winfo_exists(): self.master.destroy()
                    raise SystemExit(msg)
                if field == 'position_size_mode' and section == 'BYBIT' and str(value).lower() not in ['fixed', 'risk_percentage']:
                    msg = f"ค่าไม่ถูกต้องสำหรับ [{section}] {field}: '{value}' (ต้องเป็น 'fixed' หรือ 'risk_percentage')"
                    logger.critical(msg); messagebox.showerror("ข้อผิดพลาด Config", msg)
                    if self.master and self.master.winfo_exists(): self.master.destroy()
                    raise SystemExit(msg)

        logger.info("Configuration validation successful.")


    def _create_widgets(self):
        # --- เมนูหลัก ---
        menubar = tk.Menu(self.master)
        filemenu = tk.Menu(menubar, tearoff=0)
        filemenu.add_command(label="ตั้งค่า", command=self._open_settings_dialog)
        filemenu.add_separator()
        filemenu.add_command(label="ออก", command=self._on_closing)
        menubar.add_cascade(label="ไฟล์", menu=filemenu)

        # --- เพิ่มเมนู Backtest/Report และ Auto Update ---
        toolsmenu = tk.Menu(menubar, tearoff=0)
        toolsmenu.add_command(label="ดูรายงานย้อนหลัง", command=self.show_report)
        toolsmenu.add_command(label="อัปเดตบอท (Auto Update)", command=self.auto_update)
        toolsmenu.add_command(label="อัปเดตบอท (zip)", command=self.auto_update_zip)
        toolsmenu.add_command(label="เช็คเวอร์ชันใหม่", command=self.check_new_version)
        menubar.add_cascade(label="เครื่องมือ", menu=toolsmenu)
        self.master.config(menu=menubar)

        helpmenu = tk.Menu(menubar, tearoff=0)
        helpmenu.add_command(label="เกี่ยวกับ", command=self._show_about)
        menubar.add_cascade(label="ช่วยเหลือ", menu=helpmenu)

        # --- แผงข้อมูลด้านบน ---
        top_info_panel_outer = ttk.Frame(self.master)
        top_info_panel_outer.pack(fill=tk.X, padx=10, pady=(10,5)) # pady top 10, bottom 5

        # Line 1 of Top Info (User Info, License)
        top_info_line1 = ttk.Frame(top_info_panel_outer)
        top_info_line1.pack(fill=tk.X)
        self.user_info_label = ttk.Label(top_info_line1, text="ข้อมูลผู้ใช้:", font=("Arial", 10, "bold"))
        self.user_info_label.pack(side=tk.LEFT, padx=(0,10))
        self.license_label = ttk.Label(top_info_line1, text="วันหมดอายุ: -", font=("Arial", 10))
        self.license_label.pack(side=tk.LEFT, padx=(0, 5))
        self.license_days_remaining_label = ttk.Label(top_info_line1, text="เหลือ: - วัน", font=("Arial", 10))
        self.license_days_remaining_label.pack(side=tk.LEFT, padx=(0,20))

        # Line 2 of Top Info (Platform, Start/Stop, Cancel All)
        top_info_line2 = ttk.Frame(top_info_panel_outer)
        top_info_line2.pack(fill=tk.X, pady=(5,0)) # pady top 5
        platform_selection_label = ttk.Label(top_info_line2, text="เลือก Exchange:", font=("Arial", 10, "bold"))
        platform_selection_label.pack(side=tk.LEFT, padx=(0,5))
        self.platform_combo = ttk.Combobox(top_info_line2, values=["Bybit"], state="readonly", width=12)
        self.platform_combo.set("Bybit")
        self.platform_combo.pack(side=tk.LEFT, padx=(0,10))
        self.start_button = ttk.Button(top_info_line2, text="เริ่ม", command=self._start_bot, width=10)
        self.start_button.pack(side=tk.LEFT, padx=(0,5))
        self.stop_button = ttk.Button(top_info_line2, text="หยุด", command=self._stop_bot, width=10, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=(0,5))
        self.cancel_all_button = ttk.Button(top_info_line2, text="ยกเลิก Order ทั้งหมด", command=self._cancel_all_orders, width=18)
        self.cancel_all_button.pack(side=tk.RIGHT, padx=(5,0))


        # --- กรอบข้อมูลการเทรด ---
        trading_info_outer_frame = ttk.LabelFrame(self.master, text="ข้อมูลการเทรด", padding=5)
        trading_info_outer_frame.pack(fill=tk.X, padx=10, pady=5)

        # Line 1: Balance, Total Orders, Status
        ti_line1 = ttk.Frame(trading_info_outer_frame)
        ti_line1.pack(fill=tk.X, pady=(0,2)) # Small bottom padding
        self.balance_label = ttk.Label(ti_line1, text="ยอดคงเหลือ: N/A (ว่าง: N/A, ใช้ไป: N/A)", font=("Arial", 10))
        self.balance_label.pack(side=tk.LEFT, padx=(0,10))
        self.orders_label = ttk.Label(ti_line1, text="Order ทั้งหมด: 0", font=("Arial", 10, "bold"))
        self.orders_label.pack(side=tk.LEFT, padx=10)
        ti_spacer1 = ttk.Frame(ti_line1) # Spacer
        ti_spacer1.pack(side=tk.LEFT, expand=True, fill=tk.X)
        self.status_label = ttk.Label(ti_line1, text="สถานะ: ยังไม่เชื่อมต่อ", font=("Arial", 10, "bold"))
        self.status_label.pack(side=tk.RIGHT, padx=(10,0))

        # Line 2: Bybit Status, Telegram Status, Runtime
        ti_line2 = ttk.Frame(trading_info_outer_frame)
        ti_line2.pack(fill=tk.X)
        self.bybit_status_label = ttk.Label(ti_line2, text="Bybit: ❌", font=("Arial", 10, "bold"), foreground="red")
        self.bybit_status_label.pack(side=tk.LEFT, padx=(0,10))
        self.telegram_status_label = ttk.Label(ti_line2, text="Telegram: ❌", font=("Arial", 10, "bold"), foreground="red")
        self.telegram_status_label.pack(side=tk.LEFT, padx=10)
        ti_spacer2 = ttk.Frame(ti_line2) # Spacer
        ti_spacer2.pack(side=tk.LEFT, expand=True, fill=tk.X)
        self.runtime_label = ttk.Label(ti_line2, text="เวลาทำงาน: 00:00:00", font=("Arial", 10, "bold"))
        self.runtime_label.pack(side=tk.RIGHT, padx=(10,0))


        # --- PanedWindow หลัก ---
        main_pane = ttk.PanedWindow(self.master, orient=tk.VERTICAL)
        main_pane.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # --- ตารางประวัติการเทรด (ส่วนบน) ---
        trade_history_frame = ttk.LabelFrame(main_pane, text="ประวัติการเทรด", padding=5)
        main_pane.add(trade_history_frame, weight=2) # Give more weight to history

        columns_history = ("time", "symbol", "type", "amount", "price", "order_id", "status", "pnl")
        column_texts_history = ["เวลา", "เหรียญ", "ประเภท", "จำนวน", "ราคา", "Order ID", "สถานะ", "กำไร/ขาดทุน"]
        column_widths_history = [140, 80, 100, 80, 90, 120, 150, 80] # Adjusted status width
        self.trade_history_tree = ttk.Treeview(trade_history_frame, columns=columns_history, show="headings", height=8) # Initial height
        for col, text, width in zip(columns_history, column_texts_history, column_widths_history):
            self.trade_history_tree.heading(col, text=text, anchor=tk.W)
            anchor_val = tk.W
            if col in ["amount", "price", "pnl"]: anchor_val = tk.E # Right align numbers
            elif col == "time" or col == "order_id": anchor_val = tk.CENTER
            self.trade_history_tree.column(col, width=width, anchor=anchor_val, minwidth=50)
        self.trade_history_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar_history = ttk.Scrollbar(trade_history_frame, orient="vertical", command=self.trade_history_tree.yview)
        scrollbar_history.pack(side=tk.RIGHT, fill=tk.Y)
        self.trade_history_tree.configure(yscrollcommand=scrollbar_history.set)
        # Tags for coloring rows
        self.trade_history_tree.tag_configure('entry_placed', background='#E1F5FE') # Light Blue for entry placed
        self.trade_history_tree.tag_configure('tp_placed', background='#FFF9C4') # Light Yellow for TP placed
        self.trade_history_tree.tag_configure('sl_placed', background='#FFEBEE') # Light Pink for SL placed
        self.trade_history_tree.tag_configure('sl_breakeven', background='#E8EAF6') # Indigo light for SL to BE
        self.trade_history_tree.tag_configure('filled_profit', background='#C8E6C9') # Light Green for filled profit
        self.trade_history_tree.tag_configure('filled_loss', background='#FFCDD2') # Light Red for filled loss
        self.trade_history_tree.tag_configure('cancelled', background='#E0E0E0') # Grey for cancelled
        self.trade_history_tree.tag_configure('error', background='#FFAB91') # Light Orange/Red for error
        self.trade_history_tree.tag_configure('system_alert', background='#FFF59D', foreground='black') # Yellow for system alerts


        # --- ส่วนล่าง (Active Positions และ Log) ---
        bottom_pane_container = ttk.Frame(main_pane) # Container for bottom two frames
        main_pane.add(bottom_pane_container, weight=1)

        # --- ตาราง Active Positions ---
        active_positions_frame = ttk.LabelFrame(bottom_pane_container, text="สถานะ Position & Order ปัจจุบัน", padding=5)
        active_positions_frame.pack(fill=tk.BOTH, expand=True, pady=(0,5)) # pady bottom 5
        
        columns_active = ("symbol_pidx", "side_size", "entry", "pnl", "sl", "tp1", "be_applied", "main_order_status", "tp_orders_left")
        column_texts_active = ["เหรียญ (Idx)", "ด้าน | ขนาด", "ราคาเข้า", "กำไร/ขาดทุนสด", "SL ที่ตั้งใจ", "TP1 ที่ตั้งใจ", "BE?", "สถานะ Order หลัก", "TP คงเหลือ"]
        column_widths_active = [100, 100, 90, 90, 90, 90, 60, 120, 80] # Adjusted BE width
        self.active_positions_tree = ttk.Treeview(active_positions_frame, columns=columns_active, show="headings", height=4) # Initial height
        for col, text, width in zip(columns_active, column_texts_active, column_widths_active):
            self.active_positions_tree.heading(col, text=text, anchor=tk.W)
            anchor_val = tk.W
            if col in ["entry", "pnl", "sl", "tp1"]: anchor_val = tk.E # Right align numbers
            self.active_positions_tree.column(col, width=width, anchor=anchor_val, minwidth=40)
        self.active_positions_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar_active = ttk.Scrollbar(active_positions_frame, orient="vertical", command=self.active_positions_tree.yview)
        scrollbar_active.pack(side=tk.RIGHT, fill=tk.Y)
        self.active_positions_tree.configure(yscrollcommand=scrollbar_active.set)
        self.active_positions_tree.tag_configure('long_pos', foreground='green')
        self.active_positions_tree.tag_configure('short_pos', foreground='red')


        # --- กรอบ Log ---
        log_frame = ttk.LabelFrame(bottom_pane_container, text="บันทึกการทำงาน", padding=5)
        log_frame.pack(fill=tk.BOTH, expand=True)
        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, height=5, font=("Consolas", 9)) # Consolas for logs
        self.log_text.pack(fill=tk.BOTH, expand=True)
        self.log_text.config(state=tk.DISABLED) # Log text is read-only
        
        # --- Status Bar ---
        self.status_bar = ttk.Label(self.master, text="พร้อมทำงาน", anchor="w") # Thai "Ready"
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X, padx=10, pady=2)


    def update_license_info_on_startup(self):
        # This method is called after config is loaded
        license_key = self.get_config_value("LICENSE", "key", "N/A")
        # Display the key (or part of it) and a placeholder status
        self.update_license_info(f"{license_key}", "รอตรวจสอบ")


    def _show_about(self):
        messagebox.showinfo("เกี่ยวกับ Bybit Copy Bot", "Bybit Copy Bot | v2.6.1 (ภาษาไทย)\nพัฒนาโดย Tawin") # Updated version and developer

    def _check_log_queue(self):
        """ Periodically check the log queue and display records in the UI """
        try:
            while True: # Process all available records
                record = log_queue.get_nowait()
                self._display_log_record(record)
        except queue.Empty:
            pass # No more records for now
        finally:
            # Reschedule itself if the master window still exists
            if self.master and self.master.winfo_exists():
                self.master.after(100, self._check_log_queue) # Check every 100ms

    def _display_log_record(self, record_str: str):
        """ Displays a single log record string in the ScrolledText widget """
        if hasattr(self, 'log_text') and self.log_text.winfo_exists():
            self.log_text.config(state=tk.NORMAL)
            self.log_text.insert(tk.END, record_str + "\n")
            self.log_text.see(tk.END) # Scroll to the end
            self.log_text.config(state=tk.DISABLED)

    def get_config_value(self, section: str, option: str, default: Any = '') -> Any:
        # Ensure logger is available, even if it's a basic one during early init
        current_logger = logger if logger else logging.getLogger("TradingApp.get_config_value_early")
        try:
            return self.config.get(section, option, fallback=default)
        except (configparser.NoSectionError, configparser.NoOptionError):
            current_logger.warning(f"Config value not found: [{section}] {option}. Returning default: '{default}'")
            return default
        except Exception as e: # Catch any other potential errors from SecureConfig's get
            current_logger.error(f"Error getting config value [{section}] {option}: {e}. Returning default: '{default}'")
            return default

    def _open_settings_dialog(self):
        if self.settings_window and self.settings_window.winfo_exists():
            self.settings_window.lift() # Bring to front if already open
            return

        self.settings_window = tk.Toplevel(self.master)
        self.settings_window.title("ตั้งค่าโปรแกรม")
        self.settings_window.geometry("700x700") # Wider for Thai text
        self.settings_window.transient(self.master) # Keep on top of master
        self.settings_window.grab_set() # Modal behavior
        self.settings_window.resizable(False, False)

        notebook = ttk.Notebook(self.settings_window)
        self.settings_entries = {} # To store StringVars for entries

        # Define settings structure with Thai labels
        settings_structure = {
            "BYBIT": [("api_key", "API Key:"), ("api_secret", "API Secret:"),
                      ("default_symbol", "เหรียญเริ่มต้น (Default Symbol):"), ("default_leverage", "Leverage เริ่มต้น:"),
                      ("default_coin", "เหรียญอ้างอิง (Default Coin):"), ("category", "ประเภทสัญญา (linear/inverse):"),
                      ("account_type", "ประเภทบัญชี (UNIFIED/CONTRACT):"),
                      ("position_size_mode", "โหมดคำนวณขนาด Position (fixed/risk_percentage):"), # New
                      ("max_position_size", "ขนาด Position สูงสุด (หน่วยสินทรัพย์, สำหรับโหมด 'fixed'):"),
                      ("risk_per_trade_percentage", "ความเสี่ยง % ต่อเทรด (สำหรับโหมด 'risk_percentage', เช่น 1.0 สำหรับ 1%):"), # New
                      ("take_profit_percentage", "TP % เริ่มต้น (กรณีไม่มีใน Signal):"),
                      ("stop_loss_percentage", "SL % เริ่มต้น (กรณีไม่มีใน Signal):"),
                      ("limit_tp", "จำนวน TP สูงสุดจาก Signal:"),
                      ("enable_breakeven_on_tp1", "เปิดใช้งาน Break-Even เมื่อถึง TP1 (True/False):"),
                      ("cancel_orders_on_new_signal", "ยกเลิก Order เก่าเมื่อมี Signal ใหม่ (True/False):"),
                      ("default_qty_precision", "ทศนิยม Quantity เริ่มต้น:"),
                      ("default_price_precision", "ทศนิยม Price เริ่มต้น:"),
                      ("recv_window", "Recv Window (ms):"),
                      ("cache_duration_seconds", "API Cache (วินาที):"),
                      ("balance_check_interval_seconds", "ความถี่ตรวจสอบ Balance (วินาที):"),("testnet", "Testnet (True/False):")],
            "Telegram": [("api_id", "API ID:"), ("api_hash", "API Hash:"),
                         ("phone", "เบอร์โทรศัพท์ (+รหัสประเทศ):"), # Thai label
                         ("target_chat", "Chat ID/Username เป้าหมาย (สำหรับรับ Signal):"),
                         ("notification_chat_id", "Chat ID/Username แจ้งเตือน (เหมือน Target Chat ได้):")],
            "LICENSE": [("key", "License Key:"), ("server_url", "URL License Server:")],
            "General": [("log_level", "ระดับ Log (INFO/DEBUG):"),
                        ("log_file", "ที่อยู่ไฟล์ Log:"),
                        ("trader_mode", "โหมดการเทรด (เช่น bybit):")]
        }

        for section_name, fields in settings_structure.items():
            tab = ttk.Frame(notebook, padding=10)
            notebook.add(tab, text=section_name) # Section name can remain English for tab ID
            self._create_settings_group(tab, section_name, fields)

        notebook.pack(expand=True, fill='both', padx=10, pady=10)

        button_frame = ttk.Frame(self.settings_window)
        button_frame.pack(fill=tk.X, padx=10, pady=(0,10), side=tk.BOTTOM) # Place at bottom
        save_button = ttk.Button(button_frame, text="บันทึกการตั้งค่า", command=self._save_settings_from_dialog)
        save_button.pack(side=tk.RIGHT, padx=5)
        cancel_button = ttk.Button(button_frame, text="ยกเลิก", command=self.settings_window.destroy)
        cancel_button.pack(side=tk.RIGHT, padx=5)

    def _create_settings_group(self, parent_tab, section_name, fields):
        for i, (option_key, label_text) in enumerate(fields):
            ttk.Label(parent_tab, text=label_text).grid(row=i, column=0, sticky=tk.W, pady=3, padx=5)
            current_value = self.get_config_value(section_name, option_key, '')
            var = tk.StringVar(value=str(current_value)) # Use StringVar
            entry = ttk.Entry(parent_tab, textvariable=var, width=55) # Wider entry for Thai
            entry.grid(row=i, column=1, sticky=tk.EW, pady=3, padx=5)
            self.settings_entries[f"{section_name}_{option_key}"] = var
        parent_tab.grid_columnconfigure(1, weight=1) # Make entry column expandable

    def _save_settings_from_dialog(self):
        logger.info("Attempting to save settings from dialog...")
        try:
            for key_path, string_var in self.settings_entries.items():
                section, option = key_path.split("_", 1) # Maxsplit = 1
                new_value = string_var.get()

                # Use SecureConfig's set method
                set_method = getattr(self.config, 'set', None)
                has_section_method = getattr(self.config, 'has_section', None)
                add_section_method = getattr(self.config, 'add_section', None) # SecureConfig might not have add_section

                if callable(set_method):
                    # SecureConfig might handle section creation internally or require it to exist
                    if callable(has_section_method) and not has_section_method(section) and callable(add_section_method):
                         add_section_method(section) # This might fail if SecureConfig doesn't support it
                    set_method(section, option, new_value)
                else: # Fallback for raw ConfigParser if type is mixed (should not happen with SecureConfig)
                    if not self.config.has_section(section): self.config.add_section(section)
                    self.config.set(section, option, new_value)
                logger.debug(f"Set [{section}] {option} = {new_value}")

            # Use SecureConfig's save method
            save_method = getattr(self.config, 'save', None)
            if callable(save_method):
                self.config.save()
            else: # Fallback for raw ConfigParser
                with open('config.ini', 'w', encoding='utf-8') as configfile:
                    self.config.write(configfile)

            logger.info("Configuration has been saved successfully.")
            messagebox.showinfo("บันทึกการตั้งค่าแล้ว", "บันทึกการตั้งค่าเรียบร้อย!\nกรุณารีสตาร์ทโปรแกรมเพื่อให้การเปลี่ยนแปลงทั้งหมดมีผล", parent=self.settings_window)
            self.update_status_bar("บันทึกการตั้งค่าแล้ว แนะนำให้รีสตาร์ท", "blue")

            # Update logger level if changed
            new_log_level_str = self.get_config_value('General', 'log_level', 'INFO').upper()
            new_log_level = getattr(logging, new_log_level_str, logging.INFO)
            if logger and logger.getEffectiveLevel() != new_log_level:
                 logger.setLevel(new_log_level);
                 for handler in logger.handlers: handler.setLevel(new_log_level) # Also set for handlers
                 logger.info(f"Log level updated to {new_log_level_str}.")

            self.update_license_info_on_startup() # Re-check/display license info

            if self.settings_window and self.settings_window.winfo_exists():
                self.settings_window.destroy()
                self.settings_window = None
        except Exception as e:
            logger.error(f"Failed to save settings: {e}", exc_info=True)
            messagebox.showerror("เกิดข้อผิดพลาดในการบันทึก", f"ไม่สามารถบันทึกการตั้งค่า: {e}",
                                 parent=self.settings_window if self.settings_window and self.settings_window.winfo_exists() else self.master)


    def _start_bot(self):
        logger.info("TradingApp: _start_bot called")
        if self.trading_bot and self.trading_bot.running:
            logger.info("TradingApp: Bot is already running.")
            messagebox.showinfo("สถานะบอท", "บอทกำลังทำงานอยู่แล้ว") # Thai
            return

        logger.info("TradingApp: Attempting to start bot...")
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        self.update_status_bar("กำลังเริ่มบอท...", "blue") # Thai "Starting bot..."

        try:
            # Re-read config before starting, SecureConfig handles decryption
            reload_method = getattr(self.config, '_load_config', None) # SecureConfig specific
            if callable(reload_method):
                self.config._load_config()
            else: # Fallback for standard ConfigParser
                self.config.read('config.ini', encoding='utf-8')
            self.validate_config() # Re-validate after re-reading
            logger.info("TradingApp: Configuration re-read and validated successfully.")
        except SystemExit: # If validation fails
            logger.critical("TradingApp: Config validation failed during bot start. Aborting.")
            self.start_button.config(state=tk.NORMAL)
            self.stop_button.config(state=tk.DISABLED)
            self.update_status_bar("เริ่มบอทล้มเหลว: Config ผิดพลาด", "red") # Thai
            return
        except Exception as e:
            logger.error(f"TradingApp: Error re-reading/validating config.ini: {e}", exc_info=True)
            messagebox.showerror("ข้อผิดพลาด Config", f"ไม่สามารถอ่าน/ตรวจสอบ config.ini: {e}\nบอทไม่สามารถเริ่มทำงานได้") # Thai
            self.start_button.config(state=tk.NORMAL)
            self.stop_button.config(state=tk.DISABLED)
            return

        logger.info("TradingApp: Creating TradingBot instance.")
        self.trading_bot = TradingBot(config=self.config, telegram_code_callback=self._request_telegram_code_from_ui, app_instance=self)

        logger.info("TradingApp: Starting bot_thread.")
        self.bot_thread = threading.Thread(target=self._run_bot_asyncio_loop, name="TradingBotThread", daemon=True)
        self.bot_thread.start()
        logger.info("TradingApp: bot_thread initiated.")


    def _run_bot_asyncio_loop(self):
        logger.info(f"TradingBotThread: Setting up new asyncio event loop for thread {threading.current_thread().name}")
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        if self.trading_bot:
            self.trading_bot.loop = self.loop # Pass the loop to the trading_bot instance
            try:
                logger.info("TradingBotThread: Running trading_bot.start()")
                self.loop.run_until_complete(self.trading_bot.start())
            except asyncio.CancelledError:
                logger.info("TradingBotThread: Main trading_bot task was cancelled.")
            except Exception as err:
                logger.critical(f"TradingBotThread: Unhandled exception in bot's asyncio loop: {err}", exc_info=True)
                # Schedule UI update on main thread
                if self.master and self.master.winfo_exists():
                    self.master.after(0, lambda e_val=err: self.update_status_bar(f"ข้อผิดพลาดขณะบอททำงาน: {str(e_val)[:50]}", "red")) # Thai
            finally:
                logger.info("TradingBotThread: Asyncio loop's main task finished. Cleaning up loop.")
                if self.loop.is_running():
                    try:
                        # Cancel all remaining tasks in this loop
                        tasks = asyncio.all_tasks(self.loop)
                        for task in tasks:
                            if not task.done() and not task.cancelled():
                                task.cancel()
                        # await asyncio.gather(*tasks, return_exceptions=True) # Optionally wait for cancellations
                    except Exception as e_gather:
                        logger.error(f"TradingBotThread: Error during task gathering on loop cleanup: {e_gather}")

                if not self.loop.is_closed():
                    self.loop.close()
                logger.info("TradingBotThread: Asyncio event loop closed.")
                # Schedule UI update for bot stopped state on the main thread
                if self.master and self.master.winfo_exists(): # Check if UI still exists
                    self.master.after(0, self._bot_stopped_ui_update_on_main_thread)
        else:
            logger.error("TradingBotThread: trading_bot instance is None. Cannot run.")
            if self.master and self.master.winfo_exists(): # Check if UI still exists
                self.master.after(0, self._bot_stopped_ui_update_on_main_thread)


    def _bot_stopped_ui_update_on_main_thread(self):
        logger.info("TradingApp: Bot stopped/failed. Updating UI.")
        if not (self.master and self.master.winfo_exists()): return # Exit if UI is gone

        self.start_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        # Update status labels if they exist
        if hasattr(self, 'telegram_status_label'): self.telegram_status_label.config(text="Telegram: ❌", foreground="red")
        if hasattr(self, 'bybit_status_label'): self.bybit_status_label.config(text="Bybit: ❌", foreground="red")
        if hasattr(self, 'runtime_label'): self.runtime_label.config(text="เวลาทำงาน: 00:00:00") # Thai
        if hasattr(self, 'status_label'): self.status_label.config(text="สถานะ: หยุดทำงาน") # Thai

        if self.trading_bot: # Reset bot's internal state flags if it exists
            self.trading_bot.bybit_connected = False
            self.trading_bot.telegram_connected = False
        self.update_trading_info_ui() # Refresh all trading info
        logger.info("TradingApp: UI updated for bot stopped state.")

        if self._is_shutting_down: # If this stop was part of app shutdown
            logger.info("TradingApp: App shutting down. Destroying UI.")
            self._final_destroy_ui()


    def _stop_bot(self):
        logger.info("TradingApp: Stop button pressed.")
        if self.trading_bot and self.trading_bot.running:
            logger.info("TradingApp: Attempting to stop running bot...")
            self.update_status_bar("กำลังหยุดบอท...", "orange") # Thai "Stopping bot..."
            try:
                if self.loop and self.loop.is_running() and not self.loop.is_closed():
                    logger.info("TradingApp: Scheduling trading_bot.stop() in its event loop.")
                    asyncio.run_coroutine_threadsafe(self.trading_bot.stop(), self.loop)
                else:
                    logger.warning("TradingApp: Bot's loop not active. Forcing UI update for stopped state.")
                    if self.trading_bot: self.trading_bot.running = False # Manually set if loop is dead
                    self._bot_stopped_ui_update_on_main_thread() # Directly update UI
            except Exception as e:
                logger.error(f"TradingApp: Error during bot stop scheduling: {e}", exc_info=True)
                if self.trading_bot: self.trading_bot.running = False # Ensure flag is set
                self._bot_stopped_ui_update_on_main_thread() # Update UI
        else:
            logger.info("TradingApp: Bot not running.")
            messagebox.showinfo("สถานะบอท", "บอทไม่ได้กำลังทำงาน") # Thai
            self.update_status_bar("บอทไม่ได้กำลังทำงาน", "gray") # Thai
            self.start_button.config(state=tk.NORMAL) # Ensure start button is enabled
            self.stop_button.config(state=tk.DISABLED)


    async def _close_all_positions_async(self):
        # This now runs in the bot's asyncio loop
        if not self.trading_bot or not self.trading_bot.bybit_trader or not self.trading_bot.bybit_connected:
            if self.master and self.master.winfo_exists(): # Check UI existence
                messagebox.showwarning("ข้อผิดพลาดการเชื่อมต่อ", "ไม่ได้เชื่อมต่อ Bybit", parent=self.master) # Thai
            self.master.after(0, lambda: self.update_status_bar("ไม่ได้เชื่อมต่อ Bybit", "red")) # Thai
            return

        logger.info("Attempting to close all open positions...")
        self.master.after(0, lambda: self.update_status_bar("กำลังปิด Position ทั้งหมด...", "blue")) # Thai

        try:
            # Assuming bybit_trader has a method like close_all_positions
            await self.trading_bot.bybit_trader.close_all_positions(settleCoin=self.trading_bot.bybit_trader.default_coin)
            await asyncio.sleep(1) # Give some time for orders to process
            if self.trading_bot: await self.trading_bot.update_initial_trading_data() # Refresh data
            
            remaining_pos_count = self.trading_bot.total_positions_count if self.trading_bot else 0
            if remaining_pos_count == 0:
                if self.master and self.master.winfo_exists():
                    messagebox.showinfo("ปิด Position แล้ว", "ปิด Position ทั้งหมดแล้ว/ยืนยันแล้ว", parent=self.master) # Thai
                logger.info("All positions closed.")
                self.master.after(0, lambda: self.update_status_bar("ปิด Position ทั้งหมดแล้ว", "green")) # Thai
            else:
                if self.master and self.master.winfo_exists():
                    messagebox.showwarning("ปิดบางส่วน", f"อาจมี {remaining_pos_count} Position เหลืออยู่", parent=self.master) # Thai
                logger.warning(f"{remaining_pos_count} positions may remain.")
                self.master.after(0, lambda: self.update_status_bar(f"ปิดบางส่วน: {remaining_pos_count} อาจเหลืออยู่", "orange")) # Thai

        except Exception as e_close_pos:
            error_message = str(e_close_pos)[:50] # Truncate for status bar
            logger.error(f"Error closing all positions: {e_close_pos}", exc_info=True)
            if self.master and self.master.winfo_exists():
                messagebox.showerror("ข้อผิดพลาดการปิด Position", f"เกิดข้อผิดพลาด: {e_close_pos}", parent=self.master) # Thai
            self.master.after(0, lambda msg=error_message: self.update_status_bar(f"ข้อผิดพลาดการปิด Position: {msg}", "red")) # Thai


    def _cancel_all_orders(self):
        logger.info("TradingApp: Cancel All Orders pressed.")
        if self.trading_bot and self.trading_bot.running and self.loop and self.loop.is_running():
            if messagebox.askyesno("ยืนยัน", "ต้องการยกเลิก Order ที่เปิดอยู่ทั้งหมดหรือไม่?", parent=self.master): # Thai
                logger.info("TradingApp: Scheduling _cancel_all_orders_async.")
                asyncio.run_coroutine_threadsafe(self._cancel_all_orders_async(), self.loop)
        elif not (self.trading_bot and self.trading_bot.running):
            messagebox.showwarning("บอทไม่ได้ทำงาน", "กรุณาเริ่มบอทเพื่อยกเลิก Order", parent=self.master) # Thai
            self.update_status_bar("บอทไม่ได้ทำงาน", "red") # Thai
        else: # Bot exists but loop isn't running (shouldn't happen if running is true)
            messagebox.showerror("ข้อผิดพลาด", "Loop ของบอทไม่ทำงาน", parent=self.master) # Thai

    async def _cancel_all_orders_async(self):
        # This now runs in the bot's asyncio loop
        if not self.trading_bot or not self.trading_bot.bybit_trader or not self.trading_bot.bybit_connected:
            if self.master and self.master.winfo_exists():
                messagebox.showwarning("ข้อผิดพลาดการเชื่อมต่อ", "ไม่ได้เชื่อมต่อ Bybit", parent=self.master) # Thai
            self.master.after(0, lambda: self.update_status_bar("ไม่ได้เชื่อมต่อ Bybit", "red")) # Thai
            return

        logger.info("Attempting to cancel all open orders...")
        self.master.after(0, lambda: self.update_status_bar("กำลังยกเลิก Order ทั้งหมด...", "blue")) # Thai

        try:
            await self.trading_bot.bybit_trader.cancel_all_orders(settleCoin=self.trading_bot.bybit_trader.default_coin)
            await asyncio.sleep(1) # Give time for cancellations
            if self.trading_bot: await self.trading_bot.update_initial_trading_data() # Refresh data

            remaining_ord_count = self.trading_bot.total_orders_count if self.trading_bot else 0
            if remaining_ord_count == 0:
                if self.master and self.master.winfo_exists():
                    messagebox.showinfo("ยกเลิก Order แล้ว", "ยกเลิก Order ทั้งหมดแล้ว/ยืนยันแล้ว", parent=self.master) # Thai
                logger.info("All orders cancelled.")
                self.master.after(0, lambda: self.update_status_bar("ยกเลิก Order ทั้งหมดแล้ว", "green")) # Thai
            else:
                if self.master and self.master.winfo_exists():
                    messagebox.showwarning("ยกเลิกบางส่วน", f"อาจมี {remaining_ord_count} Order เหลืออยู่", parent=self.master) # Thai
                logger.warning(f"{remaining_ord_count} orders may remain.")
                self.master.after(0, lambda: self.update_status_bar(f"ยกเลิกบางส่วน: {remaining_ord_count} อาจเหลืออยู่", "orange")) # Thai
        except Exception as e_cancel_ord:
            error_message = str(e_cancel_ord)[:50] # Truncate
            logger.error(f"Error cancelling all orders: {e_cancel_ord}", exc_info=True)
            if self.master and self.master.winfo_exists():
                messagebox.showerror("ข้อผิดพลาดการยกเลิก Order", f"เกิดข้อผิดพลาด: {e_cancel_ord}", parent=self.master) # Thai
            self.master.after(0, lambda msg=error_message: self.update_status_bar(f"ข้อผิดพลาดการยกเลิก Order: {msg}", "red")) # Thai


    async def _request_telegram_code_from_ui(self, prompt_type: str = "code") -> Optional[str]:
        logger.info(f"TradingApp: Requesting Telegram {prompt_type} from UI...")
        if not (self.master and self.master.winfo_exists()):
            logger.error("TradingApp: Master window gone. Cannot request Telegram code.")
            return None

        # Ensure we are using the bot's asyncio loop for creating the future
        current_loop = self.loop
        if not (current_loop and current_loop.is_running()):
            logger.warning("TradingApp: Bot's asyncio loop not available or not running. Attempting to get/create one for dialog.")
            try:
                current_loop = asyncio.get_running_loop() # Try to get current if in async context
            except RuntimeError:
                logger.info("TradingApp: No current asyncio loop, creating a new temporary one for dialog.")
                current_loop = asyncio.new_event_loop() # Fallback, less ideal

        self.telegram_code_dialog_future = current_loop.create_future()

        def ask_code_in_main_thread_tk():
            dialog_root = None # To ensure it's defined for finally
            code_received = None
            prompt_message = f"กรุณาใส่รหัสยืนยัน Telegram ({prompt_type}):" # Thai
            dialog_title = f"ต้องการรหัสยืนยัน Telegram ({prompt_type.capitalize()})" # Thai
            try:
                logger.debug(f"TradingApp: ask_code_in_main_thread_tk called for {prompt_type}")
                if not (self.master and self.master.winfo_exists()): # Double check master
                    if not self.telegram_code_dialog_future.done():
                        self.telegram_code_dialog_future.set_result(None) # Resolve future if UI gone
                    return

                # Create a temporary Toplevel for the dialog to ensure it's on top and modal
                self.master.attributes('-topmost', False) # Allow dialog to be on top
                dialog_root = tk.Toplevel(self.master)
                dialog_root.withdraw() # Hide initially to prevent flicker
                dialog_root.title(dialog_title)
                dialog_root.transient(self.master) # Set as transient to master
                dialog_root.grab_set() # Make it modal
                dialog_root.attributes('-topmost', True) # Try to bring to front

                # Center dialog relative to master window
                self.master.update_idletasks() # Ensure master window dimensions are current
                master_x = self.master.winfo_x(); master_y = self.master.winfo_y()
                master_width = self.master.winfo_width(); master_height = self.master.winfo_height()
                dialog_width = 350; dialog_height = 120 # Adjust as needed
                dialog_x = master_x + (master_width - dialog_width) // 2
                dialog_y = master_y + (master_height - dialog_height) // 2
                dialog_root.geometry(f"{dialog_width}x{dialog_height}+{dialog_x}+{dialog_y}")
                dialog_root.deiconify() # Show centered dialog

                code_received = simpledialog.askstring(dialog_title, prompt_message, parent=dialog_root)
                logger.debug(f"TradingApp: {prompt_type.capitalize()} entered: {'******' if code_received and prompt_type=='password' else code_received}")

            except Exception as e_dialog:
                logger.error(f"TradingApp: Error in Telegram {prompt_type} dialog: {e_dialog}", exc_info=True)
                if not self.telegram_code_dialog_future.done():
                    self.telegram_code_dialog_future.set_exception(e_dialog) # Propagate exception
            finally:
                if dialog_root and dialog_root.winfo_exists():
                    dialog_root.destroy() # Clean up dialog window
                if not self.telegram_code_dialog_future.done():
                    self.telegram_code_dialog_future.set_result(code_received) # Set result if not already set

        # Schedule the Tkinter dialog to run in the main thread
        if self.master and self.master.winfo_exists():
            self.master.after(0, ask_code_in_main_thread_tk)
        else: # If UI is already gone
             if not self.telegram_code_dialog_future.done():
                self.telegram_code_dialog_future.set_result(None)

        try:
            code = await self.telegram_code_dialog_future
            if code: logger.info(f"TradingApp: Telegram {prompt_type} received from UI.")
            else: logger.warning(f"TradingApp: No Telegram {prompt_type} entered or dialog cancelled.")
            return code
        except Exception as e_await: # If future had an exception set
            logger.error(f"TradingApp: Failed to get Telegram {prompt_type} from UI future (exception was set): {e_await}", exc_info=False) # Log only message
            return None


    def update_license_info(self, expiry_display_str: str, days_remaining_str: str = "N/A"):
        # This method should be callable from any thread via master.after(0, ...)
        if hasattr(self, 'license_label') and self.license_label.winfo_exists():
            self.license_label.config(text=f"วันหมดอายุ: {expiry_display_str}") # Thai
            # Color coding based on validity (example)
            if "Invalid" in expiry_display_str or "Error" in expiry_display_str or "N/A" in expiry_display_str :
                self.license_label.config(foreground="red")
            else:
                self.license_label.config(foreground="black") # Default/valid color

        if hasattr(self, 'license_days_remaining_label') and self.license_days_remaining_label.winfo_exists():
            self.license_days_remaining_label.config(text=f"เหลือ: {days_remaining_str} วัน") # Thai
            try:
                if days_remaining_str == "N/A" or days_remaining_str == "-" or "รอ" in days_remaining_str or "Error" in days_remaining_str: # Thai "Waiting"
                    self.license_days_remaining_label.config(foreground="orange")
                else:
                    days = int(days_remaining_str)
                    if days <= 0: self.license_days_remaining_label.config(foreground="red")
                    elif days <= 7: self.license_days_remaining_label.config(foreground="orange")
                    else: self.license_days_remaining_label.config(foreground="green")
            except ValueError: # If days_remaining_str is not a number
                self.license_days_remaining_label.config(foreground="orange") # Default to orange if parsing fails


    def update_telegram_status_ui(self, connected: bool):
        logger.debug(f"TradingApp UI: update_telegram_status_ui called with connected={connected}")
        if hasattr(self, 'telegram_status_label') and self.telegram_status_label.winfo_exists():
            if connected:
                self.telegram_status_label.config(text="Telegram: ✅ เชื่อมต่อแล้ว", foreground="green") # Thai
            else:
                self.telegram_status_label.config(text="Telegram: ❌ ยังไม่เชื่อมต่อ", foreground="red") # Thai

    def update_bybit_status_ui(self, connected: bool):
        logger.debug(f"TradingApp UI: update_bybit_status_ui called with connected={connected}")
        if hasattr(self, 'bybit_status_label') and self.bybit_status_label.winfo_exists():
            if connected:
                self.bybit_status_label.config(text="Bybit: ✅ เชื่อมต่อแล้ว", foreground="green") # Thai
            else:
                self.bybit_status_label.config(text="Bybit: ❌ ยังไม่เชื่อมต่อ", foreground="red") # Thai

    def update_runtime_ui(self, runtime_str: str):
        if hasattr(self, 'runtime_label') and self.runtime_label.winfo_exists():
            self.runtime_label.config(text=f"เวลาทำงาน: {runtime_str}") # Thai

    def update_status_bar(self, message: str, color: str ="black"):
        if hasattr(self, 'status_bar') and self.status_bar.winfo_exists():
            self.status_bar.config(text=message, foreground=color)
        logger.debug(f"Status Bar Updated: {message} (Color: {color})") # Keep debug log in English

    def update_trading_info_ui(self):
        # This method is called to update UI elements with data from self.trading_bot
        if not (self.master and self.master.winfo_exists()): return # Exit if UI is gone
        logger.debug("TradingApp: update_trading_info_ui called")

        bal_str, avail_str, margin_str, orders_str = "N/A", "N/A", "N/A", "N/A"

        if self.trading_bot: # If bot instance exists
            balance_val = self.trading_bot.current_balance
            available_val = self.trading_bot.current_available_balance
            margin_val = self.trading_bot.current_margin
            total_orders = self.trading_bot.total_orders_count

            logger.debug(f"TradingApp UI Update Data: Bal={balance_val}, Avail={available_val}, Margin={margin_val}, Orders={total_orders}, BybitConn={self.trading_bot.bybit_connected}, BotRun={self.trading_bot.running}")

            bal_str = f"{balance_val:.2f}" if isinstance(balance_val, (float, int)) else str(balance_val)
            avail_str = f"{available_val:.2f}" if isinstance(available_val, (float, int)) else str(available_val)
            margin_str = f"{margin_val:.2f}" if isinstance(margin_val, (float, int)) else str(margin_val)
            orders_str = str(total_orders)

            balance_text = f"ยอดคงเหลือ: {bal_str} USDT (ว่าง: {avail_str} USDT, ใช้ไป: {margin_str} USDT)" # Thai
            if hasattr(self, 'balance_label'): self.balance_label.config(text=balance_text)
            if hasattr(self, 'orders_label'): self.orders_label.config(text=f"Order ทั้งหมด: {orders_str}") # Thai

            current_status_text = "สถานะ: ยังไม่เชื่อมต่อ" # Default Thai
            if self.trading_bot.running:
                if self.trading_bot.bybit_connected:
                    current_status_text = "สถานะ: เชื่อมต่อแล้ว (บอททำงาน)" # Thai
                else:
                    current_status_text = "สถานะ: กำลังเชื่อมต่อ..." # Thai
            elif self.trading_bot.bybit_connected: # Bot not running but Bybit is connected
                current_status_text = "สถานะ: เชื่อมต่อแล้ว (บอทหยุด)" # Thai
            else: # Bot not running, Bybit not connected
                current_status_text = "สถานะ: หยุดทำงาน" # Thai
            if hasattr(self, 'status_label'): self.status_label.config(text=current_status_text)
            if hasattr(self, 'runtime_label'): self.runtime_label.config(text=f"เวลาทำงาน: {self.trading_bot.get_runtime_str()}") # Thai

        else: # trading_bot is None
            logger.debug("TradingApp UI Update: trading_bot is None, setting UI to N/A or defaults.")
            if hasattr(self, 'balance_label'): self.balance_label.config(text="ยอดคงเหลือ: N/A (ว่าง: N/A, ใช้ไป: N/A)") # Thai
            if hasattr(self, 'orders_label'): self.orders_label.config(text="Order ทั้งหมด: N/A") # Thai
            if hasattr(self, 'status_label'): self.status_label.config(text="สถานะ: ยังไม่เชื่อมต่อ") # Thai
            if hasattr(self, 'runtime_label'): self.runtime_label.config(text="เวลาทำงาน: 00:00:00") # Thai
            # Ensure connection status labels are also reset if bot is None
            if hasattr(self, 'bybit_status_label'): self.bybit_status_label.config(text="Bybit: ❌ ยังไม่เชื่อมต่อ", foreground="red") # Thai
            if hasattr(self, 'telegram_status_label'): self.telegram_status_label.config(text="Telegram: ❌ ยังไม่เชื่อมต่อ", foreground="red") # Thai

        # Update tables regardless of bot state (they will clear if no data)
        self._update_orders_positions_table() # Renamed from _update_trade_history_table
        self._update_active_positions_table() # New method for active positions
        logger.debug("TradingApp: update_trading_info_ui finished.")


    def _update_orders_positions_table(self): # Combined name for clarity
        if not hasattr(self, 'trade_history_tree') or not self.trade_history_tree.winfo_exists(): return
        # Clear existing items
        for item in self.trade_history_tree.get_children():
            self.trade_history_tree.delete(item)

        if self.trading_bot and hasattr(self.trading_bot, 'get_formatted_trade_history'):
            trade_history_data: List[Tuple[str, str, str, str, str, str]] = self.trading_bot.get_formatted_trade_history()
            logger.debug(f"Updating trade history table with {len(trade_history_data)} items.")

            for item_tuple in trade_history_data:
                if len(item_tuple) == 6: # Ensure tuple has expected number of elements
                    time_val, symbol_val, type_combined_val, amount_val, result_display_val, pnl_val = item_tuple
                    # Further parse result_display_val to extract status, price, order_id if embedded
                    status_val = str(result_display_val)
                    price_val = "N/A" # Default
                    order_id_short_val = "N/A" # Default

                    # Attempt to extract Order ID like (...123456)
                    id_match = re.search(r"\(ID: \.\.\.(.+?)\)", result_display_val)
                    if id_match:
                        order_id_short_val = f"...{id_match.group(1)}"
                        status_val = status_val.replace(id_match.group(0), "").strip() # Remove from status

                    # Attempt to extract price like @12345.67
                    price_match = re.search(r"@\s*([\d\.]+)", result_display_val)
                    if price_match:
                        price_val = price_match.group(1)
                        status_val = status_val.replace(price_match.group(0), "").strip() # Remove from status
                    
                    # Attempt to extract notes like (Some notes)
                    notes_match = re.search(r"\(([^)]+)\)$", status_val) # Notes at the end in parens
                    notes_val = ""
                    if notes_match:
                        notes_val = notes_match.group(1)
                        status_val = status_val.replace(f"({notes_val})", "").strip() # Remove from status
                    
                    status_val = status_val.strip() # Clean up status

                    self.add_trade_history(
                        time_str=time_val, symbol=symbol_val, order_type_display=type_combined_val,
                        amount_str=amount_val, status_str=status_val, price_str=price_val,
                        order_id_str=order_id_short_val, profit_str=pnl_val, notes_str=notes_val
                    )
                else:
                    logger.warning(f"Trade history item from get_formatted_trade_history has incorrect format: {item_tuple}, expected 6 items.")

        # เพิ่ม log กำไร/ขาดทุนล่าสุดแบบภาษาไทย
        if self.trading_bot and hasattr(self.trading_bot, 'get_formatted_trade_history'):
            trade_history_data: List[Tuple[str, str, str, str, str, str]] = self.trading_bot.get_formatted_trade_history()
            if trade_history_data:
                last_trade = trade_history_data[0]
                if len(last_trade) == 6:
                    time_val, symbol_val, type_combined_val, amount_val, result_display_val, pnl_val = last_trade
                    if pnl_val not in ("N/A", "0.00", "0"):
                        ui_status_log(f"กำไร/ขาดทุนล่าสุด: {pnl_val} USDT ({symbol_val})")


    def _update_active_positions_table(self):
        if not hasattr(self, 'active_positions_tree') or not self.active_positions_tree.winfo_exists(): return
        # Clear existing items
        for item in self.active_positions_tree.get_children():
            self.active_positions_tree.delete(item)

        if self.trading_bot and hasattr(self.trading_bot, 'get_active_positions_display_data'):
            active_positions_data: List[Dict[str, Any]] = self.trading_bot.get_active_positions_display_data()
            logger.debug(f"Updating active positions table with {len(active_positions_data)} items.")

            for item_dict in active_positions_data:
                # Prepare values for the treeview from the item_dict
                symbol_pidx = f"{item_dict.get('symbol', 'N/A')} ({item_dict.get('pos_idx', 'N/A')})"
                side_size = f"{item_dict.get('side', 'N/A')} | {item_dict.get('size', 'N/A')}"
                entry = str(item_dict.get('entry_price', 'N/A'))
                pnl = f"{float(item_dict.get('unrealised_pnl', '0')):.2f}" if item_dict.get('unrealised_pnl', '0') != 'N/A' else 'N/A'
                sl = str(item_dict.get('intended_sl', 'N/A'))
                tp1 = str(item_dict.get('intended_tp1', 'N/A'))
                be_applied = str(item_dict.get('breakeven_applied', 'N/A')) # Should be Yes/No from bot
                main_order_status = str(item_dict.get('main_order_status', 'N/A'))
                tp_orders_left = str(item_dict.get('remaining_tp_orders', 'N/A')) # Example key
                
                tag_to_apply = 'long_pos' if item_dict.get('side', '').upper() == 'BUY' else ('short_pos' if item_dict.get('side', '').upper() == 'SELL' else '')

                values = (symbol_pidx, side_size, entry, pnl, sl, tp1, be_applied, main_order_status, tp_orders_left)
                try:
                    if self.master and self.master.winfo_exists(): # Check UI
                        self.active_positions_tree.insert('', 'end', values=values, tags=(tag_to_apply,))
                except tk.TclError as e: # Catch Tcl errors if UI is misbehaving
                    logger.error(f"TradingApp: TclError inserting into active_positions_tree: {e}.")
                except Exception as e_gen:
                    logger.error(f"TradingApp: Generic error inserting into active_positions_tree: {e_gen}", exc_info=True)


    def _on_closing(self):
        logger.info("TradingApp: WM_DELETE_WINDOW.")
        if messagebox.askokcancel("ออก", "คุณต้องการออกจากโปรแกรมหรือไม่?", parent=self.master): # Thai
            logger.info("TradingApp: User confirmed quit.")
            self._is_shutting_down = True # Signal that shutdown is intentional

            # Destroy settings window if it exists
            if self.settings_window and self.settings_window.winfo_exists():
                logger.debug("TradingApp: Destroying settings window.")
                self.settings_window.destroy()
                self.settings_window = None

            if self.trading_bot and self.trading_bot.running:
                logger.info("TradingApp: Bot running. Requesting stop.")
                self.update_status_bar("กำลังปิดโปรแกรม - หยุดการทำงานบอท...", "orange") # Thai
                self._stop_bot() # This will eventually call _bot_stopped_ui_update_on_main_thread
                                 # which will then call _final_destroy_ui if _is_shutting_down is True
            else:
                logger.info("TradingApp: Bot not running. Proceeding to final UI destroy.")
                self.update_status_bar("กำลังปิดโปรแกรม...", "gray") # Thai
                self._final_destroy_ui()
        else:
            logger.info("TradingApp: Quit cancelled.")


    def _final_destroy_ui(self):
        logger.info("TradingApp: _final_destroy_ui called.")
        if logger: # Check if logger itself still exists
            logger.info("TradingApp: Closing log handlers.")
            # Close and remove all handlers from the root logger
            for handler in logging.getLogger().handlers[:]:
                try:
                    handler.close()
                    logging.getLogger().removeHandler(handler)
                except Exception as h_e:
                    print(f"ERROR: Exception closing/removing log handler: {h_e}") # Use print as logger might be gone

        if self.master and self.master.winfo_exists():
            logger.info("TradingApp: Destroying master window.")
            self.master.destroy()
            self.master = None # Ensure it's marked as destroyed
        else:
            logger.info("TradingApp: Master window already destroyed or not initialized.")
        print("TradingApp: Application shutdown complete.") # Final message to console


    def add_trade_history(self, time_str: str, symbol: str, order_type_display: str,
                          amount_str: str, status_str: str, price_str: str,
                          order_id_str: str, profit_str: str, notes_str: Optional[str] = None): # Added notes_str
        if not hasattr(self, 'trade_history_tree') or not self.trade_history_tree.winfo_exists():
            return

        tag_to_apply = 'pending' # Default tag
        status_lower = status_str.lower()
        profit_val_for_tag = 0.0
        try:
            profit_val_for_tag = float(profit_str)
        except (ValueError, TypeError):
            pass # Keep 0.0 if PnL is not a valid float

        # Determine tag based on status
        if "placed" in status_lower or "ส่งคำสั่ง" in status_lower : # Thai "order sent"
            if "entry" in status_lower or "เข้า" in status_lower: tag_to_apply = 'entry_placed' # Thai "entry"
            elif "tp" in status_lower: tag_to_apply = 'tp_placed'
            elif "sl" in status_lower: tag_to_apply = 'sl_placed'
        elif "sl to break-even" in status_lower or "sl ไปจุดคุ้มทุน" in status_lower: # Thai "SL to breakeven"
            tag_to_apply = 'sl_breakeven'
        elif "filled" in status_lower or "closed" in status_lower or "ปิดแล้ว" in status_lower: # Thai "closed"
            tag_to_apply = 'filled_profit' if profit_val_for_tag >=0 else 'filled_loss'
        elif "cancelled" in status_lower or "ยกเลิก" in status_lower: # Thai "cancel"
            tag_to_apply = 'cancelled'
        elif "error" in status_lower or "fail" in status_lower or "alert" in status_lower or "ผิดพลาด" in status_lower: # Thai "error"
            if "critical" in status_lower or "ร้ายแรง" in status_lower: tag_to_apply = 'system_alert' # Thai "critical"
            else: tag_to_apply = 'error'


        # Ensure Order ID is displayed consistently (e.g., last 6 chars)
        display_order_id = str(order_id_str)
        if display_order_id != "N/A" and not display_order_id.startswith("..."): # Avoid re-shortening
            display_order_id = f"...{display_order_id[-6:]}" if len(display_order_id) > 6 else display_order_id
        
        full_status_display = status_str
        if notes_str and notes_str.strip(): # Append notes if they exist
            full_status_display += f" ({notes_str.strip()})"


        values = (
            str(time_str), str(symbol), str(order_type_display), str(amount_str),
            str(price_str), display_order_id, full_status_display, str(profit_str) # Use full_status_display
        )
        try:
            if self.master and self.master.winfo_exists(): # Check UI
                self.trade_history_tree.insert('', 0, values=values, tags=(tag_to_apply,)) # Insert at the top
        except tk.TclError as e: # Catch Tcl errors if UI is misbehaving
            logger.error(f"TradingApp: TclError inserting into trade_history_tree: {e}.")
        except Exception as e_gen:
            logger.error(f"TradingApp: Generic error inserting into trade_history_tree: {e_gen}", exc_info=True)


# --- เพิ่มฟังก์ชัน log ภาษาไทยสำหรับ UI ---
def ui_status_log(msg: str):
    logging.info(f"[THAI] {msg}")

    def show_report(self):
        import json
        import os
        from tkinter import Toplevel, Label
        # อ่าน trade_history.json
        trade_file = os.path.join("state", "trade_history.json")
        if not os.path.exists(trade_file):
            messagebox.showinfo("รายงานย้อนหลัง", "ยังไม่มีข้อมูลการเทรดย้อนหลัง")
            return
        with open(trade_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not data:
            messagebox.showinfo("รายงานย้อนหลัง", "ยังไม่มีข้อมูลการเทรดย้อนหลัง")
            return
        df = pd.DataFrame(data)
        pnl_sum = df['pnl'].astype(float).sum()
        win_count = (df['pnl'].astype(float) > 0).sum()
        loss_count = (df['pnl'].astype(float) < 0).sum()
        total = len(df)
        winrate = (win_count / total * 100) if total > 0 else 0
        cum_pnl = df['pnl'].astype(float).cumsum()
        drawdown = (cum_pnl.cummax() - cum_pnl).max()
        # แสดง popup
        popup = Toplevel(self.master)
        popup.title("รายงานย้อนหลัง")
        popup.geometry("400x250")
        Label(popup, text=f"กำไร/ขาดทุนรวม: {pnl_sum:.2f} USDT", font=("Arial", 12, "bold")).pack(pady=10)
        Label(popup, text=f"Winrate: {winrate:.2f}% ({win_count} / {total})", font=("Arial", 12)).pack(pady=5)
        Label(popup, text=f"Drawdown สูงสุด: {drawdown:.2f} USDT", font=("Arial", 12)).pack(pady=5)
        Label(popup, text=f"จำนวนไม้ที่ขาดทุน: {loss_count}", font=("Arial", 12)).pack(pady=5)
        Label(popup, text=f"จำนวนไม้ที่ชนะ: {win_count}", font=("Arial", 12)).pack(pady=5)
        # ... เพิ่มเติมได้ ...

    def auto_update(self):
        try:
            subprocess.check_call(['git', 'pull'])
            messagebox.showinfo("Auto Update", "อัปเดตโค้ดสำเร็จ! รีสตาร์ทโปรแกรมใหม่")
            os.execv(sys.executable, ['python'] + sys.argv)
        except Exception as e:
            messagebox.showerror("Auto Update", f"อัปเดตไม่สำเร็จ: {e}")

    def auto_update_zip(self):
        try:
            zip_url = "https://github.com/yourusername/yourrepo/archive/refs/heads/main.zip"  # TODO: เปลี่ยน url ให้ตรง repo จริง
            r = requests.get(zip_url)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            for member in z.namelist():
                if any(skip in member for skip in ['config.ini', 'state/', 'logs/']):
                    continue
                z.extract(member, ".")
            messagebox.showinfo("Auto Update", "อัปเดตไฟล์สำเร็จ! รีสตาร์ทโปรแกรมใหม่")
            os.execv(sys.executable, ['python'] + sys.argv)
        except Exception as e:
            messagebox.showerror("Auto Update", f"อัปเดตไม่สำเร็จ: {e}")

    def check_new_version(self):
        local_version = "2.6.1"  # หรืออ่านจากไฟล์ version.txt
        try:
            version_url = "https://raw.githubusercontent.com/yourusername/yourrepo/main/version.txt"  # TODO: เปลี่ยน url ให้ตรง repo จริง
            r = requests.get(version_url, timeout=5)
            latest_version = r.text.strip()
            if latest_version > local_version:
                messagebox.showinfo("เช็คเวอร์ชัน", f"มีเวอร์ชันใหม่: {latest_version}\nกรุณาอัปเดตบอท")
            else:
                messagebox.showinfo("เช็คเวอร์ชัน", "คุณใช้เวอร์ชันล่าสุดแล้ว")
        except Exception as e:
            messagebox.showerror("เช็คเวอร์ชัน", f"เช็คเวอร์ชันไม่สำเร็จ: {e}")


if __name__ == "__main__":
    # Ensure logs directory exists
    if not os.path.exists('logs'):
        try:
            os.makedirs('logs')
        except OSError as e:
            print(f"CRITICAL: Could not create logs directory: {e}") # Use print as logger might not be set
            sys.exit(1)

    # Basic config for initial messages before full logger setup in TradingApp
    # This will be overridden by setup_logger_for_app once TradingApp initializes
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(name)s: %(message)s', datefmt='%H:%M:%S')

    root = None # Initialize root to None for finally block
    try:
        root = tk.Tk()
        app = TradingApp(root) # This will set up the full logger
        if app.master and app.master.winfo_exists(): # Check if app and master window are valid
            root.mainloop()
    except SystemExit as se:
        # Use logger if available, otherwise print
        log_target = logger if logger else logging.getLogger("SystemExitHandler")
        log_target.info(f"Application exiting due to SystemExit: {se}")
    except Exception as main_e:
        log_target = logger if logger else logging.getLogger("MAIN_ERROR")
        log_target.critical(f"Critical unhandled error in __main__: {main_e}", exc_info=True)
        if root and root.winfo_exists(): # Check if root was created and still exists
            try:
                messagebox.showerror("ข้อผิดพลาดร้ายแรง", f"เกิดข้อผิดพลาดร้ายแรง: {main_e}\n\nกรุณาตรวจสอบ Log\nโปรแกรมจะปิดตัวลง") # Thai
            except Exception as mb_error: # If messagebox itself fails
                print(f"ERROR: Could not display critical error messagebox: {mb_error}")
            root.destroy() # Attempt to close UI
    finally:
        final_message = "Application __main__ block finished."
        if logger: logger.info(final_message)
        else: print(final_message)

        # Attempt to close any remaining handlers on the root logger
        # This is a best-effort cleanup
        for handler in logging.getLogger().handlers[:]:
            try:
                if handler and hasattr(handler, 'close'):
                    handler.close()
                logging.getLogger().removeHandler(handler)
            except: # Catch all exceptions during this final cleanup
                pass
        print("Application has shut down.")

