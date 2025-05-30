import logging
import asyncio
import re
from datetime import datetime
import pytz
import json
import os
from typing import Optional, Any, Union
from pyrogram import Client, filters, enums
from pyrogram.handlers import MessageHandler
from pyrogram.errors import (
    FloodWait, RPCError, AuthBytesInvalid, PhoneNumberInvalid,
    PhoneCodeEmpty, PhoneCodeInvalid, SessionPasswordNeeded,
    PasswordHashInvalid, UserDeactivated, SessionExpired, PeerIdInvalid, UserNotParticipant, AuthKeyUnregistered, UserAlreadyParticipant,
    ChannelInvalid, ChannelPrivate # Added ChannelInvalid and ChannelPrivate
)
from signal_parser import SignalParser, TradingSignal
from pyrogram.types import User

import tkinter.messagebox as messagebox

logger = logging.getLogger(__name__)
THAILAND_TZ = pytz.timezone('Asia/Bangkok')

class TelegramBot:
    def __init__(self, config, bybit_trader=None, code_request_callback=None, app_instance=None, trading_bot_instance=None):
        self.config = config
        self.bybit_trader = bybit_trader
        self.code_request_callback = code_request_callback
        self._app_instance = app_instance
        self._trading_bot_instance = trading_bot_instance
        self.signal_parser = SignalParser()
        self._stop_trigger_event = asyncio.Event()

        self.client: Optional[Client] = None
        self.telegram_connected: bool = False
        self._message_handler_task: Optional[asyncio.Task] = None

        self.api_id = self.config.getint('Telegram', 'api_id')
        self.api_hash = self.config.get('Telegram', 'api_hash')
        self.phone_number_config = self.config.get('Telegram', 'phone')

        self.target_chat_id_numeric: Optional[int] = None
        self.target_chat_str: Optional[str] = self.config.get('Telegram', 'target_chat', fallback=None)
        self.notification_chat_id: Optional[Union[int, str]] = self.config.get('Telegram', 'notification_chat_id', fallback=self.target_chat_str)

        if self.target_chat_str:
            try:
                # Try to parse as numeric ID first if it starts with '-'
                if str(self.target_chat_str).startswith('-'):
                    self.target_chat_id_numeric = int(self.target_chat_str)
                logger.info(f"Telegram target_chat configured as: '{self.target_chat_str}' (Numeric if parsed: {self.target_chat_id_numeric})")
            except ValueError:
                logger.warning(f"Target chat '{self.target_chat_str}' could not be parsed as a valid numeric ID. Will be treated as username/link if not numeric.")
        else:
            logger.warning("Telegram target_chat is not configured. Bot will listen to private messages only for signals.")

        if not self.notification_chat_id:
            logger.warning("Telegram notification_chat_id is not configured. Bot will not send notifications.")
        else:
            try:
                if isinstance(self.notification_chat_id, str) and (self.notification_chat_id.startswith('-') or self.notification_chat_id.isdigit()):
                    self.notification_chat_id = int(self.notification_chat_id)
            except ValueError:
                pass
            logger.info(f"Telegram notifications will be sent to: '{self.notification_chat_id}'")

        self.session_name = self.phone_number_config.replace('+', '')
        self.app_data_path = "telegram_sessions"
        if not os.path.exists(self.app_data_path):
            try:
                os.makedirs(self.app_data_path)
                logger.info(f"Created telegram_sessions directory at: {self.app_data_path}")
            except OSError as e:
                logger.error(f"Failed to create session dir '{self.app_data_path}': {e}. Using current directory.")
                self.app_data_path = "."
        # Check write permission for session dir
        try:
            testfile = os.path.join(self.app_data_path, "_test_write.tmp")
            with open(testfile, "w") as f:
                f.write("test")
            os.remove(testfile)
            logger.info(f"Write permission to session dir '{self.app_data_path}' OK.")
        except Exception as e:
            logger.error(f"Cannot write to session dir '{self.app_data_path}'. Please check folder permissions! Error: {e}")
        logger.info(f"TelegramBot initialized. Session name: {self.session_name} in workdir: {self.app_data_path}.")
        session_file_path = os.path.join(self.app_data_path, f"{self.session_name}.session")
        if os.path.exists(session_file_path):
            logger.info(f"Session file found: {session_file_path} (will reuse for login)")
        else:
            logger.warning(f"Session file NOT found: {session_file_path}. You will need to login/OTP on first run.")

    async def send_message_async(self, message_text: str, chat_id_override: Optional[Union[int, str]] = None):
        target_chat_to_send = chat_id_override if chat_id_override else self.notification_chat_id
        if not self.client or not self.client.is_connected or not target_chat_to_send:
            logger.warning(f"Telegram client not connected or no target chat for notification. Message not sent: {message_text[:50]}")
            return False
        try:
            chat_to_use: Union[int, str]
            if isinstance(target_chat_to_send, str) and target_chat_to_send.startswith('@'):
                chat_to_use = target_chat_to_send.lstrip('@')
            else:
                try:
                    chat_to_use = int(str(target_chat_to_send))
                except ValueError:
                    chat_to_use = str(target_chat_to_send)
            await self.client.send_message(chat_id=chat_to_use, text=message_text)
            logger.info(f"Sent Telegram notification to '{chat_to_use}': {message_text[:70]}")
            return True
        except Exception as e:
            logger.error(f"Failed to send Telegram notification to '{target_chat_to_send}': {e}", exc_info=True)
            return False

    async def run(self):
        logger.info("TelegramBot.run: called")
        self._stop_trigger_event.clear()
        try:
            self.client = Client(
                name=self.session_name, api_id=self.api_id, api_hash=self.api_hash,
                phone_number=self.phone_number_config, workdir=self.app_data_path,
            )
            logger.info(f"Pyrogram Client instantiated for session '{self.session_name}'. Session file: {os.path.join(self.app_data_path, f'{self.session_name}.session')}")
        except Exception as e_init_client:
            logger.error(f"Fatal error instantiating Pyrogram Client: {e_init_client}", exc_info=True)
            self._update_ui_telegram_status(False)
            self._show_ui_messagebox("Telegram Critical Error", f"Client init failed: {e_init_client}. Bot cannot start.")
            return

        try:
            chat_identifier_for_handler_and_get_chat: Any
            if self.target_chat_id_numeric is not None:
                chat_identifier_for_handler_and_get_chat = self.target_chat_id_numeric
                logger.info(f"Primary target chat identifier set to Numeric ID: {chat_identifier_for_handler_and_get_chat}")
            elif self.target_chat_str:
                chat_identifier_for_handler_and_get_chat = self.target_chat_str.lstrip('@')
                logger.info(f"Primary target chat identifier set to String (Username/Link): {chat_identifier_for_handler_and_get_chat}")
            else:
                chat_identifier_for_handler_and_get_chat = filters.private
                logger.info("Primary target chat identifier set to Private DMs (no target_chat in config).")

            if chat_identifier_for_handler_and_get_chat:
                 self.client.add_handler(MessageHandler(self._message_handler, filters.chat(chat_identifier_for_handler_and_get_chat) if chat_identifier_for_handler_and_get_chat != filters.private else filters.private))
                 logger.info(f"Added message handler for target: {chat_identifier_for_handler_and_get_chat}")
            else:
                 logger.error("No valid target for message handler after processing config. This is unexpected.")


            self._update_ui_telegram_status(False)
            logger.info("TelegramBot: Attempting to connect client...")
            await self.client.connect()

            if not self.client or not self.client.is_connected:
                logger.error("TelegramBot: Failed to connect to Telegram or client is None.")
                self._update_ui_telegram_status(False)
                self._show_ui_messagebox("Telegram Connection Error", "Could not connect to Telegram.\n\nกรุณาตรวจสอบอินเทอร์เน็ตและ session file ใน telegram_sessions ว่ามีสิทธิ์เขียนไฟล์หรือไม่\nถ้า session file หายจะต้อง login ใหม่ทุกครั้ง")
                if self.client: await self._stop_client_safely()
                return

            if self.client.me is None: 
                logger.info("User not authorized (self.client.me is None after connect). Starting sign-in process.")
                phone_code_hash_val = ""
                try:
                    logger.info(f"Requesting OTP for phone: {self.phone_number_config}")
                    sent_code_info = await self.client.send_code(self.phone_number_config)
                    phone_code_hash_val = sent_code_info.phone_code_hash
                    logger.info(f"OTP request sent. Phone code hash: {phone_code_hash_val[:10]}...")

                    if not self.code_request_callback:
                        logger.error("code_request_callback is not set. Cannot ask for OTP via UI.")
                        await self._stop_client_safely()
                        return
                    phone_code_from_user = await self.code_request_callback(prompt_type="code")
                    if not phone_code_from_user:
                        logger.error("User did not provide OTP. Sign-in cancelled.")
                        self._show_ui_messagebox("Telegram Auth Error", "OTP not provided. Sign-in cancelled.")
                        await self._stop_client_safely()
                        return

                    logger.info(f"Attempting to sign in with OTP: '{phone_code_from_user[:1]}***{phone_code_from_user[-1:]}' and hash: '{phone_code_hash_val[:10]}...'")
                    signed_in_user_object = await self.client.sign_in(
                        phone_number=self.phone_number_config,
                        phone_code_hash=phone_code_hash_val,
                        phone_code=phone_code_from_user
                    )
                    if signed_in_user_object and isinstance(signed_in_user_object, User):
                        self.client.me = signed_in_user_object
                        logger.info(f"Assigned returned User object to self.client.me. ID: {self.client.me.id}")

                    if not self.client.me: 
                        logger.error(f"Post OTP sign-in check failed: self.client.me is still None. OTP might be incorrect or session issue.\n\nถ้า OTP ถูกต้องแต่ยังเข้าไม่ได้ ให้ลบ session file ที่ {os.path.join(self.app_data_path, f'{self.session_name}.session')} แล้ว RESTART BOT")
                        self._show_ui_messagebox("Telegram Auth Error", f"OTP Sign-in failed. Please double-check OTP. If correct, delete the session file at {os.path.join(self.app_data_path, f'{self.session_name}.session')} and RESTART THE BOT.")
                        await self._stop_client_safely()
                        return
                    logger.info(f"Successfully signed in as {getattr(self.client.me, 'username', getattr(self.client.me, 'id', 'Unknown User'))}. self.client.me is populated.")

                except SessionPasswordNeeded:
                    logger.info("Telegram session needs 2FA password.")
                    if not self.code_request_callback:
                        logger.error("code_request_callback is not set for 2FA password.")
                        await self._stop_client_safely()
                        return
                    password_from_user = await self.code_request_callback(prompt_type="password")
                    if not password_from_user:
                        logger.error("User did not provide 2FA password.")
                        self._show_ui_messagebox("Telegram Auth Error", "2FA Password not provided.")
                        await self._stop_client_safely()
                        return
                    try:
                        logger.info("Checking 2FA password...")
                        await self.client.check_password(password_from_user)
                        if not self.client.me: 
                             logger.error("Post 2FA check failed: self.client.me is still None. Password might be incorrect.")
                             self._show_ui_messagebox("Telegram Auth Error", "2FA Password check failed (self.client.me is None).")
                             await self._stop_client_safely()
                             return
                        logger.info(f"2FA Password check successful. User: {self.client.me.username or self.client.me.id}")
                    except PasswordHashInvalid:
                        logger.error("Invalid 2FA password provided.")
                        self._show_ui_messagebox("Telegram Auth Error", "Invalid 2FA Password.")
                        await self._stop_client_safely()
                        return
                    except Exception as e_pwd_check:
                        logger.error(f"Error checking 2FA password: {e_pwd_check}", exc_info=True)
                        self._show_ui_messagebox("Telegram Auth Error", f"Error during 2FA: {e_pwd_check}")
                        await self._stop_client_safely()
                        return
                except (PhoneNumberInvalid, PhoneCodeInvalid, PhoneCodeEmpty) as e_auth:
                    logger.error(f"Telegram sign-in error: {type(e_auth).__name__} - {e_auth}", exc_info=True)
                    self._show_ui_messagebox("Telegram Auth Error", f"{type(e_auth).__name__}: {e_auth}. Please check and restart.")
                    await self._stop_client_safely()
                    return
                except FloodWait as e_flood: # MODIFIED: Catch FloodWait here
                    logger.error(f"FloodWait error during sign-in/send_code: Have to wait {e_flood.value} seconds.", exc_info=True)
                    self._show_ui_messagebox("Telegram FloodWait", f"Too many attempts. Please wait {e_flood.value} seconds and try restarting the bot.")
                    await self._stop_client_safely() # Stop the client as we can't proceed
                    return # Exit run method
                except Exception as e_signin:
                    logger.error(f"Unexpected error during Telegram sign-in: {e_signin}", exc_info=True)
                    self._show_ui_messagebox("Telegram Sign-In Error", f"An error occurred: {e_signin}")
                    await self._stop_client_safely()
                    return
            elif self.client.me:
                logger.info(f"User already authorized with Telegram as @{self.client.me.username or self.client.me.id}.")
            else: 
                logger.error("Client connected but self.client.me is None and no sign-in triggered. This is unexpected.")
                self.telegram_connected = False
                await self._stop_client_safely() 
                return

            if self.client.me: 
                logger.info(f"TelegramBot: Client authorized as @{self.client.me.username or self.client.me.id}.")
                self.telegram_connected = True
            else:
                logger.error("TelegramBot: Client authorization ultimately failed (self.client.me is None).")
                self.telegram_connected = False
                if not self._stop_trigger_event.is_set():
                    self._show_ui_messagebox("Telegram Auth Failed", "Could not authorize with Telegram. Delete .session file and RESTART THE BOT.")
                if self.client: await self._stop_client_safely()
                return

            self._update_ui_telegram_status(self.telegram_connected)

            if self.telegram_connected and chat_identifier_for_handler_and_get_chat != filters.private:
                chat_info_object_from_dialogs = None
                target_id_to_find = None

                if isinstance(chat_identifier_for_handler_and_get_chat, int):
                    target_id_to_find = chat_identifier_for_handler_and_get_chat
                elif isinstance(chat_identifier_for_handler_and_get_chat, str):
                    try: 
                        target_id_to_find = int(chat_identifier_for_handler_and_get_chat)
                    except ValueError: 
                        logger.info(f"Target chat is a username '{chat_identifier_for_handler_and_get_chat}'. Will rely on direct get_chat or already cached peer by add_handler.")
                
                if target_id_to_find is not None: 
                    try:
                        logger.info(f"Attempting to find target chat ID {target_id_to_find} in initial dialogs...")
                        dialog_count = 0
                        async for dialog in self.client.get_dialogs(limit=50): 
                            dialog_count += 1
                            logger.debug(f"Scanning Dialog {dialog_count}: ID {dialog.chat.id}, Title/User: {getattr(dialog.chat, 'title', getattr(dialog.chat, 'username', 'N/A'))}")
                            if dialog.chat.id == target_id_to_find:
                                chat_info_object_from_dialogs = dialog.chat
                                logger.info(f"Target chat {target_id_to_find} FOUND in dialogs. Title/User: {getattr(chat_info_object_from_dialogs, 'title', getattr(chat_info_object_from_dialogs, 'username', 'N/A'))}")
                                break 
                        if not chat_info_object_from_dialogs:
                            logger.warning(f"Target chat ID {target_id_to_find} NOT found in the first {dialog_count} dialogs.")
                        elif dialog_count == 0:
                             logger.info("No dialogs returned by get_dialogs().")
                    except Exception as e_dialogs:
                        logger.warning(f"Error fetching or iterating initial dialogs: {e_dialogs}", exc_info=True)
                
                await asyncio.sleep(0.1) 

                if chat_info_object_from_dialogs:
                    logger.info(f"Using chat_info from dialogs for target chat {target_id_to_find if target_id_to_find else chat_identifier_for_handler_and_get_chat}.")
                else:
                    logger.info(f"Target chat not found in initial dialog scan or target is username. Attempting direct self.client.get_chat for: {chat_identifier_for_handler_and_get_chat}")
                    try:
                        chat_info_object_from_dialogs = await self.client.get_chat(chat_identifier_for_handler_and_get_chat)
                        logger.info(f"Successfully validated/retrieved target chat via direct get_chat: ID {chat_info_object_from_dialogs.id}, Title/User: {getattr(chat_info_object_from_dialogs, 'title', getattr(chat_info_object_from_dialogs, 'username', 'N/A'))}")
                    except (PeerIdInvalid, ChannelInvalid, ChannelPrivate) as e_chat_specific_invalid: 
                        logger.error(f"TARGET CHAT ERROR (direct get_chat): Chat '{chat_identifier_for_handler_and_get_chat}' is invalid or inaccessible ({type(e_chat_specific_invalid).__name__}): {e_chat_specific_invalid}. "
                                     f"BOT ACCOUNT: '{self.client.me.username or self.client.me.id}'. "
                                     f"ACTION: 1. VERIFY Chat ID/Username '{chat_identifier_for_handler_and_get_chat}' is 100% correct (use @userinfobot with bot's account). "
                                     f"2. ENSURE bot account IS AN ACTIVE MEMBER of this chat/channel. "
                                     f"3. Delete .session file and RESTART bot.\n\nถ้าโดนเตะออกจากกลุ่มหรือกลุ่มเป็น private จะไม่สามารถเข้าถึงได้ ต้องให้ bot join กลุ่มใหม่อีกครั้ง")
                        self.telegram_connected = False 
                        self._update_ui_telegram_status(False)
                        self._show_ui_messagebox("Telegram: Target Chat Error",
                                                 f"Cannot access Target Chat: '{chat_identifier_for_handler_and_get_chat}'.\n"
                                                 f"Error: {type(e_chat_specific_invalid).__name__}.\n\n"
                                                 f"PLEASE VERIFY (using the bot's account '{self.client.me.username or self.client.me.id}'):\n"
                                                 f"1. Chat ID/Username in config.ini is EXACTLY correct for the target channel.\n"
                                                 f"2. The bot's account IS AN ACTIVE MEMBER of this chat/channel.\n"
                                                 f"3. If issues persist, delete '.session' file and RESTART bot.\n\nถ้าโดนเตะออกจากกลุ่มหรือกลุ่มเป็น private จะไม่สามารถเข้าถึงได้ ต้องให้ bot join กลุ่มใหม่อีกครั้ง")
                        if self.client: await self._stop_client_safely()
                        return
                    except UserNotParticipant: 
                        logger.error(f"Bot account '{self.client.me.username or self.client.me.id}' is NOT a participant in the target chat '{chat_identifier_for_handler_and_get_chat}'.")
                        self.telegram_connected = False
                        self._update_ui_telegram_status(False)
                        self._show_ui_messagebox("Telegram: Not in Chat",
                                                 f"Bot account ({self.client.me.username or self.client.me.id}) is NOT a member of the target chat/channel:\n'{chat_identifier_for_handler_and_get_chat}'.\n\n"
                                                 f"Please ADD THE BOT to the chat/channel and restart.")
                        if self.client: await self._stop_client_safely()
                        return
                    except Exception as e_chat_access: 
                        logger.error(f"Unexpected error accessing target chat '{chat_identifier_for_handler_and_get_chat}' via direct get_chat: {e_chat_access}", exc_info=True)
                        self.telegram_connected = False
                        self._update_ui_telegram_status(False)
                        self._show_ui_messagebox("Telegram: Chat Access Error", f"Error accessing target chat '{chat_identifier_for_handler_and_get_chat}':\n{e_chat_access}")
                        if self.client: await self._stop_client_safely()
                        return
                
                if not chat_info_object_from_dialogs: 
                    logger.error(f"Failed to obtain chat_info for target '{chat_identifier_for_handler_and_get_chat}' after all attempts.")
                    self.telegram_connected = False
                    self._update_ui_telegram_status(False)
                    self._show_ui_messagebox("Telegram: Target Chat Error", f"Could not resolve target chat '{chat_identifier_for_handler_and_get_chat}'. Please verify ID/membership and restart.")
                    if self.client: await self._stop_client_safely()
                    return

            if self.telegram_connected:
                logger.info("Telegram Bot is running and connected. Waiting for stop signal.")
                await self._stop_trigger_event.wait()
                logger.info("TelegramBot: _stop_trigger_event was set, run() loop is finishing.")
            else: 
                logger.error("Telegram Bot failed to connect/authorize. Run method will exit.")
                if self.client and self.client.is_initialized and not self.client.is_connected and not self._stop_trigger_event.is_set():
                    await self._stop_client_safely()

        except (AuthKeyUnregistered, AuthBytesInvalid) as e_auth_critical:
            logger.error(f"Telegram critical auth error: {e_auth_critical}", exc_info=True)
            if isinstance(e_auth_critical, AuthBytesInvalid): self._handle_corrupted_session()
            self._update_ui_telegram_status(False)
            self._show_ui_messagebox("Telegram Auth Error", f"Critical Auth error: {e_auth_critical}. Delete .session file and RESTART.")
        except RPCError as rpc_e: 
            logger.error(f"Telegram RPCError: {rpc_e}", exc_info=True)
            self._update_ui_telegram_status(False)
            self._show_ui_messagebox("Telegram RPC Error", str(rpc_e))
        except AttributeError as ae: 
            logger.error(f"AttributeError in TelegramBot.run(): {ae}", exc_info=True)
            self._update_ui_telegram_status(False)
            self._show_ui_messagebox("Telegram Internal Error", f"Attribute error: {ae}. Check logs.")
        except Exception as e: 
            logger.error(f"Unexpected error in TelegramBot.run(): {e}", exc_info=True)
            self._update_ui_telegram_status(False)
            self._show_ui_messagebox("Telegram Error", f"Runtime error: {e}. Check logs.")
        finally:
            logger.info("TelegramBot.run() method is finishing (outer finally).")
            if self.client and self.client.is_initialized and not self._stop_trigger_event.is_set(): 
                await self._stop_client_safely()
            self.telegram_connected = False 
            self._update_ui_telegram_status(False)
            logger.info("TelegramBot.run() task method fully finished.")

    def _handle_corrupted_session(self):
        session_file_path = os.path.join(self.app_data_path, f"{self.session_name}.session")
        if os.path.exists(session_file_path):
            try:
                os.remove(session_file_path)
                logger.info(f"Removed corrupted session: {session_file_path}")
            except Exception as e_del:
                logger.error(f"Could not remove session {session_file_path}: {e_del}")

    async def _stop_client_safely(self):
        if self.client and self.client.is_initialized:
            try:
                if self.client.is_connected:
                    logger.info("Safely stopping connected Pyrogram client...")
                    await self.client.stop(block=False) 
                    logger.info("Pyrogram client stop initiated.")
                else:
                    logger.info("Pyrogram client initialized but not connected.")
            except Exception as e: 
                logger.error(f"Exception during safe client stop: {e}", exc_info=True)
        self.client = None 

    async def stop(self):
        logger.info("TelegramBot.stop: called")
        self._stop_trigger_event.set() 
        await self._stop_client_safely()
        self.telegram_connected = False
        self._update_ui_telegram_status(False)
        logger.info("TelegramBot.stop() completed.")

    def _update_ui_telegram_status(self, connected: bool):
        if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master and hasattr(self._app_instance.master, 'winfo_exists') and self._app_instance.master.winfo_exists():
            self._app_instance.master.after(0, lambda conn=connected: self._app_instance.update_telegram_status_ui(conn))
        else:
            logger.warning("TelegramBot: Cannot update UI status, _app_instance or master is not available.")

    def _show_ui_messagebox(self, title: str, message: str):
        if self._app_instance and hasattr(self._app_instance, 'master') and self._app_instance.master and hasattr(self._app_instance.master, 'winfo_exists') and self._app_instance.master.winfo_exists():
            self._app_instance.master.after(0, lambda t=title, m=message: messagebox.showerror(t, m, parent=self._app_instance.master))
        else:
            logger.warning(f"TelegramBot: Cannot show messagebox '{title}', _app_instance or master is not available.")

    async def _message_handler(self, client: Client, message):
        try:
            if not message or not message.text: return
            if client is not self.client:
                logger.warning("Message handler received a client instance different from self.client. Ignoring.")
                return

            chat_id = message.chat.id
            chat_title = getattr(message.chat, 'title', getattr(message.chat, 'username', f"DM_{message.chat.id}"))

            if not (self._trading_bot_instance and self._trading_bot_instance.running and self.telegram_connected):
                logger.debug(f"Ignoring message: TradingBot/TelegramBot not fully active. Chat: {chat_title} ({chat_id})")
                return

            is_intended_recipient = False
            effective_target_filter: Any 
            if self.target_chat_id_numeric is not None:
                effective_target_filter = self.target_chat_id_numeric
            elif self.target_chat_str:
                effective_target_filter = self.target_chat_str.lstrip('@') 
            else:
                effective_target_filter = filters.private 

            if effective_target_filter == filters.private: 
                if message.chat.type == enums.ChatType.PRIVATE:
                    is_intended_recipient = True
            elif isinstance(effective_target_filter, int): 
                if chat_id == effective_target_filter:
                    is_intended_recipient = True
            elif isinstance(effective_target_filter, str): 
                if message.chat.username and message.chat.username.lower() == effective_target_filter.lower():
                    is_intended_recipient = True
            
            if not is_intended_recipient:
                logger.debug(f"Ignoring message from non-target chat '{chat_title}' ({chat_id}). Configured target: '{self.target_chat_str or 'Private DM'}'")
                return

            logger.info(f"Received message in '{chat_title}' ({chat_id}): \"{message.text[:100].replace(chr(10), ' ')}\"")

            if message.text.startswith('/'):
                command = message.text.lower().strip().split(maxsplit=1)[0]
                await self._handle_command(client, message, command)
                return

            parsed_signal: Optional[TradingSignal] = self.signal_parser.parse_signal(message.text)
            if parsed_signal:
                logger.info(f"Successfully parsed signal: {parsed_signal}")
                if self.signal_parser.validate_signal(parsed_signal):
                    logger.info(f"Validated signal for {parsed_signal.symbol} is being sent to TradingBot.")
                    if self._trading_bot_instance and hasattr(self._trading_bot_instance, 'execute_trade_from_signal'):
                        asyncio.create_task(self._trading_bot_instance.execute_trade_from_signal(parsed_signal))
                    else:
                        logger.error("TradingBot instance or execute_trade_from_signal method not available.")
                else:
                    logger.warning(f"Parsed signal failed validation: {parsed_signal}")
            else:
                logger.debug(f"Message from '{chat_title}' did not parse as a trading signal or command.")
        except Exception as e:
            logger.error(f"Error in Telegram message handler: {e}", exc_info=True)

    async def _handle_command(self, client: Client, message, command: str):
        if client is not self.client:
            logger.warning("Command handler received a client instance different from self.client. Ignoring command.")
            return
        incoming_chat_id = message.chat.id
        response_message: str = "Processing command..."
        try:
            if command == "/balance":
                if self.bybit_trader and self.bybit_trader.bybit_connected:
                    await client.send_message(incoming_chat_id, "Fetching Bybit balance...")
                    balance_data = await self.bybit_trader.get_wallet_balance()
                    if balance_data:
                        response_message = (
                            f"Current USDT Balance:\n"
                            f"Total Equity: {balance_data.get('total_balance', 0.0):.2f}\n"
                            f"Available: {balance_data.get('available_balance', 0.0):.2f}\n"
                            f"Used Margin: {balance_data.get('used_margin', 0.0):.2f}"
                        )
                    else: response_message = "Failed to fetch Bybit balance."
                else: response_message = "Bybit API is not connected."
            elif command == "/orders":
                if self.bybit_trader and self.bybit_trader.bybit_connected:
                    await client.send_message(incoming_chat_id, "Fetching open orders...")
                    open_orders = await self.bybit_trader.get_open_orders(settleCoin=self.bybit_trader.default_coin)
                    if open_orders:
                        order_list = "Open Orders (max 5 shown):\n";
                        for order in open_orders[:5]:
                            order_list += (f"  Sym: {order.get('symbol')}, Side: {order.get('side')}, "
                                           f"Qty: {order.get('qty')}, Price: {order.get('price', 'N/A')}, "
                                           f"St: {order.get('orderStatus')}\n")
                        if len(open_orders) > 5: order_list += f"...and {len(open_orders)-5} more."
                        response_message = order_list
                    else: response_message = "No open orders found."
                else: response_message = "Bybit API is not connected."
            elif command == "/positions":
                if self.bybit_trader and self.bybit_trader.bybit_connected:
                    await client.send_message(incoming_chat_id, "Fetching open positions...")
                    open_positions = await self.bybit_trader.get_open_positions(settleCoin=self.bybit_trader.default_coin)
                    if open_positions:
                        position_list = "Open Positions (max 5 shown):\n"
                        for position in open_positions[:5]:
                            pnl_str = f"{float(position.get('unrealisedPnl', 0)):.2f}"
                            position_list += (f"  Sym: {position.get('symbol')}, Side: {position.get('side')}, "
                                              f"Size: {position.get('size')}, Entry: {position.get('avgPrice')}, "
                                              f"PnL: {pnl_str}\n")
                        if len(open_positions) > 5: position_list += f"...and {len(open_positions)-5} more."
                        response_message = position_list
                    else: response_message = "No open positions found."
                else: response_message = "Bybit API is not connected."
            elif command == "/help":
                response_message = ("Available Commands:\n/balance\n/orders\n/positions\n/help\n\nSignals are processed automatically.")
            else:
                logger.debug(f"Unknown command received: {command}")
                response_message = f"Unknown command: {command}. Try /help."
            if response_message != "Processing command...":
                await client.send_message(incoming_chat_id, response_message)
        except Exception as e_cmd:
            logger.error(f"Error handling command '{command}': {e_cmd}", exc_info=True)
            try:
                await client.send_message(incoming_chat_id, f"Error processing command '{command}'.")
            except Exception as e_send_err:
                 logger.error(f"Failed to send error message to chat {incoming_chat_id}: {e_send_err}")

