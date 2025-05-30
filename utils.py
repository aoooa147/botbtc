import httpx
import json
import logging
import logging.handlers
import os
from typing import Any, Dict, Optional, Tuple # Added Tuple
from configparser import ConfigParser, NoSectionError, NoOptionError # Added specific exceptions
from cryptography.fernet import Fernet
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError # Added RetryError
import time
import asyncio
import inspect
from pybit.unified_trading import HTTP

# Load environment variables from .env file
load_dotenv()

# Configure basic logging (can be overridden by main app's logger setup)
# This initial setup is useful if utils.py is imported before main logger is configured.
if not logging.getLogger().hasHandlers(): # Check if root logger already has handlers
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler() # Default to console if no file handler set up yet
        ]
    )

logger = logging.getLogger(__name__) # Get a logger specific to this module

class SecureConfig:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self._config = ConfigParser(interpolation=None) # Use interpolation=None for raw values
        self._key = self._get_or_create_key()
        if self._key: # Only create Fernet if key is available
            self._fernet = Fernet(self._key)
        else:
            self._fernet = None # No encryption/decryption if key is missing
            logger.warning("Encryption key not found or generated. Sensitive config values will not be encrypted/decrypted.")
        self._load_config()

    def _get_or_create_key(self) -> Optional[bytes]:
        """Get encryption key from environment or create a new one."""
        try:
            key_str = os.getenv('CONFIG_ENCRYPTION_KEY')
            if key_str:
                return key_str.encode()
            
            logger.info("CONFIG_ENCRYPTION_KEY not found in .env, generating a new one.")
            key = Fernet.generate_key()
            # Try to append to .env, handle potential errors
            try:
                with open('.env', 'a') as f: # 'a' to append
                    f.write(f'\nCONFIG_ENCRYPTION_KEY={key.decode()}\n')
                logger.info("New CONFIG_ENCRYPTION_KEY appended to .env file.")
                return key
            except IOError as e:
                logger.error(f"Could not write new encryption key to .env file: {e}. Encryption will be disabled.")
                return None
        except Exception as e:
            logger.error(f"Error getting or creating encryption key: {e}", exc_info=True)
            return None


    def _load_config(self):
        """Load and decrypt configuration."""
        if not os.path.exists(self.config_path):
            logger.error(f"Config file not found: {self.config_path}")
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        try:
            # *** KEY CHANGE: Specify encoding as UTF-8 ***
            self._config.read(self.config_path, encoding='utf-8')
            logger.info(f"Successfully read config file: {self.config_path} with UTF-8 encoding.")
        except Exception as e:
            logger.error(f"Failed to read config file {self.config_path} with UTF-8: {e}", exc_info=True)
            # Fallback or re-raise depending on desired behavior
            raise
            
        if self._fernet: # Only decrypt if fernet is initialized
            self._decrypt_sensitive_values()
        else:
            logger.warning("Fernet not initialized, skipping decryption of config values.")


    def _decrypt_sensitive_values(self):
        """Decrypt sensitive configuration values if fernet is available."""
        if not self._fernet: return

        sensitive_keys_map = {
            'BYBIT': ['api_key', 'api_secret'],
            'Telegram': ['api_hash', 'phone'], # api_id is usually numeric, not secret
            'LICENSE': ['key']
        }
        logger.debug("Attempting to decrypt sensitive config values...")
        for section in self._config.sections():
            if section in sensitive_keys_map:
                for key in self._config[section]:
                    if key in sensitive_keys_map[section]:
                        value = self._config[section][key]
                        # Check if value looks like a Fernet token (starts with 'gAAAA')
                        if value and isinstance(value, str) and value.startswith('gAAAA'):
                            try:
                                decrypted_value = self._fernet.decrypt(value.encode()).decode()
                                self._config[section][key] = decrypted_value
                                logger.debug(f"Decrypted [{section}].{key}")
                            except Exception as e:
                                logger.error(f"Failed to decrypt [{section}].{key}: {e}. Value might not be encrypted or key is incorrect.")
                        # else: logger.debug(f"Value for [{section}].{key} does not look encrypted, skipping decryption.")
        logger.info("Sensitive value decryption process completed (if applicable).")


    def _encrypt_sensitive_values(self):
        """Encrypt sensitive configuration values before saving if fernet is available."""
        if not self._fernet: 
            logger.warning("Fernet not initialized, cannot encrypt values for saving.")
            return

        sensitive_keys_map = {
            'BYBIT': ['api_key', 'api_secret'],
            'Telegram': ['api_hash', 'phone'],
            'LICENSE': ['key']
        }
        logger.debug("Attempting to encrypt sensitive config values for saving...")
        for section in self._config.sections():
            if section in sensitive_keys_map:
                for key in self._config[section]:
                    if key in sensitive_keys_map[section]:
                        value_to_encrypt = self._config[section][key]
                        # Only encrypt if it's not already looking like an encrypted token
                        if value_to_encrypt and not value_to_encrypt.startswith('gAAAA'):
                            try:
                                encrypted_value = self._fernet.encrypt(value_to_encrypt.encode()).decode()
                                self._config[section][key] = encrypted_value # Temporarily set for writing
                                logger.debug(f"Encrypted [{section}].{key} for saving.")
                            except Exception as e:
                                logger.error(f"Failed to encrypt [{section}].{key} for saving: {e}")
        logger.info("Sensitive value encryption process for saving completed (if applicable).")


    def save(self):
        """Save configuration. Encrypts sensitive values before writing, then decrypts them back for runtime use."""
        temp_config_to_save = ConfigParser(interpolation=None)
        temp_config_to_save.read_dict(self._config) # Create a copy to encrypt for saving

        if self._fernet:
            # Encrypt sensitive values in the temporary copy
            sensitive_keys_map = {
                'BYBIT': ['api_key', 'api_secret'],
                'Telegram': ['api_hash', 'phone'],
                'LICENSE': ['key']
            }
            for section in temp_config_to_save.sections():
                if section in sensitive_keys_map:
                    for key in temp_config_to_save[section]:
                        if key in sensitive_keys_map[section]:
                            value_to_encrypt = temp_config_to_save[section][key]
                            if value_to_encrypt and not value_to_encrypt.startswith('gAAAA'):
                                try:
                                    encrypted_value = self._fernet.encrypt(value_to_encrypt.encode()).decode()
                                    temp_config_to_save[section][key] = encrypted_value
                                except Exception as e:
                                    logger.error(f"Failed to encrypt [{section}].{key} during save: {e}")
        else:
            logger.warning("Fernet not initialized, saving config values in plaintext.")

        try:
            with open(self.config_path, 'w', encoding='utf-8') as f:
                temp_config_to_save.write(f)
            logger.info(f"Configuration saved to {self.config_path}.")
        except IOError as e:
            logger.error(f"Failed to write config file {self.config_path}: {e}")
            raise
        # No need to re-decrypt self._config as it was never changed if fernet was active.
        # If fernet was not active, values were already plaintext.

    def get(self, section: str, option: str, fallback: Any = None) -> Any:
        try:
            return self._config.get(section, option, fallback=fallback)
        except (NoSectionError, NoOptionError):
            # logger.warning(f"Config: [{section}]/[{option}] not found, returning fallback '{fallback}'.")
            return fallback

    def getint(self, section: str, option: str, fallback: Optional[int] = None) -> Optional[int]:
        try:
            return self._config.getint(section, option, fallback=fallback)
        except (NoSectionError, NoOptionError, ValueError):
            # logger.warning(f"Config: [{section}]/[{option}] not found or not int, returning fallback '{fallback}'.")
            return fallback
            
    def getfloat(self, section: str, option: str, fallback: Optional[float] = None) -> Optional[float]:
        try:
            return self._config.getfloat(section, option, fallback=fallback)
        except (NoSectionError, NoOptionError, ValueError):
            # logger.warning(f"Config: [{section}]/[{option}] not found or not float, returning fallback '{fallback}'.")
            return fallback

    def getboolean(self, section: str, option: str, fallback: Optional[bool] = None) -> Optional[bool]:
        try:
            return self._config.getboolean(section, option, fallback=fallback)
        except (NoSectionError, NoOptionError, ValueError):
            # logger.warning(f"Config: [{section}]/[{option}] not found or not bool, returning fallback '{fallback}'.")
            return fallback

    def set(self, section: str, option: str, value: Any):
        if not self._config.has_section(section):
            self._config.add_section(section)
        self._config.set(section, option, str(value))

    def has_section(self, section: str) -> bool:
        return self._config.has_section(section)

    def read(self, path: str, encoding: Optional[str] = None): # Added for compatibility if main.py calls it
        logger.warning("SecureConfig.read() called. Config is usually loaded at initialization. Re-loading...")
        self._config.read(path, encoding=encoding or 'utf-8')
        if self._fernet: self._decrypt_sensitive_values()

    def write(self, fp, space_around_delimiters=True): # Added for compatibility
        logger.warning("SecureConfig.write() called. Use .save() for encryption-aware saving.")
        # This will write plaintext if called directly.
        self._config.write(fp, space_around_delimiters=space_around_delimiters)


# --- API Call Utilities ---
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def safe_api_call(func, *args, **kwargs) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Safely calls an API function, handling common errors and retries.
    Returns (result_data, error_data).
    error_data will contain {'code': ..., 'msg': ...} if an API or HTTP error occurs.
    """
    try:
        # Ensure rate limit is handled before the call if applicable
        # if 'bybit_rate_limiter' in globals() and func.__module__.startswith('pybit'): # Example check
        #     await bybit_rate_limiter.acquire()
        
        response = func(*args, **kwargs)
        if inspect.isawaitable(response):
            response = await response
        
        # Standard Bybit API v5 response structure check
        if isinstance(response, dict):
            ret_code = response.get('retCode')
            ret_msg = response.get('retMsg', 'Unknown error')
            
            if ret_code is not None and ret_code == 0:
                logger.debug(f"API call {func.__name__} successful. Result keys: {list(response.get('result', {}).keys()) if response.get('result') else 'No result field'}")
                return response, None # Success
            else:
                # Log more specific Bybit errors if possible
                if 'insufficient available balance' in ret_msg.lower():
                    logger.error(f"Bybit API Error (Insufficient Balance) for {func.__name__}: {ret_code} - {ret_msg}. Args: {args}, Kwargs: {kwargs}")
                elif 'ordernotexist' in ret_msg.lower() or 'order has been filled or cancelled' in ret_msg.lower():
                    logger.warning(f"Bybit API Info (Order Not Found/Filled/Cancelled) for {func.__name__}: {ret_code} - {ret_msg}")
                else:
                    logger.error(f"Bybit API Error for {func.__name__}: {ret_code} - {ret_msg}. Args: {args}, Kwargs: {kwargs}. Full Response: {response}")
                return None, {'code': ret_code, 'msg': ret_msg, 'response': response} # Include full response in error for context
        
        # If response is not a dict or doesn't match Bybit structure, treat as success but log it
        logger.debug(f"API call {func.__name__} returned non-standard response (or not a Bybit API call): {type(response)}. Assuming success.")
        return response, None

    except httpx.HTTPStatusError as e: # Specific to httpx if used by func
        logger.error(f"HTTP Status Error during API call {func.__name__}: {e.response.status_code} - {e.response.text}", exc_info=True)
        return None, {'code': e.response.status_code, 'msg': e.response.text}
    except httpx.RequestError as e: # Specific to httpx
        logger.error(f"Request Error (Network/Connection) during API call {func.__name__}: {e}", exc_info=True)
        # This is a candidate for retry by tenacity
        raise # Re-raise to allow tenacity to retry
    except RetryError as e_retry: # If tenacity gives up
        logger.error(f"API call {func.__name__} failed after multiple retries: {e_retry}", exc_info=True)
        return None, {'code': -2, 'msg': f"API call failed after retries: {e_retry}"}
    except Exception as e:
        logger.error(f"Unexpected exception during API call {func.__name__}: {e}", exc_info=True)
        return None, {'code': -1, 'msg': str(e)}

class RateLimiter:
    """Simple asyncio rate limiter."""
    def __init__(self, calls_per_interval: float, interval_seconds: float = 1.0):
        self.calls_per_interval = calls_per_interval
        self.interval_seconds = interval_seconds
        self._semaphore = asyncio.Semaphore(int(calls_per_interval)) # Allow this many concurrent calls within an interval logic
        self._timestamps: List[float] = []

    async def acquire(self):
        await self._semaphore.acquire()
        current_time = time.monotonic()
        
        # Remove timestamps older than the interval
        self._timestamps = [t for t in self._timestamps if current_time - t < self.interval_seconds]
        
        if len(self._timestamps) >= self.calls_per_interval:
            # Calculate time to wait until the oldest call in the window expires
            wait_time = (self._timestamps[0] + self.interval_seconds) - current_time
            if wait_time > 0:
                logger.debug(f"RateLimiter: Waiting {wait_time:.3f}s to respect rate limit ({self.calls_per_interval}/{self.interval_seconds}s).")
                await asyncio.sleep(wait_time)
                # Re-check after sleep
                current_time = time.monotonic()
                self._timestamps = [t for t in self._timestamps if current_time - t < self.interval_seconds]

        self._timestamps.append(current_time)
        # The semaphore release is implicit when the context is exited if used as `async with limiter:`
        # If used as `await limiter.acquire()`, a corresponding `limiter.release()` is needed.
        # For simple acquire-before-call, this is okay, but semaphore might not be the best primitive here.
        # A token bucket or leaky bucket might be more accurate for "X calls per Y seconds".
        # For now, this provides some basic control.

    def release(self): # If using semaphore explicitly
        self._semaphore.release()

# Global rate limiter instances (adjust rates based on actual API limits)
# Bybit's V5 API limits are more complex (e.g., 120 requests/second for some, 10/second for others)
# This simple limiter might not be sufficient for all endpoints.
# It's better to apply specific limiters per endpoint group if needed.
bybit_rate_limiter = RateLimiter(calls_per_interval=10, interval_seconds=1.0) # Example: 10 calls/sec overall
telegram_rate_limiter = RateLimiter(calls_per_interval=20, interval_seconds=1.0) # Example: 20 calls/sec

# Example of how validate_online_key might look if it were still in utils.py
# async def validate_online_key(user_token: str, license_server_url: str) -> Dict[str, Any]:
#     payload = {"key": user_token}
#     logger.info(f"Validating license key with server: {license_server_url}")
#     try:
#         async with httpx.AsyncClient() as client:
#             response = await client.post(license_server_url, json=payload, timeout=10)
#         response.raise_for_status() # Will raise an exception for 4xx/5xx errors
#         data = response.json()
#         is_valid = data.get("is_valid", False)
#         days_remaining = data.get("days_remaining", 0)
#         message = data.get("message", "Validation successful." if is_valid else "Key invalid or expired.")
#         logger.info(f"License validation response: Valid={is_valid}, Days Left={days_remaining}, Msg='{message}'")
#         return {"is_valid": is_valid, "days_remaining": days_remaining, "message": message}
#     except httpx.RequestError as e:
#         logger.error(f"Network error during license check: {e}")
#         return {"is_valid": False, "days_remaining": 0, "message": f"Cannot connect to license server: {e}"}
#     except httpx.HTTPStatusError as e:
#         logger.error(f"License server HTTP error: {e.response.status_code} - {e.response.text}")
#         return {"is_valid": False, "days_remaining": 0, "message": f"License server error: {e.response.status_code}"}
#     except json.JSONDecodeError:
#         logger.error(f"Failed to decode JSON from license server. Response: {response.text if 'response' in locals() else 'N/A'}")
#         return {"is_valid": False, "days_remaining": 0, "message": "Invalid response from license server."}
#     except Exception as e:
#         logger.error(f"Unexpected error during license validation: {e}", exc_info=True)
#         return {"is_valid": False, "days_remaining": 0, "message": f"Unexpected error: {e}"}
#     except Exception as e:
#         logger.error(f"Unexpected error during API call {func.__name__}: {e}", exc_info=True)
#         return None, {'code': -1, 'msg': str(e)}

