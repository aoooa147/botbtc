import logging
import os
import sys
import queue # เพิ่ม import queue

# คลาส Handler ที่จะส่ง log record ไปยัง Queue
class QueueHandler(logging.Handler):
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue
    def emit(self, record):
        # ใส่ record ที่จัดรูปแบบแล้วเข้าไปใน queue
        self.log_queue.put(self.format(record))

def setup_logger(log_file='logs/trading.log', level=logging.INFO, log_queue=None):
    """
    ตั้งค่า logger ให้บันทึกทั้ง console, ไฟล์ และ Queue (สำหรับ UI)
    :param log_file: เส้นทางของไฟล์ log
    :param level: ระดับของ log (เช่น logging.INFO, logging.DEBUG)
    :param log_queue: queue.Queue object สำหรับส่ง log ไปยัง UI
    """
    # ตรวจสอบและสร้าง directory สำหรับ log file ถ้ายังไม่มี
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Get the root logger
    root_logger = logging.getLogger()

    # ล้าง handler ที่มีอยู่แล้วเพื่อป้องกันการ duplicate log
    # This is important if this function can be called multiple times
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close() # Close the handler before removing

    # สร้าง formatter สำหรับ log ทั้งหมด - Updated format
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%H:%M:%S')

    # Handler สำหรับบันทึกลงไฟล์
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)

    # Handler สำหรับแสดงผลใน console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    try:
        # พยายามตั้งค่า encoding ของ console เป็น UTF-8
        # This might not always work depending on the environment (e.g., some IDE consoles)
        sys.stdout.reconfigure(encoding='utf-8') # More modern way if Python 3.7+
    except Exception as e:
        logging.warning(f"Could not set UTF-8 encoding for console logging: {e}. Console output might show encoding errors for some characters.")

    # ตั้งค่า handlers หลัก
    handlers_list = [file_handler, console_handler]

    # ถ้ามี log_queue ส่งเข้ามา ให้เพิ่ม QueueHandler ด้วย
    if log_queue:
        queue_handler = QueueHandler(log_queue)
        queue_handler.setFormatter(formatter) # ใช้ formatter เดียวกัน
        handlers_list.append(queue_handler)

    # Configure the root logger
    root_logger.setLevel(level)
    for handler in handlers_list:
        root_logger.addHandler(handler)

    logging.info(f"Logger setup complete (from logger.py). Logs will be saved to {log_file}")

