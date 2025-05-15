import logging
import os

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

logger_product_category_service = logging.getLogger('product_category_service')
logger_product_category_service.setLevel(os.getenv('LOG_LEVEL', 'INFO'))
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger_product_category_service.addHandler(console_handler)

logger_profile_service = logging.getLogger('profile_service')
logger_profile_service.setLevel(os.getenv('LOG_LEVEL', 'INFO'))
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger_profile_service.addHandler(console_handler)
