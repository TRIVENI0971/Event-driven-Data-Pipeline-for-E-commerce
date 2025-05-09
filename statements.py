

import logging


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("app.log"),  
        logging.StreamHandler()  
    ]
)

# Create a logger instance
logger = logging.getLogger(__name__)
