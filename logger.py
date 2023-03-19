import logging

logger = logging.getLogger(__name__)
format = '%(levelname)s:%(name)s:%(message)s'
formatter = logging.Formatter(format)

file_handler = logging.FileHandler('sprint_one.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logging.basicConfig(format=format, level=logging.DEBUG, datefmt='%H:%M:%S')

