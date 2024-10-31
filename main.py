# main.py

import sys
import logging
from database import DatabaseManager
from gui import Application

# Настройка логирования
logging.basicConfig(filename='data_generator.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')


def main():
    app = Application()
    app.run()


if __name__ == '__main__':
    main()
