# utils.py

import logging


def generate_sql_value(value, data_type):
    try:
        if value is None:
            return 'NULL'
        if data_type in ['NVARCHAR', 'VARCHAR', 'CHAR', 'NCHAR', 'TEXT']:
            return f"N'{str(value).replace('\'', '\'\'')}'"
        elif data_type in ['DATE', 'DATETIME', 'TIMESTAMP', 'DATETIME2', 'SMALLDATETIME']:
            return f"'{value}'"
        elif data_type in ['INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'FLOAT', 'REAL', 'DECIMAL', 'NUMERIC', 'BIT']:
            return str(value)
        elif data_type == 'UNIQUEIDENTIFIER':
            return f"'{value}'"
        elif data_type == 'VARBINARY':
            return f"0x{value.hex()}"
        else:
            return f"N'{value}'"
    except Exception as e:
        logging.error(f"Ошибка в generate_sql_value для значения {value} и типа {data_type}: {e}")
        return 'NULL'
