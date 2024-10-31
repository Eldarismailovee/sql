# database.py

import random
import uuid
import logging
import pyodbc
from faker import Faker
from utils import generate_sql_value
import tkinter.messagebox as messagebox

# Инициализация Faker с русской локализацией
fake = Faker('ru_RU')
MAX_RECORDS = 1000


class DatabaseManager:
    def __init__(self):
        self.conn = None

    def connect(self, server, auth_method, username='', password=''):
        try:
            if auth_method == 'SQL Server Authentication':
                connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};UID={username};PWD={password};'
            else:
                connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};Trusted_Connection=yes;'
            self.conn = pyodbc.connect(connection_string)
            logging.info(f"Успешное подключение к серверу {server}")
            return True
        except Exception as e:
            logging.error(f"Ошибка подключения к серверу {server}: {e}")
            messagebox.showerror("Ошибка подключения", f"Ошибка подключения: {e}")
            return False

    def get_databases(self):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb');")
            databases = [row.name for row in cursor.fetchall()]
            cursor.close()
            return databases
        except Exception as e:
            logging.error(f"Ошибка при получении списка баз данных: {e}")
            messagebox.showerror("Ошибка", f"Ошибка при получении списка баз данных: {e}")
            return []

    def use_database(self, database_name):
        try:
            self.conn.autocommit = True
            self.conn.execute(f"USE [{database_name}];")
            logging.info(f"Переключено на базу данных {database_name}")
            return True
        except Exception as e:
            logging.error(f"Ошибка при переключении базы данных {database_name}: {e}")
            messagebox.showerror("Ошибка", f"Ошибка при переключении базы данных: {e}")
            return False

    def get_tables(self):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';")
            tables = [row.TABLE_NAME for row in cursor.fetchall()]
            cursor.close()
            return tables
        except Exception as e:
            logging.error(f"Ошибка при получении списка таблиц: {e}")
            messagebox.showerror("Ошибка", f"Ошибка при получении списка таблиц: {e}")
            return []

    def get_table_schema(self, table_name, schema='dbo'):
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT 
                    COLUMN_NAME, 
                    DATA_TYPE, 
                    CHARACTER_MAXIMUM_LENGTH,
                    IS_NULLABLE,
                    COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') AS IsIdentity
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?
            """, (table_name, schema))
            schema_info = cursor.fetchall()
            logging.info(f"Получена схема таблицы {schema}.{table_name}")
            return schema_info
        except Exception as e:
            logging.error(f"Ошибка при получении схемы таблицы {schema}.{table_name}: {e}")
            messagebox.showerror("Ошибка", f"Ошибка при получении схемы таблицы {table_name}: {e}")
            return []
        finally:
            cursor.close()

    def get_foreign_keys(self, table_name, schema='dbo'):
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT
                    fk.name AS ForeignKey,
                    cp.name AS ParentColumn,
                    tr.name AS ReferencedTable,
                    cr.name AS ReferencedColumn
                FROM
                    sys.foreign_keys AS fk
                INNER JOIN
                    sys.foreign_key_columns AS fkc ON fk.object_id = fkc.constraint_object_id
                INNER JOIN
                    sys.columns AS cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
                INNER JOIN
                    sys.tables AS tr ON fkc.referenced_object_id = tr.object_id
                INNER JOIN
                    sys.columns AS cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
                WHERE
                    fk.parent_object_id = OBJECT_ID(?)
            """, (f"{schema}.{table_name}",))
            foreign_keys = cursor.fetchall()
            logging.info(f"Получены внешние ключи для таблицы {schema}.{table_name}")
            return foreign_keys
        except Exception as e:
            logging.error(f"Ошибка при получении внешних ключей для таблицы {schema}.{table_name}: {e}")
            messagebox.showerror("Ошибка", f"Ошибка при получении внешних ключей для таблицы {table_name}: {e}")
            return []
        finally:
            cursor.close()

    def get_unique_columns(self, table_name, schema='dbo'):
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT
                    kcu.COLUMN_NAME,
                    c.DATA_TYPE
                FROM
                    INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                        ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                    JOIN INFORMATION_SCHEMA.COLUMNS c
                        ON c.TABLE_NAME = tc.TABLE_NAME AND c.COLUMN_NAME = kcu.COLUMN_NAME
                WHERE
                    tc.TABLE_NAME = ?
                    AND tc.TABLE_SCHEMA = ?
                    AND tc.CONSTRAINT_TYPE = 'UNIQUE'
            """, (table_name, schema))
            unique_columns_info = cursor.fetchall()
            unique_columns = [(row.COLUMN_NAME, row.DATA_TYPE) for row in unique_columns_info if row.DATA_TYPE.upper() not in ['BIT']]
            logging.info(f"Уникальные столбцы для таблицы {schema}.{table_name}: {[col[0] for col in unique_columns]}")
            return unique_columns
        except Exception as e:
            logging.error(f"Ошибка при получении уникальных столбцов для таблицы {schema}.{table_name}: {e}")
            messagebox.showerror("Ошибка", f"Ошибка при получении уникальных столбцов для таблицы {table_name}: {e}")
            return []
        finally:
            cursor.close()

    def get_existing_fk_values(self, referenced_table, referenced_column, schema='dbo'):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT [{referenced_column}] FROM [{schema}].[{referenced_table}]")
            values = [row[0] for row in cursor.fetchall()]
            logging.info(f"Получены существующие значения для внешнего ключа {referenced_column} из таблицы {schema}.{referenced_table}")
            return values
        except Exception as e:
            logging.error(f"Ошибка при получении существующих значений из таблицы {schema}.{referenced_table}: {e}")
            messagebox.showerror("Ошибка", f"Ошибка при получении существующих значений из таблицы {referenced_table}: {e}")
            return []
        finally:
            cursor.close()

    def generate_value(self, data_type):
        try:
            if data_type in ['NVARCHAR', 'VARCHAR', 'CHAR', 'NCHAR', 'TEXT']:
                return fake.word()
            elif data_type in ['INT', 'BIGINT', 'SMALLINT', 'TINYINT']:
                return fake.unique.random_int(min=1, max=1000000)
            elif data_type in ['FLOAT', 'REAL', 'DECIMAL', 'NUMERIC']:
                return round(random.uniform(1.0, 1000.0), 2)
            elif data_type == 'DATE':
                return fake.date_between(start_date='-30y', end_date='today').strftime('%Y-%m-%d')
            elif data_type in ['DATETIME', 'DATETIME2', 'SMALLDATETIME']:
                return fake.date_time_between(start_date='-30y', end_date='now').strftime('%Y-%m-%d %H:%M:%S')
            elif data_type == 'BIT':
                return random.choice([0, 1])
            elif data_type == 'UNIQUEIDENTIFIER':
                return str(uuid.uuid4())
            elif data_type == 'VARBINARY':
                return fake.binary(length=16)
            else:
                return fake.word()
        except Exception as e:
            logging.error(f"Ошибка в generate_value для типа данных {data_type}: {e}")
            return None

    def generate_records(self, table_name, n, schema='dbo'):
        try:
            schema_info = self.get_table_schema(table_name, schema)
            if not schema_info:
                logging.warning(f"Схема таблицы {schema}.{table_name} пустая.")
                return []

            foreign_keys = self.get_foreign_keys(table_name, schema)
            fk_columns = {fk.ParentColumn: (fk.ReferencedTable, fk.ReferencedColumn) for fk in foreign_keys}

            unique_columns = self.get_unique_columns(table_name, schema)
            unique_generators = {}

            for col, data_type in unique_columns:
                if data_type.upper() in ['NVARCHAR', 'VARCHAR', 'CHAR', 'NCHAR', 'TEXT']:
                    unique_generators[col] = fake.unique.word
                elif data_type.upper() in ['INT', 'BIGINT', 'SMALLINT', 'TINYINT']:
                    unique_generators[col] = lambda: fake.unique.random_int(min=1, max=1000000)
                elif data_type.upper() == 'UNIQUEIDENTIFIER':
                    unique_generators[col] = fake.unique.uuid4
                else:
                    unique_generators[col] = fake.unique.word

            records = []
            for i in range(n):
                record = {}
                for column in schema_info:
                    column_name, data_type, char_max_length, is_nullable, is_identity = column
                    if is_identity == 1:
                        continue

                    if column_name in fk_columns:
                        referenced_table, referenced_column = fk_columns[column_name]
                        existing_values = self.get_existing_fk_values(referenced_table, referenced_column, schema)
                        if not existing_values:
                            messagebox.showerror("Ошибка", f"Нет существующих значений для внешнего ключа {column_name} в таблице {referenced_table}.")
                            logging.error(f"Нет существующих значений для внешнего ключа {column_name} в таблице {referenced_table}.")
                            return []
                        value = random.choice(existing_values)
                    else:
                        if any(col[0] == column_name for col in unique_columns):
                            try:
                                value = unique_generators[column_name]()
                            except Exception as e:
                                messagebox.showerror("Ошибка", f"Ошибка при генерации уникального значения для столбца {column_name}: {e}")
                                logging.error(f"Ошибка при генерации уникального значения для столбца {column_name}: {e}")
                                return []
                        else:
                            value = self.generate_value(data_type.upper())

                        if data_type.upper() in ['NVARCHAR', 'VARCHAR', 'CHAR', 'NCHAR', 'TEXT'] and char_max_length:
                            value = value[:char_max_length] if len(value) > char_max_length else value

                    if value is None:
                        if is_nullable == 'YES':
                            record[column_name] = None
                        else:
                            while value is None:
                                if column_name in fk_columns:
                                    referenced_table, referenced_column = fk_columns[column_name]
                                    existing_values = self.get_existing_fk_values(referenced_table, referenced_column, schema)
                                    if not existing_values:
                                        messagebox.showerror("Ошибка", f"Нет существующих значений для внешнего ключа {column_name} в таблице {referenced_table}.")
                                        logging.error(f"Нет существующих значений для внешнего ключа {column_name} в таблице {referenced_table}.")
                                        return []
                                    value = random.choice(existing_values)
                                else:
                                    if any(col[0] == column_name for col in unique_columns):
                                        try:
                                            value = unique_generators[column_name]()
                                        except Exception as e:
                                            messagebox.showerror("Ошибка", f"Ошибка при генерации уникального значения для столбца {column_name}: {e}")
                                            logging.error(f"Ошибка при генерации уникального значения для столбца {column_name}: {e}")
                                            return []
                                    else:
                                        value = self.generate_value(data_type.upper())
                                        if data_type.upper() in ['NVARCHAR', 'VARCHAR', 'CHAR', 'NCHAR', 'TEXT'] and char_max_length:
                                            value = value[:char_max_length] if len(value) > char_max_length else value
                                if value is not None:
                                    break
                            record[column_name] = value
                    else:
                        record[column_name] = value
                records.append(record)

            logging.info(f"Сгенерировано {len(records)} записей для таблицы {schema}.{table_name}")
            return records
        except Exception as e:
            logging.error(f"Ошибка при генерации записей для таблицы {schema}.{table_name}: {e}")
            messagebox.showerror("Ошибка", f"Ошибка при генерации записей: {e}")
            return []

    def generate_insert_queries(self, table_name, records, schema='dbo'):
        if not records:
            return []

        queries = []

        for record in records:
            columns = record.keys()
            values = []
            try:
                for column in columns:
                    value = record[column]
                    cursor = self.conn.cursor()
                    cursor.execute("""
                        SELECT DATA_TYPE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = ? AND COLUMN_NAME = ? AND TABLE_SCHEMA = ?
                    """, (table_name, column, schema))
                    result = cursor.fetchone()
                    data_type = result[0] if result else 'NVARCHAR'
                    sql_value = generate_sql_value(value, data_type.upper())
                    values.append(sql_value)
                    cursor.close()
                columns_str = ', '.join([f"[{col}]" for col in columns])
                query = f"INSERT INTO [{schema}].[{table_name}] ({columns_str}) VALUES ({', '.join(values)});"
                queries.append(query)
            except Exception as e:
                logging.error(f"Ошибка при генерации запроса для столбца {column} в таблице {schema}.{table_name}: {e}")
                continue

        logging.info(f"Сгенерировано {len(queries)} SQL-запросов для вставки данных в таблицу {schema}.{table_name}")
        return queries

    def execute_queries(self, queries):
        cursor = self.conn.cursor()
        try:
            for query in queries:
                cursor.execute(query)
            self.conn.commit()
            messagebox.showinfo("Успех", f"Успешно выполнено {len(queries)} запросов.")
            logging.info(f"Успешно выполнено {len(queries)} запросов.")
        except Exception as e:
            self.conn.rollback()
            messagebox.showerror("Ошибка", f"Ошибка при выполнении запросов: {e}")
            logging.error(f"Ошибка при выполнении запросов: {e}")
        finally:
            cursor.close()

    def close_connection(self):
        if self.conn:
            self.conn.close()
