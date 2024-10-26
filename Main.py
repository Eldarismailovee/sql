import random
import uuid  # Для генерации GUID
from faker import Faker
import pyodbc
import PySimpleGUI as sg
import sys
import logging

# Настройка логирования
logging.basicConfig(filename='data_generator.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Инициализация Faker с русской локализацией
fake = Faker('ru_RU')

# Возможные значения для пола
GENDERS = ['M', 'F']

# Глобальные списки для хранения сгенерированных ID
# В реальной ситуации лучше получать реальные ID после вставки
generated_students = []
generated_courses = []

# Максимальное количество записей для вставки
MAX_RECORDS = 1000


def generate_sql_value(value, data_type):
    try:
        if value is None:
            return 'NULL'
        if data_type in ['NVARCHAR', 'VARCHAR', 'CHAR', 'NCHAR', 'TEXT']:
            return f"N'{value.replace('\'', '\'\'')}'"  # Экранирование одинарных кавычек
        elif data_type in ['DATE', 'DATETIME', 'TIMESTAMP', 'DATETIME2', 'SMALLDATETIME']:
            return f"'{value}'"
        elif data_type in ['INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'FLOAT', 'REAL', 'DECIMAL', 'NUMERIC', 'BIT']:
            return str(value)
        elif data_type == 'VARBINARY':
            # Генерация случайных байтов, длина может быть изменена по необходимости
            random_bytes = fake.binary(length=16)
            return f"0x{random_bytes.hex()}"
        else:
            return f"N'{value}'"
    except Exception as e:
        logging.error(f"Ошибка в generate_sql_value для значения {value} и типа {data_type}: {e}")
        return 'NULL'


def get_table_schema(conn, table_name):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT 
                COLUMN_NAME, 
                DATA_TYPE, 
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE,
                COLUMNPROPERTY(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') AS IsIdentity
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
        """, (table_name,))
        schema = cursor.fetchall()
        logging.info(f"Получена схема таблицы {table_name}")
        return schema
    except Exception as e:
        sg.popup_error(f"Ошибка при получении схемы таблицы: {e}")
        logging.error(f"Ошибка при получении схемы таблицы {table_name}: {e}")
        return []
    finally:
        cursor.close()


def generate_value(data_type):
    try:
        if data_type in ['NVARCHAR', 'VARCHAR', 'CHAR', 'NCHAR', 'TEXT']:
            return fake.word()
        elif data_type in ['INT', 'BIGINT', 'SMALLINT', 'TINYINT']:
            return random.randint(1, 1000)
        elif data_type in ['FLOAT', 'REAL', 'DECIMAL', 'NUMERIC']:
            return round(random.uniform(1.0, 1000.0), 2)
        elif data_type in ['DATE']:
            return fake.date_between(start_date='-30y', end_date='today').strftime('%Y-%m-%d')
        elif data_type in ['DATETIME', 'DATETIME2', 'SMALLDATETIME']:
            return fake.date_time_between(start_date='-30y', end_date='now').strftime('%Y-%m-%d %H:%M:%S')
        elif data_type in ['BIT']:
            return random.choice([0, 1])
        elif data_type == 'UNIQUEIDENTIFIER':
            return str(uuid.uuid4())  # Генерация действительного GUID
        elif data_type == 'VARBINARY':
            return fake.binary(length=16)  # Генерация случайных байтов
        else:
            return fake.word()  # Для остальных типов данных можно генерировать слово или реализовать дополнительную логику
    except Exception as e:
        logging.error(f"Ошибка в generate_value для типа данных {data_type}: {e}")
        return None


def generate_records_dynamic(table_name, n, conn):
    try:
        schema = get_table_schema(conn, table_name)
        if not schema:
            logging.warning(f"Схема таблицы {table_name} пустая.")
            return []

        records = []
        for i in range(n):
            record = {}
            for column in schema:
                column_name, data_type, char_max_length, is_nullable, is_identity = column
                # Пропускаем столбцы с автоинкрементом
                if is_identity == 1:
                    continue
                # Генерируем значение для столбца
                value = generate_value(data_type.upper())
                # Ограничиваем длину строковых значений
                if data_type.upper() in ['NVARCHAR', 'VARCHAR', 'CHAR', 'NCHAR', 'TEXT']:
                    if char_max_length is not None and value is not None:
                        if len(value) > char_max_length:
                            logging.warning(f"Обрезано значение для столбца {column_name}: {value} до длины {char_max_length}")
                            value = value[:char_max_length]  # Обрезаем строку до максимальной длины
                if value is None:
                    if is_nullable == 'YES':
                        record[column_name] = None
                    else:
                        # Если столбец не допускает NULL, генерируем значение снова
                        while value is None:
                            value = generate_value(data_type.upper())
                            if data_type.upper() in ['NVARCHAR', 'VARCHAR', 'CHAR', 'NCHAR', 'TEXT'] and char_max_length is not None and value is not None:
                                if len(value) > char_max_length:
                                    value = value[:char_max_length]
                            if value is not None:
                                break
                        record[column_name] = value
                else:
                    record[column_name] = value
            records.append(record)
        logging.info(f"Сгенерировано {len(records)} записей для таблицы {table_name}")
        return records
    except Exception as e:
        sg.popup_error(f"Ошибка при генерации записей: {e}")
        logging.error(f"Ошибка при генерации записей для таблицы {table_name}: {e}")
        return []


def generate_insert_queries_dynamic(table_name, records, conn):
    if not records:
        return []

    queries = []

    for record in records:
        columns = record.keys()
        values = []
        try:
            for column in columns:
                value = record[column]
                # Получаем тип данных столбца
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = ? AND COLUMN_NAME = ?
                """, (table_name, column))
                result = cursor.fetchone()
                data_type = result[0] if result else 'NVARCHAR'
                sql_value = generate_sql_value(value, data_type.upper())
                values.append(sql_value)
                cursor.close()
            # Экранируем имена столбцов и таблицы
            columns_str = ', '.join([f"[{col}]" for col in columns])
            query = f"INSERT INTO [{table_name}] ({columns_str}) VALUES ({', '.join(values)});"
            queries.append(query)
        except Exception as e:
            logging.error(f"Ошибка при генерации запроса для столбца {column} в таблице {table_name}: {e}")
            continue  # Пропускаем эту запись и продолжаем

    logging.info(f"Сгенерировано {len(queries)} SQL-запросов для вставки данных в таблицу {table_name}")
    return queries


def execute_queries(conn, queries):
    cursor = conn.cursor()
    try:
        for query in queries:
            cursor.execute(query)
        conn.commit()
        sg.popup_ok(f"Успешно выполнено {len(queries)} запросов.")
        logging.info(f"Успешно выполнено {len(queries)} запросов.")
    except Exception as e:
        conn.rollback()
        sg.popup_error(f"Ошибка при выполнении запросов: {e}")
        logging.error(f"Ошибка при выполнении запросов: {e}")
    finally:
        cursor.close()


def connection_window():
    sg.theme('BlueMono')  # Выбор более современной темы

    layout_connection = [
        [sg.Text('=== Подключение к SQL Server ===', font=('Helvetica', 14), justification='center')],
        [sg.Text('Имя сервера:', size=(20, 1)), sg.Input(key='server', expand_x=True)],
        [sg.Text('Метод аутентификации:', size=(20, 1)),
         sg.Combo(['Windows Authentication', 'SQL Server Authentication'],
                  default_value='Windows Authentication',
                  key='auth_method', enable_events=True, expand_x=True)],
        [sg.Text('Имя пользователя:', size=(20, 1)),
         sg.Input(key='username', disabled=True, expand_x=True)],
        [sg.Text('Пароль:', size=(20, 1)),
         sg.Input(password_char='*', key='password', disabled=True, expand_x=True)],
        [sg.Button('Подключиться', size=(12, 1)), sg.Button('Выйти', size=(10, 1))]
    ]

    return sg.Window('SQL Server Data Generator - Подключение',
                     layout_connection,
                     resizable=True,
                     finalize=True)


def database_selection_window(databases):
    layout_database = [
        [sg.Text('=== Выбор базы данных ===', font=('Helvetica', 14), justification='center')],
        [sg.Text('Доступные базы данных:', size=(20, 1))],
        [sg.Listbox(databases, size=(50, 10), key='database', enable_events=True, expand_x=True, expand_y=True)],
        [sg.Button('Выбрать', size=(12, 1)), sg.Button('Выйти', size=(10, 1))]
    ]

    return sg.Window('SQL Server Data Generator - Выбор базы данных',
                     layout_database,
                     resizable=True,
                     finalize=True)


def main_window(tables):
    layout_main = [
        [sg.Text('=== Генерация и Вставка Данных ===', font=('Helvetica', 14), justification='center')],
        [sg.Text('Выберите таблицу:', size=(20, 1)),
         sg.Combo(tables, key='table', readonly=True, expand_x=True)],
        [sg.Text('Количество записей:', size=(20, 1)), sg.Input(key='num_records', expand_x=True)],
        [sg.Text(f"Максимально допустимое количество записей: {MAX_RECORDS}", text_color='red')],
        [sg.Button('Генерировать и Вставить', size=(20, 1)), sg.Button('Назад', size=(10, 1)),
         sg.Button('Выйти', size=(10, 1))],
        [sg.Multiline(size=(80, 20), key='log', disabled=True, expand_x=True, expand_y=True)]
    ]

    return sg.Window('SQL Server Data Generator - Основное Окно',
                     layout_main,
                     resizable=True,
                     finalize=True)


def main():
    # Фаза подключения
    window_conn = connection_window()
    conn = None
    while True:
        try:
            event, values = window_conn.read()
            if event == sg.WINDOW_CLOSED or event == 'Выйти':
                window_conn.close()
                sys.exit()
            if event == 'auth_method':
                if values['auth_method'] == 'Windows Authentication':
                    window_conn['username'].update(disabled=True)
                    window_conn['password'].update(disabled=True)
                else:
                    window_conn['username'].update(disabled=False)
                    window_conn['password'].update(disabled=False)
            if event == 'Подключиться':
                server = values['server']
                auth_method = values['auth_method']
                username = values['username']
                password = values['password']

                if not server:
                    sg.popup_error("Пожалуйста, введите имя сервера.")
                    continue  # Продолжаем цикл подключения
                if auth_method == 'SQL Server Authentication':
                    if not username or not password:
                        sg.popup_error("Пожалуйста, введите имя пользователя и пароль.")
                        continue
                    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};UID={username};PWD={password};'
                else:
                    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};Trusted_Connection=yes;'

                try:
                    conn = pyodbc.connect(connection_string)
                    sg.popup_ok("Успешно подключено к SQL Server.")
                    window_conn.close()
                    break  # Выходим из цикла подключения
                except Exception as e:
                    sg.popup_error(f"Ошибка подключения: {e}")
                    logging.error(f"Ошибка подключения к серверу {server}: {e}")
                    continue  # Попробуем снова
        except Exception as e:
            sg.popup_error(f"Неизвестная ошибка: {e}")
            logging.error(f"Неизвестная ошибка в фазе подключения: {e}")
            window_conn.close()
            if conn:
                conn.close()
            sys.exit(1)

    # Фаза выбора базы данных
    while True:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb');")
            all_databases = [row[0] for row in cursor.fetchall()]
            cursor.close()
            if not all_databases:
                sg.popup_error("Нет доступных баз данных для выбора.")
                logging.warning("Нет доступных баз данных для выбора.")
                # Возвращаемся к окну подключения
                window_conn = connection_window()
                while True:
                    event, values = window_conn.read()
                    if event == sg.WINDOW_CLOSED or event == 'Выйти':
                        window_conn.close()
                        conn.close()
                        sys.exit()
                    if event == 'auth_method':
                        if values['auth_method'] == 'Windows Authentication':
                            window_conn['username'].update(disabled=True)
                            window_conn['password'].update(disabled=True)
                        else:
                            window_conn['username'].update(disabled=False)
                            window_conn['password'].update(disabled=False)
                    if event == 'Подключиться':
                        server = values['server']
                        auth_method = values['auth_method']
                        username = values['username']
                        password = values['password']

                        if not server:
                            sg.popup_error("Пожалуйста, введите имя сервера.")
                            continue
                        if auth_method == 'SQL Server Authentication':
                            if not username or not password:
                                sg.popup_error("Пожалуйста, введите имя пользователя и пароль.")
                                continue
                            connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};UID={username};PWD={password};'
                        else:
                            connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};Trusted_Connection=yes;'

                        try:
                            conn = pyodbc.connect(connection_string)
                            sg.popup_ok("Успешно подключено к SQL Server.")
                            window_conn.close()
                            break
                        except Exception as e:
                            sg.popup_error(f"Ошибка подключения: {e}")
                            logging.error(f"Ошибка подключения к серверу {server}: {e}")
                            continue
                continue  # После успешного подключения снова проверяем наличие баз данных
        except Exception as e:
            sg.popup_error(f"Ошибка при получении списка баз данных: {e}")
            logging.error(f"Ошибка при получении списка баз данных: {e}")
            conn.close()
            sys.exit(1)

        # Открываем окно выбора базы данных
        window_db = database_selection_window(all_databases)
        while True:
            try:
                event, values = window_db.read()
                if event == sg.WINDOW_CLOSED or event == 'Выйти':
                    window_db.close()
                    conn.close()
                    sys.exit()
                if event == 'Выбрать':
                    if not values['database']:
                        sg.popup_error("Пожалуйста, выберите базу данных.")
                        continue
                    selected_db = values['database'][0]
                    sg.popup_ok(f"Выбрана база данных: {selected_db}")
                    try:
                        conn.autocommit = True  # Для переключения базы данных
                        conn.execute(f"USE [{selected_db}];")
                        logging.info(f"Переключено на базу данных {selected_db}")
                    except Exception as e:
                        sg.popup_error(f"Ошибка при переключении базы данных: {e}")
                        logging.error(f"Ошибка при переключении базы данных {selected_db}: {e}")
                        window_db.close()
                        conn.close()
                        sys.exit(1)

                    # Получение списка таблиц в выбранной базе данных
                    try:
                        cursor = conn.cursor()
                        cursor.execute(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';")
                        all_tables = [row[0] for row in cursor.fetchall()]
                        cursor.close()

                        if not all_tables:
                            sg.popup_error("В выбранной базе данных нет таблиц для заполнения.")
                            logging.warning(f"В базе данных {selected_db} нет таблиц для заполнения.")
                            window_db.close()
                            break  # Возвращаемся к выбору базы данных
                    except Exception as e:
                        sg.popup_error(f"Ошибка при получении списка таблиц: {e}")
                        logging.error(f"Ошибка при получении списка таблиц из базы данных {selected_db}: {e}")
                        window_db.close()
                        conn.close()
                        sys.exit(1)

                    window_db.close()
                    break  # Переходим к главному окну
            except Exception as e:
                sg.popup_error(f"Неизвестная ошибка в фазе выбора базы данных: {e}")
                logging.error(f"Неизвестная ошибка в фазе выбора базы данных: {e}")
                window_db.close()
                conn.close()
                sys.exit(1)

        if not all_tables:
            continue  # Если таблиц нет, повторяем выбор базы данных

        # Открываем главное окно с таблицами
        window_main = main_window(all_tables)
        while True:
            try:
                event, values = window_main.read()
                if event == sg.WINDOW_CLOSED or event == 'Выйти':
                    break
                if event == 'Назад':
                    window_main.close()
                    # Возвращаемся к выбору базы данных
                    window_db = database_selection_window(all_databases)
                    while True:
                        event_db, values_db = window_db.read()
                        if event_db == sg.WINDOW_CLOSED or event_db == 'Выйти':
                            window_db.close()
                            window_main.close()
                            conn.close()
                            sys.exit()
                        if event_db == 'Выбрать':
                            if not values_db['database']:
                                sg.popup_error("Пожалуйста, выберите базу данных.")
                                continue
                            selected_db = values_db['database'][0]
                            sg.popup_ok(f"Выбрана база данных: {selected_db}")
                            try:
                                conn.autocommit = True  # Для переключения базы данных
                                conn.execute(f"USE [{selected_db}];")
                                logging.info(f"Переключено на базу данных {selected_db}")
                            except Exception as e:
                                sg.popup_error(f"Ошибка при переключении базы данных: {e}")
                                logging.error(f"Ошибка при переключении базы данных {selected_db}: {e}")
                                window_db.close()
                                window_main.close()
                                conn.close()
                                sys.exit(1)

                            # Получение списка таблиц в выбранной базе данных
                            try:
                                cursor = conn.cursor()
                                cursor.execute(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';")
                                all_tables = [row[0] for row in cursor.fetchall()]
                                cursor.close()

                                if not all_tables:
                                    sg.popup_error("В выбранной базе данных нет таблиц для заполнения.")
                                    logging.warning(f"В базе данных {selected_db} нет таблиц для заполнения.")
                                    window_db.close()
                                    window_db = database_selection_window(all_databases)
                                    continue
                            except Exception as e:
                                sg.popup_error(f"Ошибка при получении списка таблиц: {e}")
                                logging.error(f"Ошибка при получении списка таблиц из базы данных {selected_db}: {e}")
                                window_db.close()
                                window_main.close()
                                conn.close()
                                sys.exit(1)

                            window_db.close()
                            # Обновляем главное окно с новыми таблицами
                            window_main = main_window(all_tables)
                            break
                    continue  # Возвращаемся к главному окну

                if event == 'Генерировать и Вставить':
                    table = values['table']
                    num_records = values['num_records']
                    if not table:
                        sg.popup_error("Пожалуйста, выберите таблицу.")
                        continue  # Продолжаем цикл обработки событий
                    if not num_records:
                        sg.popup_error("Пожалуйста, введите количество записей.")
                        continue
                    try:
                        num = int(num_records)
                        if num <= 0:
                            sg.popup_error("Количество записей должно быть положительным числом.")
                            continue
                        if num > MAX_RECORDS:
                            sg.popup_error(f"Максимально допустимое количество записей: {MAX_RECORDS}.")
                            continue
                    except ValueError:
                        sg.popup_error("Пожалуйста, введите корректное числовое значение для количества записей.")
                        continue

                    window_main['log'].update(f"Начата генерация {num} записей для таблицы '{table}'...\n")

                    records = generate_records_dynamic(table, num, conn)
                    if not records:
                        sg.popup_error("Нет данных для вставки в выбранную таблицу. Проверьте схему таблицы.")
                        window_main['log'].update(f"Нет данных для вставки в таблицу '{table}'.\n", append=True)
                        continue

                    queries = generate_insert_queries_dynamic(table, records, conn)
                    if not queries:
                        sg.popup_error("Нет запросов для выполнения. Проверьте генерацию данных.")
                        window_main['log'].update(f"Нет запросов для выполнения для таблицы '{table}'.\n", append=True)
                        continue

                    execute_queries(conn, queries)

                    window_main['log'].update(f"Завершена генерация и вставка данных для таблицы '{table}'.\n", append=True)
            except Exception as e:
                sg.popup_error(f"Неизвестная ошибка в главном окне: {e}")
                logging.error(f"Неизвестная ошибка в главном окне: {e}")
                window_main.close()
                conn.close()
                sys.exit(1)

        window_main.close()
        conn.close()
        sys.exit()


if __name__ == '__main__':
    main()
