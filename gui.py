# gui.py

import tkinter as tk
from tkinter import ttk
import tkinter.messagebox as messagebox
from database import DatabaseManager
from faker import Faker
import logging

fake = Faker('ru_RU')
MAX_RECORDS = 1000


class Application:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.root = tk.Tk()
        self.root.title("SQL Server Data Generator")
        self.current_frame = None
        self.create_connection_frame()

    def run(self):
        self.root.mainloop()

    def create_connection_frame(self):
        if self.current_frame:
            self.current_frame.destroy()

        self.current_frame = tk.Frame(self.root)
        self.current_frame.pack(fill='both', expand=True)

        tk.Label(self.current_frame, text="=== Подключение к SQL Server ===", font=('Helvetica', 14)).pack(pady=10)

        tk.Label(self.current_frame, text="Имя сервера:").pack(pady=5)
        self.server_entry = tk.Entry(self.current_frame)
        self.server_entry.pack(pady=5)

        tk.Label(self.current_frame, text="Метод аутентификации:").pack(pady=5)
        self.auth_method = tk.StringVar()
        self.auth_method.set('Windows Authentication')
        self.auth_combo = ttk.Combobox(self.current_frame, textvariable=self.auth_method, state='readonly',
                                       values=['Windows Authentication', 'SQL Server Authentication'])
        self.auth_combo.pack(pady=5)
        self.auth_combo.bind('<<ComboboxSelected>>', self.toggle_auth_method)

        tk.Label(self.current_frame, text="Имя пользователя:").pack(pady=5)
        self.username_entry = tk.Entry(self.current_frame, state='disabled')
        self.username_entry.pack(pady=5)

        tk.Label(self.current_frame, text="Пароль:").pack(pady=5)
        self.password_entry = tk.Entry(self.current_frame, show='*', state='disabled')
        self.password_entry.pack(pady=5)

        tk.Button(self.current_frame, text="Подключиться", command=self.connect_to_server).pack(pady=10)
        tk.Button(self.current_frame, text="Выйти", command=self.exit_app).pack(pady=5)

    def toggle_auth_method(self, event=None):
        method = self.auth_method.get()
        if method == 'Windows Authentication':
            self.username_entry.config(state='disabled')
            self.password_entry.config(state='disabled')
        else:
            self.username_entry.config(state='normal')
            self.password_entry.config(state='normal')

    def connect_to_server(self):
        server = self.server_entry.get()
        auth_method = self.auth_method.get()
        username = self.username_entry.get()
        password = self.password_entry.get()

        if not server:
            messagebox.showerror("Ошибка", "Пожалуйста, введите имя сервера.")
            return

        if auth_method == 'SQL Server Authentication':
            if not username or not password:
                messagebox.showerror("Ошибка", "Пожалуйста, введите имя пользователя и пароль.")
                return

        connected = self.db_manager.connect(server, auth_method, username, password)
        if connected:
            messagebox.showinfo("Успех", "Успешно подключено к SQL Server.")
            self.create_database_selection_frame()

    def create_database_selection_frame(self):
        if self.current_frame:
            self.current_frame.destroy()

        self.current_frame = tk.Frame(self.root)
        self.current_frame.pack(fill='both', expand=True)

        tk.Label(self.current_frame, text="=== Выбор базы данных ===", font=('Helvetica', 14)).pack(pady=10)

        databases = self.db_manager.get_databases()
        if not databases:
            messagebox.showerror("Ошибка", "Нет доступных баз данных для выбора.")
            self.create_connection_frame()
            return

        tk.Label(self.current_frame, text="Доступные базы данных:").pack(pady=5)
        self.database_listbox = tk.Listbox(self.current_frame)
        for db in databases:
            self.database_listbox.insert(tk.END, db)
        self.database_listbox.pack(pady=5)

        tk.Button(self.current_frame, text="Выбрать", command=self.select_database).pack(pady=10)
        tk.Button(self.current_frame, text="Выйти", command=self.exit_app).pack(pady=5)

    def select_database(self):
        selected = self.database_listbox.curselection()
        if not selected:
            messagebox.showerror("Ошибка", "Пожалуйста, выберите базу данных.")
            return
        database_name = self.database_listbox.get(selected)
        used = self.db_manager.use_database(database_name)
        if used:
            messagebox.showinfo("Успех", f"Выбрана база данных: {database_name}")
            self.create_main_frame()

    def create_main_frame(self):
        if self.current_frame:
            self.current_frame.destroy()

        self.current_frame = tk.Frame(self.root)
        self.current_frame.pack(fill='both', expand=True)

        tk.Label(self.current_frame, text="=== Генерация и Вставка Данных ===", font=('Helvetica', 14)).pack(pady=10)

        tables = self.db_manager.get_tables()
        if not tables:
            messagebox.showerror("Ошибка", "В выбранной базе данных нет таблиц для заполнения.")
            self.create_database_selection_frame()
            return

        tk.Label(self.current_frame, text="Выберите таблицу:").pack(pady=5)
        self.table_combo = ttk.Combobox(self.current_frame, values=tables, state='readonly')
        self.table_combo.pack(pady=5)

        tk.Label(self.current_frame, text="Количество записей:").pack(pady=5)
        self.num_records_entry = tk.Entry(self.current_frame)
        self.num_records_entry.pack(pady=5)

        tk.Label(self.current_frame, text=f"Максимально допустимое количество записей: {MAX_RECORDS}",
                 foreground='red').pack(pady=5)

        tk.Button(self.current_frame, text="Генерировать и Вставить", command=self.generate_and_insert).pack(pady=10)
        tk.Button(self.current_frame, text="Назад", command=self.create_database_selection_frame).pack(pady=5)
        tk.Button(self.current_frame, text="Выйти", command=self.exit_app).pack(pady=5)

        tk.Label(self.current_frame, text="Лог:").pack(pady=5)
        self.log_text = tk.Text(self.current_frame, height=10)
        self.log_text.pack(pady=5)

    def generate_and_insert(self):
        table = self.table_combo.get()
        num_records = self.num_records_entry.get()

        if not table:
            messagebox.showerror("Ошибка", "Пожалуйста, выберите таблицу.")
            return

        if not num_records:
            messagebox.showerror("Ошибка", "Пожалуйста, введите количество записей.")
            return

        try:
            num = int(num_records)
            if num <= 0:
                messagebox.showerror("Ошибка", "Количество записей должно быть положительным числом.")
                return
            if num > MAX_RECORDS:
                messagebox.showerror("Ошибка", f"Максимально допустимое количество записей: {MAX_RECORDS}.")
                return
        except ValueError:
            messagebox.showerror("Ошибка", "Пожалуйста, введите корректное числовое значение для количества записей.")
            return

        self.log_text.insert(tk.END, f"Начата генерация {num} записей для таблицы '{table}'...\n")
        self.log_text.see(tk.END)

        records = self.db_manager.generate_records(table, num, schema='dbo')
        if not records:
            self.log_text.insert(tk.END, f"Нет данных для вставки в таблицу '{table}'.\n")
            self.log_text.see(tk.END)
            return

        queries = self.db_manager.generate_insert_queries(table, records, schema='dbo')
        if not queries:
            self.log_text.insert(tk.END, f"Нет запросов для выполнения для таблицы '{table}'.\n")
            self.log_text.see(tk.END)
            return

        self.db_manager.execute_queries(queries)
        fake.unique.clear()

        self.log_text.insert(tk.END, f"Завершена генерация и вставка данных для таблицы '{table}'.\n")
        self.log_text.see(tk.END)

    def exit_app(self):
        self.db_manager.close_connection()
        self.root.destroy()
