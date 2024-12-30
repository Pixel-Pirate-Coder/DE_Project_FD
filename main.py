import os
from dotenv import load_dotenv, find_dotenv
import yaml

from py_scripts.utils import load_data_from_files, prepare_data, move_files_to_archive
from py_scripts.model import BankSchema, DWHSchema
from py_scripts.client import DWHClient, BankDBClient

if __name__ == "__main__":
    # Загружаем переменные окружения из файла .env
    load_dotenv(find_dotenv())

    # Загружаем конфигурацию из объединенного файла conf.yaml
    with open("conf.yaml", "r") as conf_file:
        config = yaml.safe_load(conf_file)

    bank_client = None
    dwh_client = None

    try:
        # Получаем настройки для банковской базы данных и схемы
        bank_schema = BankSchema.from_yaml("conf.yaml")
        bank_client = BankDBClient(
            database=os.getenv("DB_NAME"),
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            port=os.getenv("DB_PORT"),
            schema=bank_schema
        )

        # Получаем настройки для схемы DWH
        dwh_schema = DWHSchema.from_yaml("conf.yaml")  # Используем конфиг из conf.yaml
        dwh_client = DWHClient(
            database=os.getenv("DB_NAME"),
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            port=os.getenv("DB_PORT"),
            schema=dwh_schema,
            scd2_config=config["scd2"],
            fact_mapping=config["fact_mapping"],
        )

        # Инициализируем схему DWH
        dwh_client.create_schema("main.ddl")

        # Вставляем данные из банковской базы в таблицы DWH
        dwh_client.insert_bank_tables(bank_client)

        # Получаем данные для загрузки
        incoming_data = load_data_from_files(config["data_dir"], config["patterns"])

        # Подготавливаем входные данные согласно конфигурации предобработки
        incoming_data = prepare_data(incoming_data, config["preprocess"])

        for date, data in incoming_data.items():
            # Вставляем подготовленные данные в таблицы DWH
            dwh_client.insert_incoming_tables(data, date)
            # Ищем все 4 типа мошенничества
            # Тип 1
            dwh_client.insert_blacklist_fraud()
            # Тип 2
            dwh_client.insert_invalid_contract_fraud()
            # Тип 3
            dwh_client.insert_transactions_in_different_cities_fraud()
            # Тип 4
            dwh_client.insert_amount_guessing_fraud()

        # Перемещаем файлы в archive, переименовываем и удаляем из data
        move_files_to_archive(config["data_dir"], config["archive_dir"], config["patterns"])

    finally:
        # Закрываем соединения с базами данных
        if bank_client:
            bank_client.close_connection()
        if dwh_client:
            dwh_client.close_connection()