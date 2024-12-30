import logging
import pandas as pd

import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.extensions import connection as Connection

class Client:
    """Базовый класс клиента для взаимодействия с базой данных."""
    def __init__(self, database, host, user, password, port, schema):
        self.logger = logging.getLogger(__name__)
        self.connection: Connection = None
        self.schema = schema

        try:
            # Подключение к базе данных
            self.connection = psycopg2.connect(
                database=database,
                host=host,
                user=user,
                password=password,
                port=port,
            )
            self.connection.autocommit = False
        except Exception as e:
            print(e)
            raise

    def is_table_empty(self, table_name):
        """Проверяет, пустая ли таблица."""
        query = f"SELECT NOT EXISTS (SELECT 1 FROM {table_name} LIMIT 1);"
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchone()[0]

    def fetch_data_to_df(self, table_name):
        """Извлекает все данные из таблицы и возвращает их как pandas DataFrame."""
        query = f"SELECT * FROM {table_name};"
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=column_names)

    def insert_df_to_table(self, df, table_name):
        """Вставляет данные из pandas DataFrame в таблицу базы данных."""
        if df.empty:
            return

        columns = df.columns.tolist()
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
        values = [tuple(row) for row in df.itertuples(index=False, name=None)]

        with self.connection.cursor() as cursor:
            execute_batch(cursor, query, values)
            self.connection.commit()

    def clear_table(self, table_name):
        """Очищает все данные из указанной таблицы."""
        query = f"DELETE FROM {table_name};"
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()

    def insert_from_table_to_table(self, src_table, dest_table, mapping):
        """Вставляет данные из одной таблицы в другую с учетом соответствия столбцов."""
        src_cols = ', '.join(mapping.keys())
        dest_cols = ', '.join(mapping.values())
        conditions = ' AND '.join([f"dest.{dest_col} = src.{src_col}" for src_col, dest_col in mapping.items()])

        query = f"""
            INSERT INTO {dest_table} ({dest_cols})
            SELECT {src_cols}
            FROM {src_table} src
            WHERE NOT EXISTS (
                SELECT 1 FROM {dest_table} dest WHERE {conditions}
            );
        """

        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()

    def close_connection(self):
        """Закрывает соединение с базой данных."""
        if self.connection:
            try:
                self.connection.close()
            except Exception as e:
                print(f"Ошибка при закрытии соединения с базой данных: {e}", exc_info=True)

class BankDBClient(Client):
    """Клиент для взаимодействия с банковской базой данных. Например, получает информацию о клиентах из банковской базы данных."""
    def __init__(self, database, host, user, password, port, schema):
        """Инициализация экземпляра BankDBClient для взаимодействия с банковской базой данных."""
        super().__init__(database=database, host=host, user=user, password=password, port=port, schema=schema)

class DWHClient(Client):
    """Клиент для взаимодействия с базой данных хранилища данных (DWH)."""
    def __init__(self, database, host, user, password, port, schema, scd2_config = None, fact_mapping = None):
        super().__init__(database, host, user, password, port, schema)
        self.scd2_config = scd2_config or {}
        self.fact_mapping = fact_mapping or {}
        self.max_dt = "3000-01-01"
        self.min_dt = "1800-01-01"

    def create_schema(self, ddl_filepath):
        """Создает схему базы данных на основе DDL скрипта."""
        with open(ddl_filepath, 'r') as ddl_file:
            ddl_script = ddl_file.read().format(
                **{key: getattr(self.schema, key) for key in dir(self.schema) if not key.startswith('_')}
            )
        with self.connection.cursor() as cursor:
            cursor.execute(ddl_script)
            self.connection.commit()

    def insert_to_stg_table(self, field_name, data):
        """Вставляет данные в таблицу staging (временную таблицу)."""
        table_name = getattr(self.schema.STG, field_name, None)
        if table_name:
            self.clear_table(table_name)
            self.insert_df_to_table(data, table_name)
        else:
            print(f"Не найдена таблица staging для поля '{field_name}'.")

    def update_staging_timestamp_in_meta_table(self, upd_date, field_name):
        """Обновляет timestamp для стейдж-таблицы."""
        query_template = """
        UPDATE {meta_table_name}
        SET max_update_dt = to_timestamp('{upd_timestamp}', 'YYYY-MM-DD')
        WHERE table_name = '{stg_table_name}';
        """
        if hasattr(self.schema.STG, field_name):
            stg_table_name = self.schema.STG.__getattribute__(field_name)
            query = query_template.format(
                meta_table_name=self.schema.META.meta,
                stg_table_name=stg_table_name,
                upd_timestamp=upd_date.strftime("%Y-%m-%d")
            )
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                self.connection.commit()

    def insert_from_stg_table_to_dim_table(self, field_name, mapping, date_col, stg_pk, dim_pk):
        """Реализует логику SCD2 для обновления размерных таблиц из таблиц staging."""
        stg_table = getattr(self.schema.STG, field_name, None)
        dim_table = getattr(self.schema.DIM, field_name, None)

        if not stg_table or not dim_table:
            print(f"Неверная таблица staging или dimension для поля '{field_name}'.")
            return

        dim_cols = ', '.join(mapping.values())
        stg_cols = ', '.join([f"stg.{col}" for col in mapping.keys()] + [f"COALESCE(stg.\"{date_col}\", '{self.min_dt}')"])

        update_query = f"""
            UPDATE {dim_table}
            SET effective_to = stg.{date_col}, deleted_flg = TRUE
            FROM {stg_table} stg
            WHERE {dim_table}.{dim_pk} = stg.{stg_pk} 
              AND ({' OR '.join([f"{dim_table}.{dest} <> stg.{src}" for src, dest in mapping.items()])})
              AND {dim_table}.deleted_flg = FALSE;
        """

        insert_query = f"""
            INSERT INTO {dim_table} ({dim_cols}, effective_from, effective_to, deleted_flg)
            SELECT {stg_cols}, '{self.max_dt}', FALSE
            FROM {stg_table} stg
            LEFT JOIN {dim_table} dim
            ON stg.{stg_pk} = dim.{dim_pk} AND dim.deleted_flg = FALSE
            WHERE dim.{dim_pk} IS NULL;
        """

        with self.connection.cursor() as cursor:
            cursor.execute(update_query)
            cursor.execute(insert_query)
            self.connection.commit()

    def insert_bank_tables(self, bank_client):
        """Вставка данных в банковские таблицы, такие как accounts, clients, cards."""
        for dim_field_name, _ in self.schema.DIM:

            if hasattr(bank_client.schema, dim_field_name):

                bank_table_name = bank_client.schema.__getattribute__(dim_field_name)
                data = bank_client.fetch_data_to_df(bank_table_name)

                self.insert_to_stg_table(dim_field_name, data)

                scd2_config = self.scd2_config.get(dim_field_name)
                if scd2_config is not None:
                    self.insert_from_stg_table_to_dim_table(dim_field_name, **scd2_config)

    def insert_incoming_tables(self, incoming_data, date):
        """Вставка входящих данных в соответствующие таблицы."""
        for field_name, data in incoming_data.items():
            self.insert_to_stg_table(field_name, data)
            self.update_staging_timestamp_in_meta_table(date, field_name)
            scd2_config = self.scd2_config.get(field_name)
            if scd2_config is not None:
                self.insert_from_stg_table_to_dim_table(field_name, **scd2_config)
            fact_mapping = self.fact_mapping.get(field_name)
            if fact_mapping is not None:
                stg_table_name, fact_table_name = None, None
                if hasattr(self.schema.STG, field_name):
                    stg_table_name = self.schema.STG.__getattribute__(field_name)
                if hasattr(self.schema.FACT, field_name):
                    fact_table_name = self.schema.FACT.__getattribute__(field_name)
                if stg_table_name is not None and fact_table_name is not None:
                    self.insert_from_table_to_table(stg_table_name, fact_table_name, fact_mapping)

    def insert_blacklist_fraud(self):
        """Вставка данных о заблокированных или просроченных паспортах."""
        query = f"""
        INSERT INTO public.oled_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
        SELECT 
            t.trans_date AS event_dt,
            cl.passport_num AS passport,
            CONCAT(cl.last_name, ' ', cl.first_name, ' ', cl.patronymic) AS fio,
            cl.phone AS phone,
            'Заблокированный или просроченный паспорт' AS event_type,
            CURRENT_DATE AS report_dt
        FROM public.oled_dwh_fact_transactions t
        JOIN public.oled_dwh_dim_cards_hist c
            ON TRIM(t.card_num) = TRIM(c.cards_num) AND c.deleted_flg = False
        JOIN public.oled_dwh_dim_accounts_hist a
            ON c.account_num = a.account_num AND c.deleted_flg = False
        JOIN public.oled_dwh_dim_clients_hist cl
            ON a.client = cl.client_id AND c.deleted_flg = False
        JOIN public.oled_dwh_fact_passport_blacklist p
            ON cl.passport_num = p.passport_num
        WHERE (p.entry_dt <= t.trans_date OR cl.passport_valid_to <= t.trans_date)
        AND t.trans_date >= (
            SELECT MAX(max_update_dt) 
            FROM {self.schema.META.meta} 
            WHERE table_name = '{self.schema.STG.transactions}'
        );
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()

    def insert_invalid_contract_fraud(self):
        """Вставка данных о недействующем договоре."""
        query = f"""
        INSERT INTO public.oled_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
        SELECT 
            t.trans_date AS event_dt,
            cl.passport_num AS passport,
            CONCAT(cl.last_name, ' ', cl.first_name, ' ', cl.patronymic) AS fio,
            cl.phone AS phone,
            'Недействующий договор' AS event_type,
            CURRENT_DATE AS report_dt
        FROM public.oled_dwh_fact_transactions t
        JOIN public.oled_dwh_dim_cards_hist c
            ON TRIM(t.card_num) = TRIM(c.cards_num) AND c.deleted_flg = False
        JOIN public.oled_dwh_dim_accounts_hist a
            ON c.account_num = a.account_num AND a.deleted_flg = False
        JOIN public.oled_dwh_dim_clients_hist cl
            ON a.client = cl.client_id AND cl.deleted_flg = False
        WHERE a.valid_to <= t.trans_date
        AND t.trans_date >= (
            SELECT MAX(max_update_dt) 
            FROM {self.schema.META.meta} 
            WHERE table_name = '{self.schema.STG.transactions}'
        );
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()

    def insert_transactions_in_different_cities_fraud(self):
        """Вставка данных в операциях в разных городах за короткое время."""
        query = f"""
        WITH unique_cards AS (
            SELECT 
                a.client AS client_id,
                c.cards_num
            FROM public.oled_dwh_dim_cards_hist c
            JOIN public.oled_dwh_dim_accounts_hist a ON c.account_num = a.account_num AND a.deleted_flg = False
            JOIN public.oled_dwh_dim_clients_hist cl ON a.client = cl.client_id AND cl.deleted_flg = False
            GROUP BY a.client, c.cards_num
        ),
        filtered_transactions AS (
            SELECT 
                t.trans_date,
                t.card_num,
                term.terminal_city,
                cl.passport_num,
                CONCAT(cl.last_name, ' ', cl.first_name, ' ', cl.patronymic) AS fio,
                cl.phone
            FROM public.oled_dwh_fact_transactions t
            JOIN public.oled_dwh_dim_terminals_hist term ON t.terminal = term.terminal_id AND term.deleted_flg = False
            JOIN unique_cards uc ON TRIM(t.card_num) = TRIM(uc.cards_num)
            JOIN public.oled_dwh_dim_cards_hist c ON TRIM(t.card_num) = TRIM(c.cards_num) AND c.deleted_flg = False
            JOIN public.oled_dwh_dim_accounts_hist a ON c.account_num = a.account_num AND a.deleted_flg = False
            JOIN public.oled_dwh_dim_clients_hist cl ON a.client = cl.client_id AND cl.deleted_flg = FALSE
        )
        INSERT INTO public.oled_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
        SELECT DISTINCT 
            t1.trans_date AS event_dt,
            t1.passport_num AS passport,
            t1.fio,
            t1.phone,
            'Операции в разных городах за короткое время' AS event_type,
            CURRENT_DATE as report_dt
        FROM filtered_transactions t1
        JOIN filtered_transactions t2
            ON t1.passport_num = t2.passport_num
            AND t1.terminal_city != t2.terminal_city 
            AND ABS(EXTRACT(EPOCH FROM t2.trans_date) - EXTRACT(EPOCH FROM t1.trans_date)) <= 3600
        WHERE t1.trans_date >= (
            SELECT MAX(max_update_dt) 
            FROM {self.schema.META.meta} 
            WHERE table_name = '{self.schema.STG.transactions}'
        );
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()

    def insert_amount_guessing_fraud(self):
        """Вставка данных о попытке подбора суммы."""
        query = f"""
        WITH RECURSIVE ordered_transactions AS (
            SELECT 
                TRIM(t.card_num) AS card_num, 
                t.trans_date, 
                t.amt, 
                t.oper_result,
                ROW_NUMBER() OVER (PARTITION BY TRIM(t.card_num) ORDER BY t.trans_date) AS rn
            FROM public.oled_dwh_fact_transactions t
            JOIN public.oled_dwh_dim_cards_hist c
                ON TRIM(t.card_num) = TRIM(c.cards_num) AND c.deleted_flg = False
            WHERE t.trans_date >= (
                SELECT MAX(max_update_dt) 
                FROM {self.schema.META.meta} 
                WHERE table_name = '{self.schema.STG.transactions}'
            )
        ), 
        suspicious_sequences AS (
            SELECT 
                card_num, 
                trans_date AS start_date,
                trans_date AS end_date,
                amt,
                oper_result,
                rn,
                1 AS sequence_length,
                CASE WHEN oper_result = 'REJECT' THEN 1 ELSE 0 END AS reject_count
            FROM ordered_transactions
            UNION ALL
            SELECT 
                t.card_num, 
                s.start_date,
                t.trans_date,
                t.amt,
                t.oper_result,
                t.rn,
                s.sequence_length + 1,
                s.reject_count + CASE WHEN t.oper_result = 'REJECT' THEN 1 ELSE 0 END
            FROM ordered_transactions t
            JOIN suspicious_sequences s
                ON t.card_num = s.card_num AND t.rn = s.rn + 1
            WHERE t.trans_date - s.start_date <= INTERVAL '20 MINUTES'
                AND t.amt < s.amt
        ),
        final_suspicious_transactions AS (
            SELECT DISTINCT
                card_num,
                start_date,
                end_date
            FROM suspicious_sequences
            WHERE sequence_length >= 4
                AND reject_count >= 3
                AND oper_result = 'SUCCESS'
        ),
        filtered_suspicious_transactions AS (
            SELECT 
                o.card_num, 
                o.trans_date, 
                o.oper_result,
                ROW_NUMBER() OVER (PARTITION BY o.card_num, f.start_date ORDER BY o.trans_date DESC) AS row_desc
            FROM ordered_transactions o
            JOIN final_suspicious_transactions f
                ON o.card_num = f.card_num
            WHERE o.trans_date BETWEEN f.start_date AND f.end_date
        ),
        distinct_suspicious_transactions AS (
            SELECT 
                card_num,
                trans_date,
                oper_result
            FROM filtered_suspicious_transactions
            WHERE oper_result = 'SUCCESS' AND row_desc = 1
            UNION
            SELECT 
                card_num,
                trans_date,
                oper_result
            FROM filtered_suspicious_transactions
            WHERE oper_result = 'REJECT'
        )
        INSERT INTO public.oled_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
        SELECT 
            dst.trans_date AS event_dt,
            cl.passport_num AS passport,
            CONCAT(cl.last_name, ' ', cl.first_name, ' ', cl.patronymic) AS fio,
            cl.phone AS phone,
            'Попытка подбора суммы' AS event_type,
            CURRENT_DATE AS report_dt
        FROM distinct_suspicious_transactions dst
        JOIN public.oled_dwh_dim_cards_hist c
            ON TRIM(dst.card_num) = TRIM(c.cards_num) AND c.deleted_flg = False
        JOIN public.oled_dwh_dim_accounts_hist a
            ON c.account_num = a.account_num AND a.deleted_flg = False
        JOIN public.oled_dwh_dim_clients_hist cl
            ON a.client = cl.client_id AND cl.deleted_flg = False
        ORDER BY dst.trans_date;
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()
