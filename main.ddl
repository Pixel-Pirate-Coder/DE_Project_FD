-- STG таблицы

-- Таблица stg_transactions
CREATE TABLE IF NOT EXISTS {STG.transactions} (
    transaction_id VARCHAR(12) PRIMARY KEY,
    transaction_date TIMESTAMP, 
    amount DECIMAL,
    card_num VARCHAR(20),
    oper_type VARCHAR(8),
    oper_result VARCHAR(8),
    terminal VARCHAR(5)
);

-- Таблица stg_terminals
CREATE TABLE IF NOT EXISTS {STG.terminals} (
    terminal_id VARCHAR(5),
    terminal_type VARCHAR(5),
    terminal_city VARCHAR(20),
    terminal_address VARCHAR(50),
    date DATE
);

-- Таблица stg_blacklist
CREATE TABLE IF NOT EXISTS {STG.blacklist} (
    date DATE,
    passport VARCHAR(15)
);

-- Таблица stg_clients
CREATE TABLE IF NOT EXISTS {STG.clients} (
    client_id VARCHAR(10) PRIMARY KEY,
    last_name VARCHAR(20),
    first_name VARCHAR(20),
    patronymic VARCHAR(20),
    date_of_birth DATE,  
    passport_num VARCHAR(15), 
    passport_valid_to DATE,   
    phone VARCHAR(16),
    create_dt TIMESTAMP,
    update_dt TIMESTAMP
);

-- Таблица stg_accounts
CREATE TABLE IF NOT EXISTS {STG.accounts} (
    account VARCHAR(20) PRIMARY KEY,
    valid_to DATE,   
    client VARCHAR(10),
    create_dt TIMESTAMP,
    update_dt TIMESTAMP
);

-- Таблица stg_cards
CREATE TABLE IF NOT EXISTS {STG.cards} (
    card_num VARCHAR(20) PRIMARY KEY,
    account VARCHAR(20),
    create_dt TIMESTAMP,
    update_dt TIMESTAMP
);

-- DIM таблицы

-- Таблица dwh_dim_terminals_hist
CREATE TABLE IF NOT EXISTS {DIM.terminals} (
    terminal_id VARCHAR(5),
    terminal_type VARCHAR(5),
    terminal_city VARCHAR(20),
    terminal_address VARCHAR(50),
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    deleted_flg BOOLEAN NOT NULL DEFAULT FALSE
);

-- Таблица dwh_dim_clients_hist
CREATE TABLE IF NOT EXISTS {DIM.clients} (
    client_id VARCHAR(10),
    last_name VARCHAR(20),
    first_name VARCHAR(20),
    patronymic VARCHAR(20),
    date_of_birth DATE, 
    passport_num VARCHAR(15),
    passport_valid_to DATE, 
    phone VARCHAR(16),
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    deleted_flg BOOLEAN NOT NULL DEFAULT FALSE
);

-- Таблица dwh_dim_accounts_hist
CREATE TABLE IF NOT EXISTS {DIM.accounts} (
    account_num VARCHAR(20),
    valid_to DATE, 
    client VARCHAR(10),
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    deleted_flg BOOLEAN NOT NULL DEFAULT FALSE
);

-- Таблица dwh_dim_cards_hist
CREATE TABLE IF NOT EXISTS {DIM.cards} (
    cards_num VARCHAR(20),
    account_num VARCHAR(20),
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    deleted_flg BOOLEAN NOT NULL DEFAULT FALSE
);

-- FACT таблицы

-- Таблица dwh_fact_transactions
CREATE TABLE IF NOT EXISTS {FACT.transactions} (
    trans_id VARCHAR(12) PRIMARY KEY,
    trans_date TIMESTAMP, 
    card_num VARCHAR(20),
    oper_type VARCHAR(8),
    amt DECIMAL,
    oper_result VARCHAR(8),
    terminal VARCHAR(5)
);

-- Таблица dwh_fact_passport_blacklist
CREATE TABLE IF NOT EXISTS {FACT.blacklist} (
    passport_num VARCHAR(15),
    entry_dt DATE,
    PRIMARY KEY (passport_num, entry_dt)
);

-- Таблица-отчёт

-- Таблица rep_fraud
CREATE TABLE IF NOT EXISTS {REP.fraud} (
    event_dt TIMESTAMP, 
    passport VARCHAR(15),
    fio VARCHAR(65),
    phone VARCHAR(16),
    event_type VARCHAR(50),
    report_dt TIMESTAMP 
);

-- Таблица meta_info
CREATE TABLE IF NOT EXISTS {META.meta} (
    table_name VARCHAR(30),
    max_update_dt TIMESTAMP(0)
);

INSERT INTO {META.meta} (table_name, max_update_dt)
SELECT new_tables.table_name, to_timestamp('1900-01-01', 'YYYY-MM-DD')
FROM (
    SELECT '{STG.transactions}' AS table_name UNION ALL
    SELECT '{STG.terminals}' UNION ALL
    SELECT '{STG.blacklist}' UNION ALL
    SELECT '{STG.clients}' UNION ALL
    SELECT '{STG.accounts}' UNION ALL
    SELECT '{STG.cards}'
) AS new_tables
WHERE NOT EXISTS (
    SELECT 1
    FROM {META.meta}
    WHERE table_name = new_tables.table_name
);