# DE_Project_FD

### Основной python-скрипт

Скрипт main.py запускает процесс обработки данных и загрузки их в хранилище данных (DWH). Он выполняет следующие задачи:  

1. Загрузка конфигураций и переменных окружения:

  - Загружает переменные окружения из файла `.env`, содержащие параметры подключения к базе данных;
  - Загружает конфигурацию из файла `conf.yaml`, которая определяет параметры для обработки данных и схем базы данных.

2. Инициализация подключений к базам данных:

  - Создаются объекты для подключения к банковской базе данных и DWH;
  - Используются классы `BankDBClient` и `DWHClient` для взаимодействия с базами данных и схемами, указанными в конфигурации.

3. Создание схемы DWH:

  - Метод `create_schema("main.ddl")` используется для инициализации таблиц в DWH на основе SQL-скрипта из файла `main.ddl`.
   
4. Загрузка и подготовка данных:

  - Загружаются данные с помощью функции `load_data_from_files`, используя пути и шаблоны из конфигурации;
  - Данные подготавливаются с использованием функции `prepare_data`, которая применяет настройки из секции предобработки в конфигурации.
   
5. Загрузка данных в DWH и обработка мошенничества:

  - Подготовленные данные вставляются в соответствующие таблицы DWH через метод `insert_incoming_tables`;
  - Для каждого дня данных выполняется проверка на 4 типа мошенничества с использованием методов, таких как `insert_blacklist_fraud`, `insert_invalid_contract_fraud` и других.

6. Перемещение и архивирование файлов:

  - После обработки данные из папки `data` переносятся в папку `archive`, при этом к каждому файлу добавляется суффикс `.backup` для обозначения того, что он был обработан.

### Вспомогательные python-файлы

В скрипте используются функции и классы из других Python файлов, таких как:

- `py_scripts.utils` — для загрузки и подготовки данных, а также перемещения файлов в архив.
- `py_scripts.model` — для работы с конфигурациями схемы базы данных.
- `py_scripts.client` — для работы с подключениями и взаимодействия с базами данных.

### Файл с настройкой планировщика задач

Файл `main.cron` содержит настройки для планировщика задач, который запускает скрипт `main.py` каждый день в 01:00.   
Предполагается, что данные загружаются каждый день в 00:00, одного часа должно хватить для их загрузки в БД.   

### Конфигурационный файл

Файл `conf.yaml` используется для конфигурации ETL-процессов. Он определяет:

- Директории данных и архивов: Указаны пути для исходных данных и их резервных копий;
- Таблицы: Названия и структуры таблиц для разных слоев данных (STG, DIM, FACT, REP, META);
- SCD2: Настройки обработки медленно изменяющихся измерений (SCD2);
- Маппинг полей: Сопоставление полей из источников данных с целевыми таблицами;
- Шаблоны файлов: Регулярные выражения для поиска файлов по именам;
- Предобработку данных: Описание столбцов для удаления, добавления или обработки.

Если схема проверки данных отличается от стандартной (проект проверялся в учебной БД), замените `public` на название нужной схемы в соответствующих секциях конфигурации.  

### Файл с переменными БД

Файл `.env` используется для хранения конфигурации подключения к базе данных. Он содержит:

- DB_NAME: Имя базы данных, к которой осуществляется подключение;    
- DB_HOST: Адрес хоста базы данных;  
- DB_USER: Имя пользователя для доступа к базе данных;  
- DB_PASS: Пароль пользователя для авторизации;  
- DB_PORT: Порт, на котором работает сервер базы данных.

Если планируется проверять проект на другой БД, необходимо поменять соответствующие перменные.

### Файл с созданием структуры БД

Файл `main.ddl` используется для создания структуры базы данных. Он содержит SQL-скрипты для:  

1) Создания таблиц:
  - STG (Staging): Временные таблицы для загрузки данных перед преобразованием;
  - DIM (Dimensions): Исторические измерения для аналитики (с поддержкой SCD2);
  - FACT (Facts): Фактические данные;
  - REP (Report): Таблица с отчетом по типам мошенничества;
  - META (Metadata): Таблица для отслеживания максимальной даты обновления данных.
    
2) Инициализации данных:  
Вставка записей в таблицу META.meta, чтобы задать начальную точку времени для таблиц STG.


