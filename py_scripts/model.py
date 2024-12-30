import yaml
from pydantic import BaseModel, Field

class Schema(BaseModel):
    """Базовый класс для схемы, предназначенный для загрузки данных из YAML файла."""
    @classmethod
    def from_yaml(cls, file_path: str) -> "Schema":
        """Загружает данные из YAML файла и инициализирует объект схемы."""
        with open(file_path, "r") as file:
            config_data = yaml.safe_load(file)["tables"]
            return cls(**config_data)

class BankSchema(Schema):
    """Схема, хранящая имена таблиц банковской базы данных."""
    accounts: str  # Название таблицы с аккаунтами
    cards: str     # Название таблицы с картами
    clients: str   # Название таблицы с клиентами

class DIMTableNames(BaseModel):
    """Схема для хранения имен таблиц измерений (DIM)."""
    accounts: str = Field(..., description="Таблица аккаунтов")
    cards: str = Field(..., description="Таблица карт")
    clients: str = Field(..., description="Таблица клиентов")
    terminals: str = Field(..., description="Таблица терминалов")

class FACTTableNames(BaseModel):
    """Схема для хранения имен таблиц фактов (FACT)."""
    blacklist: str = Field(..., description="Таблица черного списка")
    transactions: str = Field(..., description="Таблица транзакций")

class STGTableNames(BaseModel):
    """Схема для хранения имен таблиц промежуточных данных (STG)."""
    accounts: str = Field(..., description="Таблица аккаунтов для промежуточных данных")
    blacklist: str = Field(..., description="Таблица черного списка для промежуточных данных")
    cards: str = Field(..., description="Таблица карт для промежуточных данных")
    clients: str = Field(..., description="Таблица клиентов для промежуточных данных")
    terminals: str = Field(..., description="Таблица терминалов для промежуточных данных")
    transactions: str = Field(..., description="Таблица транзакций для промежуточных данных")

class REPTableNames(BaseModel):
    """Схема для хранения имен таблиц отчетности (REP)."""
    fraud: str = Field(..., description="Таблица отчетности по мошенничеству")

class METATableNames(BaseModel):
    """Схема для хранения имен таблиц метаданных (META)."""
    meta: str = Field(..., description="Таблица с метаданными")

class DWHSchema(Schema):
    """Схема для хранения имен таблиц в базе данных хранилища данных (DWH)."""
    DIM: DIMTableNames    # Содержит имена таблиц измерений
    FACT: FACTTableNames  # Содержит имена таблиц фактов
    STG: STGTableNames    # Содержит имена таблиц промежуточных данных
    REP: REPTableNames    # Содержит имена таблицы отчетности
    META: METATableNames  # Содержит имена таблицы метаданных