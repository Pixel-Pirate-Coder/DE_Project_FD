import os
import pandas as pd
import re
import shutil
from datetime import datetime

def find_matching_filepaths(source_dir, pattern):
    """Находит пути к файлам в директории, которые соответствуют заданному регулярному выражению."""
    regex = re.compile(pattern)
    return [
        os.path.join(dirpath, filename)
        for dirpath, _, filenames in os.walk(source_dir)
        for filename in filenames if regex.match(filename)
    ]

def load_data_from_files(source_dir, file_patterns, csv_sep = ";"):
    """Загружает данные из файлов, соответствующих заданным паттернам, в pandas DataFrames."""
    dataframes = {}

    for table_name, pattern in file_patterns.items():
        filepaths = find_matching_filepaths(source_dir, pattern)

        for filepath in filepaths:
            curr_data = None

            if filepath.endswith(".xlsx"):
                curr_data = pd.read_excel(filepath, header=0)
            elif filepath.endswith(('.csv', '.txt')):
                curr_data = pd.read_csv(filepath, header=0, sep=csv_sep)

            if curr_data is not None:
                date = extract_date_from_path(filepath)
                curr_data["source_path"] = [filepath] * len(curr_data)

                if date in dataframes:
                    dataframes[date].update({table_name: curr_data})
                else:
                    dataframes[date] = {table_name: curr_data}

    dataframes = dict(sorted(dataframes.items(), key=lambda x: x[0]))

    return dataframes

def prepare_data(data, prep_config):
    """Подготавливает данные, применяя конфигурацию очистки для каждой таблицы."""
    prepared_data = {}

    for dt, tables in data.items():
        for table_name, df in tables.items():
            table_prep_config = prep_config.get(table_name, {})

            numeric_cols = table_prep_config.get("numeric_cols", [])
            add_cols = table_prep_config.get("add_cols", [])
            rm_cols = table_prep_config.get("rm_cols", [])
            if numeric_cols:
                df = clean_numeric_columns(df, numeric_cols)
            if add_cols:
                df = add_columns(df, add_cols)
            if rm_cols:
                df = df.drop(columns=[col for col in rm_cols if col in df.columns], errors='ignore')
            if dt not in prepared_data:
                prepared_data[dt] = {}
            prepared_data[dt][table_name] = df

    prepared_data = dict(sorted(prepared_data.items(), key=lambda x: x[0]))

    return prepared_data

def add_columns(df, cols):
    """Добавляет указанные колонки в DataFrame."""
    if "date" in cols:
        if "source_path" in df.columns:
            df["date"] = df["source_path"].apply(extract_date_from_path)
        else:
            raise KeyError("Column 'source_path' is required to extract dates.")
    return df

def extract_date_from_path(path, pattern = r"(\d{2})(\d{2})(\d{4})", date_format = "%d%m%Y"):
    """Извлекает дату из строки пути к файлу с использованием регулярного выражения."""
    match = re.search(pattern, path)
    if not match:
        raise ValueError(f"No date found in path: {path}")

    return datetime.strptime(''.join(match.groups()), date_format)

def clean_numeric_columns(df, cols):
    """Очищает числовые колонки, стандартизируя их формат."""
    for col in cols:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(",", ".", regex=False)  # Заменяет запятую на точку
                .str.replace(r"[^\d.]", "", regex=True)  # Удаляет все символы, кроме цифр и точки
            )
    return df

def move_files_to_archive(data_folder, archive_folder, patterns):
    """Перемещает файлы, соответствующие паттернам, из data_folder в archive_folder."""
    if not os.path.exists(archive_folder):
        os.makedirs(archive_folder)

    for pattern_name, pattern in patterns.items():
        matching_files = find_matching_filepaths(data_folder, pattern)
        for file_path in matching_files:
            filename = os.path.basename(file_path)
            new_filename = f"{filename}.backup"
            archive_path = os.path.join(archive_folder, new_filename)
            shutil.move(file_path, archive_path)