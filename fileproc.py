import pandas as pd
import processing
import os
from typing import Iterable
import gc
import warnings



warnings.filterwarnings('ignore', category=UserWarning, module=r'.*fileproc')


# читать исходный файл в чанках. идентифицировать предметы поиска в каждой строке и записать в
# новый временный файл.
def identify_queries_csv(task_file: str, output_file='files/processed_data_temp.csv', enc='utf-8'):
    # проверить наличие файла
    if not os.path.exists(task_file):
        print('Ошибка: Файл', task_file, 'не найден!')
        return

    chunksize = 10 ** 2

    dtype_dict = {
        'shutdown_id': 'int64',
        'comment': 'string',
    }

    counter = 0
    # создать временный файл для первичной обработки
    for chunk in pd.read_csv(task_file, chunksize=chunksize, sep=';',
                             header=0, encoding=enc, dtype=dtype_dict):
        counter += 1
        print(f'Обрабатывается раздел {str(counter)}...')

        chunk['comment'] = chunk['comment'].apply(processing.process_comment)

        # записать информацию во временный файл
        write_output(chunk, output_file, header=['shutdown_id', 'queries'])

        del chunk
        gc.collect()

    del processing.nlp
    return


# произвести запись чанка данных в файл формата csv
def write_output(df: pd.DataFrame, output_file: str, header: Iterable[str], enc='utf-8'):

    if not os.path.exists(output_file):  # при отсутствии файла создать с заголовком
        print(f'Создается файл вывода {output_file}...')
        df.to_csv(output_file, header=header, index=False, sep=';', encoding=enc)

        return

    print(f'Дополняется файл вывода {output_file}...')
    df.to_csv(output_file, mode='a', header=False, index=False, sep=';', encoding=enc)

    return


def search_addresses(addresses_file, output_file,
                     queries_file='files/processed_data_temp.csv', enc='utf-8'):
    # проверить наличие файлов
    if not os.path.exists(queries_file):
        print('Ошибка: Файл', queries_file, 'не найден!')
        return

    elif not os.path.exists(addresses_file):
        print('Ошибка: Файл', addresses_file, 'не найден!')
        return

    # поиск по чанкам
    chunksize = 10 ** 1
    counter = 0

    for chunk in pd.read_csv(queries_file, chunksize=chunksize, sep=';', header=0, encoding=enc):
        counter += 1
        print(f'Поиск раздела запросов {counter}...')

        chunk['queries'] = chunk['queries'].apply(get_uuids, args=(addresses_file, enc))

        write_output(chunk, output_file, header=['shutdown_id', 'house_uuids'])

        del chunk
        gc.collect()

    # удалить временный файл
    # os.remove(queries_file)

    return


def get_uuids(query: str, addresses_file, enc='utf-8'):
    if not query or pd.isna(query):
        return ''

    # поиск паттерна по базе адресов
    matched_rows = []
    for search_chunk in pd.read_csv(addresses_file, chunksize=10 ** 8, sep=';', header=0,
                                    encoding=enc):
        mask = search_chunk['house_full_address'].str.contains(query, na=False,
                                                               case=False, regex=True)
        matched_rows += (search_chunk[mask]['house_full_address'].tolist())  # change to house uuids

    return ','.join(matched_rows)
