# -*- coding: utf-8 -*-
import pandas as pd
from pathlib import Path


def columns_change(df):
    """
    Функция меняет заголовок полученного в аргументе dataframe.
    Убирает лишние символы (<, >)
    Приводит названия к нижнему регистру
    :param df:
    :return:
    """
    title_lst = df.columns.values  # Создаем список с названиями заголовков принятого в аргументе dataframe
    # В списке title_lst_new будут новые названия колонок
    title_lst_new = [x.replace('<', '') for x in title_lst]  # Удаляем символ '<' в каждом элементе списка
    title_lst_new = [x.replace('>', '') for x in title_lst_new]  # Удаляем символ '>' в каждом элементе списка
    title_lst_new = [x.lower() for x in title_lst_new]  # Приводим список с новыми заголовками к нижнему регистру
    rename_dic = dict(zip(title_lst, title_lst_new))  # Создаем словарь для переименования заголовка
    df = df.rename(columns=rename_dic)
    return df


def date_time_join(df):
    """
    Функция добавляет колонку date_time методом слияния <DATE> и <TIME>, и делает её индексом
    :param df: На вход получаем dataframe
    :return: Возвращаем измененый dataframe
    """
    df['date_time'] = df['<DATE>'].astype(str) + ' ' + df['<TIME>'].astype(str)  # Слияние столбцов в поле date_time
    df = df.set_index(pd.DatetimeIndex(df['date_time']))  # Меняем индекс и делаем его типом date
    df = df.drop('date_time', 1)  # Удаляем ненужную колонку. 1 означает, что отбрасываем колонку а не индекс
    return df


if __name__ == '__main__':
    dir_source = 'c:/data_finam_quote_csv'  # Папка откуда берем csv файлы для обработки
    file_mask = 'SPFB.RTS_5min_*.csv'  # Маска файлов, которые обрабатываем

    # Создаем пустой dataframe в который будем добавлять dataframe из прочитанных файлов (обработанные)
    df_res = pd.DataFrame()
    # Или открываем csv файл

    file_lst = list(Path(dir_source).glob(file_mask))  #
    # print(file_lst)

    for file in file_lst:
        # Загружаем файл в DF
        df_quote = pd.read_csv(file, delimiter=',')

        df_quote = date_time_join(df_quote)  # Меняем индекс в dataframe на дату и время
        df_quote = columns_change(df_quote)  # Меняем названия колонок
        df_res = df_res.combine_first(df_quote)  # Слияние двух dataframe
        # print(df_res)

    print(df_res)

    # # Добавление dataframe
    # df_quote = pd.read_csv('c:/data_finam_quote_csv/SPFB.RTS_5min_200902.csv', delimiter=',')
    # df_quote = date_time_join(df_quote)  # Меняем индекс в dataframe на дату и время
    # df_quote = columns_change(df_quote)  # Меняем названия колонок
    # df_res = df_res.combine_first(df_quote)  # Слияние двух dataframe
    # print(df_res)
    #
    # # Добавление повторно dataframe
    # df_quote = pd.read_csv('c:/data_finam_quote_csv/SPFB.RTS_5min_200901.csv', delimiter=',')
    # df_quote = date_time_join(df_quote)  # Меняем индекс в dataframe на дату и время
    # df_quote = columns_change(df_quote)  # Меняем названия колонок
    # df_res = df_res.combine_first(df_quote)  # Слияние двух dataframe
    # print(df_res)

    # Слияние dataframe без повторения данных возможно при помощи .combine_first()
