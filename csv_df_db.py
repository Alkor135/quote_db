# -*- coding: utf-8 -*-
import pandas as pd
import sqlite3
from sqlalchemy import create_engine


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
    Функция добавляет колонку date_time и делает её индексом
    :param df: На вход получаем dataframe
    :return: Возвращаем измененый dataframe
    """
    df['date_time'] = df['<DATE>'].astype(str) + ' ' + df['<TIME>'].astype(str)  # Слияние столбцов в поле date_time
    df = df.set_index(pd.DatetimeIndex(df['date_time']))  # Меняем индекс и делаем её типом date
    df = df.drop('date_time', 1)  # Удаляем ненужную колонку. 1 означает, что отбрасываем колонку а не индекс
    return df


if __name__ == '__main__':
    # Загружаем файл в DF
    df_quote = pd.read_csv('c:/data_finam_quote_csv/SPFB.RTS_5min_200901.csv', delimiter=',')

    df_quote = date_time_join(df_quote)  # Меняем индекс в dataframe на дату и время

    df_quote = columns_change(df_quote)  # Меняем названия колонок
    # print(df_quote)

    # Создание БД и(или) подключение к ней
    con = sqlite3.connect("c:/data_quote_db/my-test.db")  # или :memory: чтобы сохранить в RAM

    # Создание таблицы и запись туда dataframe
    df_quote.to_sql('rts_5min', con, if_exists='append')

    # Добавление dataframe
    df_quote = pd.read_csv('c:/data_finam_quote_csv/SPFB.RTS_5min_200902.csv', delimiter=',')
    df_quote = date_time_join(df_quote)  # Меняем индекс в dataframe на дату и время
    df_quote = columns_change(df_quote)  # Меняем названия колонок
    df_quote.to_sql('rts_5min', con, if_exists='append')

    # Добавление повторно dataframe
    df_quote = pd.read_csv('c:/data_finam_quote_csv/SPFB.RTS_5min_200901.csv', delimiter=',')
    df_quote = date_time_join(df_quote)  # Меняем индекс в dataframe на дату и время
    df_quote = columns_change(df_quote)  # Меняем названия колонок
    df_quote.to_sql('rts_5min', con, if_exists='append')
