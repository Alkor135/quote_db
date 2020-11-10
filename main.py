# -*- coding: utf-8 -*-
import pandas as pd
import sqlite3

if __name__ == '__main__':
    # Загружаем файл в DF
    df_min = pd.read_csv('c:/data_finam_quote_csv/SPFB.RTS_20200901_20200901_5min.csv', delimiter=',')
    # print(df_min)
    columns_lst = df_min.columns.values.tolist()  # Список имен заголовка
    # print(columns_lst)

    # Создание БД и(или) подключение к ней
    con = sqlite3.connect("c:/quote_db/my-test.db")  # или :memory: чтобы сохранить в RAM

    # Создание таблицы и запись туда dataframe
    df_min.to_sql('rts_5min', con)

    # Добавление dataframe
    df_min = pd.read_csv('c:/data_finam_quote_csv/SPFB.RTS_20200803_20200803_5min.csv', delimiter=',')
    df_min.to_sql('rts_5min', con, if_exists='append')

    # Добавление повторно dataframe, т.к. индекс не уникальный легко добавляются
    # TODO нужно исправлять повторное добавление одинаковых данных
    df_min = pd.read_csv('c:/data_finam_quote_csv/SPFB.RTS_20200901_20200901_5min.csv', delimiter=',')
    df_min.to_sql('rts_5min', con, if_exists='append')
