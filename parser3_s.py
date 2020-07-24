#!/usr/bin/python3.6
# -*- coding: utf-8 -*-

import psycopg2
import pandas as pd
import xlrd
import requests

file_name = '05-03 оборот розничной торговли непродовольственными.xls';
full_file_name = r'/home/ubuntu/ProjectBank/' + file_name

message = "Загрузка файла '{0}' завершена успешно ".format(file_name);

wasError = False
checkNewData = False

connection = psycopg2.connect(
    host = 'database-1.cvhbroxwkf5v.us-east-1.rds.amazonaws.com',
    port = 5432,
    user = 'postgres',
    password = 'p1o2s3t4g5r6e7s8',
    database='postgres' )
connection.autocommit=False
cursor = connection.cursor()

xls = xlrd.open_workbook(full_file_name, on_demand=True)
sheets = xls.sheet_names()
codes = ['оборот_розничной_торговли_непродовольственными_млн',
         'оборот_розничной_торговли_непродовольственными_%_соотв_месяц',
         'оборот_розничной_торговли_непродовольственными_млн_соотв_период',
         'оборот_розничной_торговли_непродовольственными_%_пред_месяц']

command = "INSERT INTO data.incoming_files (filename, uploaded_date , status) VALUES ('{0}', current_timestamp, false) RETURNING ID".format(file_name);
cursor.execute(command);
connection.commit()
xfile_id = cursor.fetchone()[0];

def executeCommandSelect(command):
    cursor.execute(command)
    result = cursor.fetchone()
    if result is None:
        raise psycopg2.DatabaseError("Command '{0}' return null".format(command))
    else:
        return result[0]

def parsing_sheet(n):
    sheet_name = sheets[n]
    code = codes[n]
    print('sheet: {0}; code: {1}'.format(sheet_name, code))
    file = pd.read_excel(full_file_name, header=[0, 1, 2, 3], sheet_name=sheet_name, index_col=0);
    file.drop(file.tail(len(file.index) - 98).index, inplace=True)

    # Получаем indicator_id
    command = "select id from data.indicators where code = '{0}'".format(code);
    xindicator_id = executeCommandSelect(command)

    # Получение данных
    try:
        wasError = False
        for level in file.columns:
            column_3 = level[2]
            column_4 = level[3]
            command = "select period_id from data.mapping_xls_period where xls_filename = '{0}' and xls_value_year = '{1}' and xls_value = '{2}'".format(file_name, column_3, column_4);
            xperiod_id = executeCommandSelect(command)
            for indx in range(len(file.index)):
                command = "select region_id from data.mapping_xls_region where xls_filename = '{0}' and xls_value = '{1}'".format(file_name, file.index[indx]);
                xregion_id = executeCommandSelect(command)
                if xregion_id is None:
                    continue
                xvalue = file.iloc[indx][level]
                if pd.isna(xvalue):
                    continue

                command = "select value from data.region_period_indicators where region_id = '{0}' and period_id = '{1}' and indicator_id = '{2}'".format(xregion_id, xperiod_id, xindicator_id)
                checkValue = executeCommandSelect(command)
                if checkValue is not None:
                    continue
                else:
                    checkNewData = True
                    #Для теста
                    print('file[{0}][{1}] = {2}; period: {3}; region: {4}; indicator: {5}'.format(file.index[indx], level, xvalue, xperiod_id, xregion_id, xindicator_id))
                    command = "INSERT INTO data.region_period_indicators(region_id, indicator_id, period_id, value, file_id) VALUES ({0}, {1}, {2}, {3}, {4})".format(xregion_id, xindicator_id, xperiod_id, xvalue, xfile_id);
                    cursor.execute(command)
                    connection.commit()

    except Exception as error:
        wasError = True
        connection.commit()
        message = "Ошибка транзакции. Отмена всех других операций транзакции. "
        print("Error in transaction. Stop parsing. Reverting all other operations of a transaction. ", error)
        command = "delete from data.region_period_indicators where file_id = {0}".format(xfile_id)
        cursor.execute(command)
        connection.commit()
    return wasError

for i in range(len(sheets)):
    wasError = parsing_sheet(i)
    if wasError == True:
        break

if wasError == False and checkNewData == True:
    command = "UPDATE data.incoming_files SET status = true WHERE ID = {0}".format(xfile_id);
    cursor.execute(command)
    connection.commit()
cursor.close()
connection.close()

#бот
BOT_TOKEN = "968097013:AAGfYL_p6CJmfcZctBN81MwEsmgZ4zeENX0"
admin_id = -448040669
API_LINK = "https://api.telegram.org/bot"

send_message = requests.get(API_LINK + BOT_TOKEN + f"/sendMessage?chat_id={admin_id}&text={message}")
