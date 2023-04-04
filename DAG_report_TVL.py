# pip install telegram
# pip install python-telegram-bot

import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
#from read_db.CH import Getch

import pandahouse as ph
from datetime import datetime, timedelta
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Подключение к данным Симулятора
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230220',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'n.tavlintseva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 18),
}

# Подключение к боту
my_token = '6251444710:AAH1dTGphjwO0HAHkfgNspTCC562bNKhb8g'
bot = telegram.Bot(token=my_token)
chat_id = -802518328


# Интервал запуска DAG
schedule_interval = '0 11 * * *'


# Создаем сообщение от бота
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_report_TVL():
    
    # показатели за прошлый день
    @task
    def extract_dau():
        query_dau ='''SELECT toDate(time) AS date,
                            COUNT(DISTINCT user_id) AS DAU,
                            countIf(user_id, action = 'view') as views,
                            countIf(user_id, action = 'like') as likes,
                            ROUND(countIf(user_id, action = 'like')/countIf(user_id, action = 'view'), 4) as CTR
                    FROM simulator_20230220.feed_actions
                    WHERE toDate(time) = yesterday()
                    GROUP BY toDate(time)
                    ORDER BY DAU DESC
        '''
        df_dau = ph.read_clickhouse(query = query_dau, connection=connection)
        return df_dau
    
    #показатели за неделю
    @task  
    def extract_wau():
        query_wau ='''SELECT toDate(time) AS date,
                             count(DISTINCT user_id) AS WAU,
                             countIf(user_id, action = 'view') as views,
                             countIf(user_id, action = 'like') as likes,
                             countIf(user_id, action = 'like')/countIf(user_id, action = 'view') as CTR
                      FROM simulator_20230220.feed_actions
                      WHERE toDate(time) between yesterday() - 6 and yesterday()
                      GROUP BY toDate(time)
                      ORDER BY toDate(time)
        '''
        df_wau = ph.read_clickhouse(query = query_wau, connection=connection)
        return df_wau
    
    #Отправляем текстовое сообщение за прошлый день
    @task  
    def text_report_dau(df_dau):
        dau_dau = df_dau.loc[0,'DAU']
        views_dau = df_dau.loc[0,'views']
        likes_dau = df_dau.loc[0,'likes']
        ctr_dau = df_dau.loc[0,'CTR']
        date_dau=df_dau.loc[0,'date'].strftime('%Y-%m-%d')
        text_report = f'''Аналитическая сводка по ленте новостей за {date_dau} : DAU - {dau_dau}, Просмотры - {views_dau}, Лайки - {likes_dau}, CTR -  {ctr_dau}'''
        bot.sendMessage(chat_id=chat_id, text=text_report)
        
    # Отправляем метрики за неделю
    @task
    def plt_report_week(df_wau):
        fig, ax = plt.subplots(nrows=3, ncols = 1, figsize=(9, 15))
        
        ax[0].set_title('Активные пользователей за неделю', fontsize = 19) 
        ax[0].set_ylabel('DAU')
        ax[0].grid(axis = 'y')
        ax[0].plot(df_wau['date'],df_wau['WAU'], marker = 'o', markersize = 5, color = 'purple')
        
        ax[1].set_title('Просмотры и лайки за неделю', fontsize = 19)
        ax[1].set_ylabel('Actions')
        ax[1].grid(axis = 'y')
        ax[1].plot(df_wau['date'],df_wau['likes'], marker = 'o', markersize = 5, color='purple')
        ax[1].plot(df_wau['date'],df_wau['views'], marker = 'o', markersize = 5, color='g')
        ax[1].legend(['likes', 'views'], loc=1)
        
        ax[2].set_title('CTR за неделю', fontsize = 19)
        ax[2].set_ylabel('CTR')
        ax[2].grid(axis = 'y')
        ax[2].plot(df_wau['date'],df_wau['CTR'], marker = 'o', markersize = 5, color='purple')
        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'plt_report_week.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        
    df_dau=extract_dau()
    df_wau=extract_wau()
    text_report_dau(df_dau)
    plt_report_week(df_wau)
    
dag_report_TVL=dag_report_TVL()