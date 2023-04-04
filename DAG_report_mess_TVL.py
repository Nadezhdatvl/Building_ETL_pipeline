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

# Функция для отправки графиков
def send_photo(data, x, y, title, xlabel, ylabel, file_name, legend=[]):
    if data is None:
        plt.figure(figsize=(15, 9))
        sns.lineplot(x=x, y=y[0], linewidth=3, color ='purple')
        sns.lineplot(x=x, y=y[1], linewidth=3, color ='g')
        plt.grid(axis = 'y')
        plt.title(title, fontsize=22)
        plt.xlabel(xlabel, fontsize = 10)
        plt.ylabel(ylabel, fontsize = 10)
        plt.legend(labels=legend)
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = f'{file_name}.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    else:
        plt.figure(figsize=(15, 9))
        sns.lineplot(data=data, x=x, y=y, marker='o', linewidth=3, color = 'g')
        plt.grid(axis = 'y')
        plt.title(title, fontsize=22)
        plt.xlabel(xlabel,fontsize = 10)
        plt.ylabel(ylabel,fontsize = 10)
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = f'{file_name}.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

# Функция для отправки тепловых графиков
def send_heatmap(data, title, xlabel, ylabel, file_name):
    sns.set(style='ticks')
    plt.figure(figsize=(9, 15))
    plt.title(title, fontsize=18)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    sns.heatmap(data.T, mask=data.T.isnull(), annot=True, fmt='.1%', linewidths=0.1, cbar=False, cmap="PiYG")
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = f'{file_name}.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)        

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
def dag_report_mess_TVL():
    
    # Показатели за прошлую неделю по всему приложению по ленте новостей
    @task
    def extract_week_action():
        q_week_action ='''SELECT toDate(t.time) as event_date,
                                 t.os as os,
                                 t.source as source,
                                 count(distinct t.user_id) as DAU,
                                 countIf(t.action = 'view') as views,
                                 countIf(t.action = 'like') as likes,
                                 count(*) as events
                           FROM (SELECT *
                           FROM simulator_20230220.feed_actions
                           WHERE toDate(time) between yesterday() - 6 and yesterday()
                                  ) t
                           LEFT JOIN 
                           (SELECT *
                           FROM simulator_20230220.message_actions
                           WHERE toDate(time) between yesterday() - 6 and yesterday()
                                  ) t1
                           ON t.user_id = t1.user_id
                           GROUP BY event_date, os, source'''

        df_action = ph.read_clickhouse(query = q_week_action, connection=connection)
        df_action['event_date'] = pd.to_datetime(df_action['event_date']).dt.date
        return df_action
    
    # Показатели за прошлую неделю по всему приложению по мессенжеру
    @task
    def extract_week_mess():
        q_week_mess = ''' SELECT toDate(t1.time) as event_date,
                                 t1.os as os,
                                 t1.source as source,
                                 count(distinct t1.user_id) as DAU,
                                 count(distinct t1.reciever_id) as receivers,
                                 count(*) as events
                            FROM (SELECT *
                            FROM simulator_20230220.feed_actions
                            WHERE toDate(time) between yesterday() - 6 and yesterday()
                                    ) t
                            RIGHT JOIN 
                            (SELECT *
                            FROM simulator_20230220.message_actions
                            WHERE toDate(time) between yesterday() - 6 and yesterday()
                                    ) t1
                            ON t.user_id = t1.user_id
                            GROUP BY event_date, os, source
        '''
        df_mess = ph.read_clickhouse(query = q_week_mess, connection=connection)
        df_mess['event_date'] = pd.to_datetime(df_mess['event_date']). dt.date
        return df_mess
    
    # Общие метрики по всему приложению за неделю
    @task
    def extract_week_all():
        q_week_all = ''' SELECT toDate(t.time) as event_date,
                                t.os as os,
                                t.source as source,
                                count(distinct t.user_id) as DAU,
                                countIf(t.action = 'view') as views,
                                countIf(t.action = 'like') as likes,
                                count(distinct t1.user_id) as senders,
                                count(distinct t1.reciever_id) as receivers,
                                count(*) as events
                        FROM (SELECT *
                        FROM simulator_20230220.feed_actions
                        WHERE toDate(time) between yesterday() - 6 and yesterday()
                                    ) t
                        INNER JOIN 
                        (SELECT *
                        FROM simulator_20230220.message_actions
                        WHERE toDate(time) between yesterday() - 6 and yesterday()
                                    ) t1
                        ON t.user_id = t1.user_id
                        GROUP BY event_date, os, source
        '''
        df_all = ph.read_clickhouse(query = q_week_all, connection=connection)
        df_all['event_date']= pd.to_datetime(df_all['event_date']).dt.date
        return df_all
        
    
    # Показатели за неделю для ретеншена по ленте новостей
    @task  
    def extract_action_ret():
        q_action_ret ='''SELECT t1.date as event_date,
                                t.start_date as start_date,
                                t1.source as source,
                                count(t.user_id) as users
                        FROM (
                         SELECT user_id,
                                min(toDate(time)) as start_date
                        FROM simulator_20230220.feed_actions
                        GROUP BY user_id
                        HAVING toDate(time) between yesterday() - 6 and yesterday()
                                 ) t
                        INNER JOIN (
                        SELECT DISTINCT user_id,
                               source,
                                toDate(time) as date
                        FROM simulator_20230220.feed_actions
                        WHERE toDate(time) between yesterday() - 6 and yesterday()
                                ) t1
                        ON t.user_id = t1.user_id
                        GROUP BY event_date, start_date, source
            '''
        df_a_ret = ph.read_clickhouse(query = q_action_ret, connection=connection)
        df_a_ret['event_date']= pd.to_datetime(df_a_ret['event_date']).dt.date
        df_a_ret['start_date']= pd.to_datetime(df_a_ret['start_date']).dt.date
        return df_a_ret
    
    # Показатели за неделю для ретеншена по мессенжеру
    @task  
    def extract_mess_ret():
        q_mess_ret = ''' SELECT t1.date as event_date,
                                t.start_date as start_date,
                                t1.source as source,
                                count(t.user_id) as users
                        FROM (
                            SELECT user_id,
                                   min(toDate(time)) as start_date
                            FROM simulator_20230220.message_actions
                            GROUP BY user_id
                            HAVING toDate(time) between yesterday() - 6 and yesterday()
                                    ) t
                        INNER JOIN (
                            SELECT DISTINCT user_id,
                                   source,
                                   toDate(time) as date
                            FROM simulator_20230220.message_actions
                            WHERE toDate(time) between yesterday() - 6 and yesterday()
                                    ) t1
                            ON t.user_id = t1.user_id
                            GROUP BY event_date, start_date, source
        '''
        
        df_m_ret = ph.read_clickhouse(query = q_mess_ret, connection=connection)
        df_m_ret['event_date']= pd.to_datetime(df_m_ret['event_date']).dt.date
        df_m_ret['start_date']= pd.to_datetime(df_m_ret['start_date']).dt.date
        return df_m_ret
    
    
    # Основные показатели за прошедшую неделю на графиках
    @task  
    def report_for_all(df_action,df_mess,df_all):
        msg = f'''Доброе утро! Предлагаю ознакомиться с динамикой показателей всего приложения за прошедшую неделю'''
        
        bot.send_message(chat_id=chat_id, text=msg)
        
        group_df_action = df_action.groupby(['event_date'], as_index=False).agg({
            'DAU' : 'sum',
            'events' : 'sum',
            'views' : 'sum',
            'likes' : 'sum'
        }).copy()
        group_df_action['CTR'] = round(group_df_action['likes'] / group_df_action['views'], 4)
        group_df_action['events/user'] = round(group_df_action['events'] / group_df_action['DAU'], 2)
        
        group_df_mess = df_mess.groupby(['event_date'], as_index=False).agg({
            'DAU' : 'sum',
            'events' : 'sum',
            'receivers' : 'sum'
        }).copy()
        group_df_mess['messages/user'] = round(group_df_mess['events'] / group_df_mess['DAU'], 2)
        group_df_mess['receivers/user'] = round(group_df_mess['receivers'] / group_df_mess['DAU'], 2)
        
        group_df_all = df_all.groupby(['event_date'], as_index=False).agg({
            'DAU' : 'sum',
            'events' : 'sum',
            'views' : 'sum',
            'likes' : 'sum',
            'receivers' : 'sum'
        }).copy()
        
        DAU_for_all = (group_df_action.rename(columns={'DAU' : 'DAU_action'})['DAU_action'], group_df_mess.rename(columns={'DAU' : 'DAU_messages'})['DAU_messages'])
        send_photo(None, group_df_action['event_date'], DAU_for_all, 'Распределение DAU за неделю для каждого из сервисов', 'date', 'DAU', 'DAU_last_week', legend=['лента новостей', 'сообщения'])
        send_photo(group_df_all, 'event_date', 'DAU', 'DAU для пользователей ленты новостей и сообщений за неделю', 'date', 'DAU', 'all_data_DAU_last_week')
        send_photo(None, group_df_action['event_date'], (group_df_action['likes'], group_df_action['views']), 'Лента новостей - распределение лайков и просмотров за неделю', 'date', 'DAU', 'action_data_likes_and_views_last_week', legend=['лайки', 'просмотры'])
        send_photo(group_df_action, 'event_date', 'CTR', 'Лента новостей - распределение CTR за неделю', 'date', 'CTR', 'action_data_CTR_last_week')
        send_photo(group_df_action, 'event_date', 'events/user', 'Лента новостей - среднее число событий на пользователя за неделю', 'date', 'среднее число событий на пользователя', 'action_data_events_per_user_last_week')
        send_photo(group_df_mess,'event_date', 'messages/user', 'Сообщения - среднее число сообщений на пользователя за неделю', 'date', 'среднее число сообщений на пользователя', 'messages_data_messages_per_user_last_week')
        send_photo(group_df_mess,'event_date', 'receivers/user', 'Сообщения - среднее число получателей сообщений на отправителя за неделю', 'date', 'число получателей сообщений на отправителя', 'messages_data_receivers_per_user_last_week')
        return True
    
    # Основные показатели с разбивкой по ос на графиках
    @task()
    def action_report_os(df_action, task_finished):
        if task_finished:
            group_df_action = df_action.groupby(['event_date', 'os'], as_index=False).agg({
                'DAU' : 'sum',
                'events' : 'sum',
                'views' : 'sum',
                'likes' : 'sum'
            }).copy()
        
            group_df_action = group_df_action.assign(total_events=group_df_action.groupby(['event_date'], as_index=False)['events'].transform('sum'))
            group_df_action['events_pct'] = round(group_df_action['events'] / group_df_action.total_events, 2)
        
            color_rectangle = np.random.rand(2, 3)
            plot1 = group_df_action.pivot(index='event_date', columns='os', values='events_pct').reset_index() \
            .plot(
                x = 'event_date',
                kind = 'area',
                stacked = True,
                title = 'Лента новостей - распределение долей ОС по дням',
                mark_right = True,
                color = color_rectangle,
                figsize=(8, 5) )
        
            plot2 = group_df_action.pivot(index='event_date', columns='os', values='DAU').reset_index() \
            .plot(
                x = 'event_date',
                kind = 'line',
                title = 'Лента новостей - распределение DAU по дням и типу ОС',
                mark_right = True,
                color = color_rectangle,
                figsize=(8, 5),
                grid = True )
        
            group_df_action['CTR'] = round(group_df_action['likes'] / group_df_action['views'], 4)
            plot3 = group_df_action.pivot(index='event_date', columns='os', values='CTR').reset_index() \
            .plot(
                x = 'event_date',
                kind = 'line',
                title = 'Лента новостей - распределение CTR по дням и типу ОС',
                mark_right = True,
                color = color_rectangle,
                figsize=(8, 5),
                grid = True )
            
            plot_object1 = io.BytesIO()
            plot1.figure.savefig(plot_object1)
            plot_object1.seek(0)
            plot_object1.name = 'os_action_data1.png'
            bot.sendPhoto(chat_id=chat_id, photo=plot_object1)

            plot_object2 = io.BytesIO()
            plot2.figure.savefig(plot_object2)
            plot_object2.seek(0)
            plot_object2.name = 'os_action_data2.png'
            bot.sendPhoto(chat_id=chat_id, photo=plot_object2)

            plot_object3 = io.BytesIO()
            plot3.figure.savefig(plot_object3)
            plot_object3.seek(0)
            plot_object3.name = 'os_action_data3.png'
            bot.sendPhoto(chat_id=chat_id, photo=plot_object3)
            return True
        return False
    
    # Считаем ретеншен по ленте новостей и мессенжеру, отправляем хитмэп
    @task
    def retention_analysis(df_a_ret, df_m_ret, task_finished):
        if task_finished:
            ads_ret_action_df = df_a_ret[df_a_ret.source == 'ads'].copy()
            organic_ret_action_df = df_a_ret[df_a_ret.source == 'organic'].copy()
            ads_ret_mess_df = df_m_ret[df_m_ret.source == 'ads'].copy()
            organic_ret_mess_df = df_m_ret[df_m_ret.source == 'organic'].copy()
        
            def calculate_retention_rate(df):
                cohorts = df.groupby(['start_date', 'event_date']).agg({'users': 'sum'})
                
                def get_cohort_period(df):
                    df['day'] = np.arange(len(df))
                    return df
                
                cohorts = cohorts.groupby(level=0).apply(get_cohort_period)
                cohort_group_size = cohorts['users'].groupby(level=0).first()
                user_retention = cohorts['users'].unstack(0).divide(cohort_group_size, axis=1)
                return user_retention
            
            df_ads_action = calculate_retention_rate(ads_ret_action_df)
            df_organic_action = calculate_retention_rate(organic_ret_action_df)
            df_ads_message = calculate_retention_rate(ads_ret_mess_df)
            df_organic_message = calculate_retention_rate(organic_ret_mess_df)
        
            send_heatmap(df_ads_action, 'Retention rate для рекламных пользователей ленты', 'когорта', 'день', 'heatmap_retention_rate_ads_feed')
            send_heatmap(df_organic_action, 'Retention rate для органических пользователей ленты', 'когорта', 'день', 'heatmap_retention_rate_organic_feed')
            send_heatmap(df_ads_message, 'Retention rate для рекламных пользователей сообщений', 'когорта', 'день', 'heatmap_retention_rate_ads_message')
            send_heatmap(df_organic_message, 'Retention rate для органических пользователей сообщений', 'когорта', 'день', 'heatmap_retention_rate_organic_message')
            return True
        return False
        
    df_action = extract_week_action()
    df_mess = extract_week_mess()
    df_all = extract_week_all()
    df_a_ret = extract_action_ret()
    df_m_ret = extract_mess_ret()
    task_finished = report_for_all(df_action, df_mess, df_all)
    task_finished = action_report_os(df_action, task_finished)
    retention_analysis(df_a_ret, df_m_ret, task_finished)
    
dag_report_mess_TVL=dag_report_mess_TVL()