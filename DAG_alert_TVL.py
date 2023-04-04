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
import sys
import os
import matplotlib as mpl


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230220',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }


# Алгоритм поиска аномалии по ключевым метрикам (межквартильный размах)
def check_anomaly(df, metric):
    if metric == 'users_action':
        a=4
        n=5
    elif metric == 'views':
        a=3.8
        n=5
    elif metric == 'likes':
        a=3.8
        n=5
    elif metric == 'CTR':
        a=6.5
        n=3
    elif metric == 'users_mess':
        a=4
        n=4
    elif metric == 'messages':
        a=3.8
        n=5
    
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df

# система алертов
def run_alerts(chat=None):
    chat_id= chat or 90870146
    bot = telegram.Bot(token='6251444710:AAH1dTGphjwO0HAHkfgNspTCC562bNKhb8g')
    
    q = """SELECT * FROM
        (SELECT toStartOfFifteenMinutes(time) as part_time,
                toDate(time) as date,
                formatDateTime(part_time, '%R') as hm,
                uniq(user_id) as users_action,
                countIf(user_id, action='view') as views,
                countIf(user_id, action='like') as likes,
                round(countIf(user_id, action = 'like') / countIf(user_id, action = 'view'), 2) as CTR
        FROM simulator_20230220.feed_actions 
        WHERE time >= today() - 1 and time < toStartOfFifteenMinutes(now())
        GROUP BY part_time, date, hm
        ORDER BY part_time) t
        JOIN 
            (SELECT toStartOfFifteenMinutes(time) as part_time,
                    toDate(time) as date,
                    formatDateTime(part_time, '%R') as hm,
                    uniq(user_id) as users_mess,
                    count (user_id) as messages
            FROM simulator_20230220.message_actions 
            WHERE time >= today() - 1 and time < toStartOfFifteenMinutes(now())
            GROUP BY part_time, date, hm
            ORDER BY part_time) t1
            using (part_time, date, hm)
    """

    data = ph.read_clickhouse(query = q, connection=connection)
    
    metrics_list = ['users_action', 'views', 'likes', 'CTR', 'users_mess', 'messages']
    for metric in metrics_list:
        df = data[['part_time', 'date', 'hm', metric]].copy()
        is_alert, df = check_anomaly(df, metric)
        
        if is_alert == 1:
            msg = """Метрика {metric}:\nтекущее значение - {current_val:.2f}\nотклонение от предыдущего значения - {last_val_diff:.2%}\nhttps://superset.lab.karpov.courses/superset/dashboard/3152/""".format(metric=metric, 
                             current_val=df[metric].iloc[-1],
                             last_val_diff=abs(1-(df[metric].iloc[-1]/df[metric].iloc[-2])))

            
            sns.set(rc={'figure.figsize': (16, 10)})
            plt.tight_layout()
            
            ax = sns.lineplot(x=df['part_time'], y=df[metric], label='metric')
            ax = sns.lineplot(x=df['part_time'], y=df['up'], label='up')
            ax = sns.lineplot(x=df['part_time'], y=df['low'], label='low')
            
            for ind, label in enumerate(ax.get_xticklabels()):
                if ind % 2 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
                    
            ax.set(xlabel='time')
            ax.set(ylabel=metric)
            
            ax.set_title(metric)
            ax.set(ylim=(0,None))
            
            plot_object = io.BytesIO() # Файловый объекст
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()
            
            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)



# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'n.tavlintseva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 18),
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'
    

# Создаем сообщение от бота
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_report_alert_TVL():
    
    def load_report():
        run_alerts(chat= -958942131)
    load_report()
    
    
dag_report_alert_TVL=dag_report_alert_TVL()