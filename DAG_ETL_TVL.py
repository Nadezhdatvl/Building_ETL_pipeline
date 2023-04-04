import pandahouse as ph
import numpy as np
from datetime import datetime, timedelta
import pandas as pd
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

# Подключение к данным ТЕСТ
connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'656e2b0c9c'
                     }

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'n.tavlintseva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 18),
}


# Интервал запуска DAG
schedule_interval = '* 11 * * *'
# timedelta(minutes=10)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_etl_TVL():
    
    # Достаем данные из таблицы Мессенжера
    @task()
    def extract_mess():
        query_m = """ WITH receiv as  
                     (SELECT toDate(time) as event_date, user_id,
                            COUNT(reciever_id) OVER (PARTITION BY user_id ) as messages_received,
                            COUNT(distinct reciever_id) OVER (PARTITION BY user_id ) as users_received,
                            os, gender, age
                        FROM simulator_20230220.message_actions
                        WHERE toDate(time)=yesterday()
                        GROUP BY event_date, user_id, reciever_id, os, gender, age ),
                      sent as
                     (SELECT toDate(time) as event_date, reciever_id,
                            COUNT(user_id) OVER (PARTITION BY reciever_id ) as messages_sent,
                            COUNT(distinct user_id) OVER (PARTITION BY reciever_id ) as users_sent,
                            os, gender, age
                        FROM simulator_20230220.message_actions
                        WHERE toDate(time)=yesterday()
                        GROUP BY event_date, reciever_id, user_id, os, gender, age ) 

                    SELECT event_date, user_id, messages_received, messages_sent, users_received, users_sent,
                            os, gender, age
                    FROM(
                    SELECT event_date, receiv.user_id, messages_received, messages_sent, users_received,   users_sent,os, gender, age
                    FROM receiv
                    FULL JOIN sent on receiv.user_id=sent.reciever_id and receiv.event_date=sent.event_date
                    GROUP BY event_date, user_id, messages_received, messages_sent, users_received, users_sent, os, gender, age
                    ORDER BY event_date )
                    WHERE user_id!=0
                    GROUP BY event_date, user_id, messages_received, messages_sent, users_received, users_sent, os, gender, age
            """
           
        df_mess = ph.read_clickhouse(query = query_m, connection=connection)
        return df_mess
    
    # Достаем данные о лайках и просмотрах
    @task()
    def extract_action():
        query_a= """
                  SELECT toDate(time) as event_date,
                          user_id,
                          countIf(user_id, action = 'view') as views,
                          countIf(user_id, action = 'like') as likes,
                          os, gender, age
                  FROM simulator_20230220.feed_actions
                  WHERE toDate(time)=yesterday()
                  GROUP BY toDate(time), user_id, os, gender, age """
        

        df_action = ph.read_clickhouse(query = query_a, connection=connection)
        return df_action
    
    # Объединение таблиц
    @task()
    def transfrom_merge(df_action, df_mess):
        df_new = df_action.merge(df_mess, on=['user_id','event_date','gender','os','age'], how='outer')
        df_new.fillna(0, inplace=True)
        df_new['age_group'] = pd.cut(df_new['age'], bins = [0 ,18 , 25, 35, 45, 55, 100],
                                            labels =['До 18', '18-25', '25-35', '35-45', '45-55', '55+'])
        df_new.drop(columns = ['age'],inplace=True)
        return df_new
    # Срез пола
    @task
    def transfrom_gender(df_new):
        df_gender = df_new[['event_date', 'gender', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
        .groupby(['event_date', 'gender'])\
        .sum()\
        .reset_index()
        df_gender=df_gender.replace({'gender': {1:'male',0:'female'}})
        df_gender['dimension'] = 'gender'
        df_gender.rename(columns = {'gender':'dimension_value'},inplace = True)
        df_gender = df_gender[['event_date',
                  'dimension',
                  'dimension_value',
                  'views',
                  'likes',
                  'messages_received',
                  'messages_sent',
                  'users_received',
                  'users_sent']]
        return df_gender
    # Срез возраста
    @task
    def transfrom_age(df_new):
        df_age = df_new[['event_date', 'age_group', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
        .groupby(['event_date', 'age_group'])\
        .sum()\
        .reset_index()
        df_age['dimension'] = 'age'
        df_age.rename(columns = {'age_group':'dimension_value'},inplace = True)
        df_age = df_age[['event_date',
                  'dimension',
                  'dimension_value',
                  'views',
                  'likes',
                  'messages_received',
                  'messages_sent',
                  'users_received',
                  'users_sent']]
        return df_age
    # Срез ос
    @task
    def transfrom_os(df_new):
        df_os = df_new[['event_date', 'os', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
        .groupby(['event_date', 'os']).sum().reset_index()
        df_os['dimension'] = 'os'
        df_os.rename(columns = {'os':'dimension_value'},inplace = True)
        df_os = df_os[['event_date',
                  'dimension',
                  'dimension_value',
                  'views',
                  'likes',
                  'messages_received',
                  'messages_sent',
                  'users_received',
                  'users_sent']]
        return df_os
    # Объединение для финальной таблицы
    @task
    def transfrom_concat(df_os,df_gender,df_age):
        df_all=pd.concat([df_os,df_gender,df_age],sort=False, axis=0)
        return df_all
    # Загрузка в финальную таблицу
    @task
    def load(df_all):
        create_query = '''
            CREATE TABLE IF NOT EXISTS test.n_tavlintseva_test (
                  event_date Date,
                  dimension String,
                  dimension_value String,
                  views Float64,
                  likes Float64,
                  messages_received Float64,
                  messages_sent Float64,
                  users_received Float64,
                  users_sent Float64
                ) ENGINE = MergeTree()
                ORDER BY
                  event_date
            '''
        ph.execute(query = create_query, connection = connection_test)
        ph.to_clickhouse(df_all, table='n_tavlintseva_test', index=False, connection=connection_test)
        context = get_current_context()
        ds = context['ds']
        print(f'Updated for {ds}')
        
        
    
    df_mess = extract_mess()
    df_action = extract_action()
    df_new = transfrom_merge(df_action, df_mess)
    df_gender = transfrom_gender(df_new)
    df_age = transfrom_age(df_new)
    df_os = transfrom_os(df_new)
    df_all = transfrom_concat(df_os,df_gender,df_age)
    load(df_all)

dag_etl_TVL = dag_etl_TVL()
