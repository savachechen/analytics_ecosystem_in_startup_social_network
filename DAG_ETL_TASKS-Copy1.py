from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#параметры соединения со схемой test
connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
    'database':'test',
    'user':'student-rw',
    'password':'656e2b0c9c'
    }
    
#запрос на создание таблицы в схеме test
create_table_query = """
CREATE TABLE IF NOT EXISTS test.s_savchits_ETL (
    event_date Date,
    dimension String,
    dimension_value String,
    views Int64,
    likes Int64,
    users_received Int64,
    messages_received Int64,
    users_sent Int64,
    messages_sent Int64
) ENGINE = MergeTree()
    ORDER BY (event_date, dimension, dimension_value);
"""
        
ph.execute(create_table_query, connection=connection_test)    

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 's.savchits',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 17),
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_sim_etl_s_savchits_upd_v3():

    @task()
    def extract_likes_and_views():
        #параметры соединения - нужны, чтобы подключиться к нужной схеме данных
        connection = {'host': 'https://clickhouse.lab.karpov.courses',
        'database':'simulator_20250220',
        'user':'student',
        'password':'dpo_python_2020'
        }
        
        #запрос на выгрузку количества лайков и просмотров на юзера
        query_1 = """SELECT toDate(time) AS event_date,
                    user_id,
                    age,
                    if(gender = 1, 'female', 'male') AS gender,
                    os,
                    countIf(action = 'view') AS views,
                    countIf(action = 'like') AS likes
               FROM {db}.feed_actions
              WHERE event_date = today() - 1
              GROUP BY event_date, user_id, age, gender, os"""
        df_likes_and_views = ph.read_clickhouse(query = query_1, connection=connection)
        return df_likes_and_views
    
    @task()
    def extract_received_and_sented():
        #параметры соединения - нужны, чтобы подключиться к нужной схеме данных
        connection = {'host': 'https://clickhouse.lab.karpov.courses',
        'database':'simulator_20250220',
        'user':'student',
        'password':'dpo_python_2020'
        }
        
        #запрос на выгрузку количества получателей, отправителей и сообщений на юзера
        query_2 = """SELECT ma_1.event_date,
                    ma_1.user_id,
                    ma_1.messages_received,
                    ma_2.messages_sent,
                    ma_1.users_received,
                    ma_2.users_sent
               FROM       
            (SELECT toDate(time) AS event_date,
                    user_id,
                    count(DISTINCT receiver_id) AS users_received,
                    count(receiver_id) AS messages_received
               FROM {db}.message_actions
              WHERE event_date = today() - 1
              GROUP BY event_date, user_id) ma_1
               LEFT JOIN
            (SELECT toDate(time) AS event_date,
                    receiver_id,
                    count(DISTINCT user_id) AS users_sent,
                    count(user_id) AS messages_sent
               FROM {db}.message_actions
              WHERE event_date = today() - 1
              GROUP BY event_date, receiver_id) ma_2
                 ON ma_1.user_id = ma_2.receiver_id AND ma_1.event_date = ma_2.event_date"""
        
        df_received_and_sented = ph.read_clickhouse(query = query_2, connection=connection)
        return df_received_and_sented

    @task
    def transfrom_merged_df(df_likes_and_views, df_received_and_sented):
        # Изменение типа данных столбцов на int16
        df_likes_and_views['likes'] = df_likes_and_views['likes'].astype('int64')
        df_likes_and_views['views'] = df_likes_and_views['views'].astype('int64')
        df_merged = df_likes_and_views.merge(df_received_and_sented, how = 'left')
        df_merged['users_received'] = df_merged['users_received'].fillna(0)
        df_merged['messages_received'] = df_merged['messages_received'].fillna(0)
        df_merged['users_sent'] = df_merged['users_sent'].fillna(0)
        df_merged['messages_sent'] = df_merged['messages_sent'].fillna(0)
        df_merged['users_received'] = df_merged['users_received'].astype('int64')
        df_merged['messages_received'] = df_merged['messages_received'].astype('int64')
        df_merged['users_sent'] = df_merged['users_sent'].astype('int64')
        df_merged['messages_sent'] = df_merged['messages_sent'].astype('int64')
        return df_merged

    @task
    def transfrom_age(df_merged):
        df_age = df_merged.groupby('age', as_index=False) \
                   .agg({
                       'views': 'sum',
                       'likes': 'sum',
                       'users_received': 'sum',
                       'messages_received': 'sum',
                       'users_sent': 'sum',
                       'messages_sent': 'sum'}) \
                   .rename(columns={'age': 'dimension_value'})
        df_age.insert(0, 'dimension', 'age')
        df_age.insert(0, 'event_date', df_merged.iloc[0, 0])
        return df_age
    
    @task
    def transfrom_gender(df_merged):
        df_gender = df_merged.groupby('gender', as_index=False) \
                      .agg({
                          'views': 'sum',
                          'likes': 'sum',
                          'users_received': 'sum',
                          'messages_received': 'sum',
                          'users_sent': 'sum',
                          'messages_sent': 'sum'}) \
                      .rename(columns={'gender': 'dimension_value'})
        df_gender.insert(0, 'dimension', 'gender')
        df_gender.insert(0, 'event_date', df_merged.iloc[0, 0])
        return df_gender
    
    
    @task
    def transfrom_os(df_merged):
        df_os = df_merged.groupby('os', as_index=False) \
                  .agg({
                      'views': 'sum',
                      'likes': 'sum',
                      'users_received': 'sum',
                      'messages_received': 'sum',
                      'users_sent': 'sum',
                      'messages_sent': 'sum'}) \
                  .rename(columns={'os': 'dimension_value'})
        df_os.insert(0, 'dimension', 'os')
        df_os.insert(0, 'event_date', df_merged.iloc[0, 0])
        return df_os
    
    @task
    def transfrom_concat_df(df_age, df_gender, df_os):
        df_combined = pd.concat([df_gender, df_os, df_age], axis=0)
        return df_combined
    
    @task
    def load(df_combined):
        ph.to_clickhouse(df=df_combined, table='s_savchits_ETL', index=False, connection=connection_test)

    df_likes_and_views = extract_likes_and_views()
    df_received_and_sented = extract_received_and_sented()
    df_merged = transfrom_merged_df(df_likes_and_views, df_received_and_sented)
    df_age = transfrom_age(df_merged)
    df_gender = transfrom_gender(df_merged)
    df_os = transfrom_os(df_merged)
    df_combined = transfrom_concat_df(df_age, df_gender, df_os)
    load(df_combined)

dag_sim_etl_s_savchits_upd_v3 = dag_sim_etl_s_savchits_upd_v3()