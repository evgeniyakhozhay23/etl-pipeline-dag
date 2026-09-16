from datetime import datetime, timedelta
import pandahouse as ph
import pandas as pd
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


def ch_get_df(query='Select 1', host='http://clickhouse.lab.karpov.courses:8123', user='student', password='#########'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

connection_test = {'host': 'http://clickhouse.lab.karpov.courses:8123',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'#########' # должен быть пароль
                     }


default_args = {
    'owner': 'e.hozhaj',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.combine(datetime.now()-timedelta(days=1), datetime.min.time()),
}

schedule_interval = '00 18 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def khozhay_dag_etl_pipline_with_table():
    
    @task()
    def extract_feed_users():
        query = """SELECT toDate(time) as event_date,
                        user_id,
                        gender,
                        os,
                        age,
                        sum(action = 'like') as likes,
                        sum(action = 'view') as views
                FROM simulator_20260620.feed_actions 
                WHERE toDate(time) = today() - 1
                GROUP BY toDate(time), user_id, gender, os, age
                format TSVWithNames"""
        df_feed_users = ch_get_df(query=query)
        return df_feed_users
    
    @task()
    def extract_message_users():
        query = """SELECT 
                    toDate(time) as event_date,
                    user_id,
                    gender,
                    os,
                    age,
                    count (user_id) as messages_sent,
                    count (DISTINCT receiver_id) as users_sent
                FROM simulator_20260620.message_actions 
                WHERE toDate(time) = today() - 1
                GROUP BY toDate(time), user_id, gender, os, age
                format TSVWithNames"""
        df_message_users = ch_get_df(query=query)
        return df_message_users
    
    @task()
    def extract_message_receivers():
        query =  """SELECT receiver_id,
                    count (*) as messages_received,
                    count (DISTINCT user_id) as users_received
                FROM simulator_20260620.message_actions 
                WHERE toDate(time) = today() - 1
                GROUP BY receiver_id
                format TSVWithNames"""
        df_message_receivers = ch_get_df(query=query)
        return df_message_receivers
    
    # Объединение информации из трех таблиц
    @task()
    def transform_merge_tables(df_feed_users, df_message_users, df_message_receivers):
        df_message = pd.merge(df_message_users, df_message_receivers, left_on='user_id', right_on='receiver_id', how='left')
        df_message = df_message.drop(columns=['receiver_id'])
        df_cube = pd.merge(df_feed_users, df_message, on=['user_id', 'event_date', 'os', 'gender', 'age'], how='left')
        df_cube['messages_received'] = df_cube['messages_received'].fillna(0).astype('int')
        df_cube['messages_sent'] = df_cube['messages_sent'].fillna(0).astype('int')
        df_cube['users_received'] = df_cube['users_received'].fillna(0).astype('int')
        df_cube['users_sent'] = df_cube['users_sent'].fillna(0).astype('int')
        df_cube['views'] = df_cube['views'].fillna(0).astype('int')
        df_cube['likes'] = df_cube['likes'].fillna(0).astype('int')
        return df_cube
    
    
    @task()
    def transform_os(df_cube):
        df_cube_os = df_cube[['event_date', 'os', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received',
                              'users_sent']]\
            .groupby(['event_date', 'os'])\
            .sum()\
            .reset_index()
        df_cube_os = df_cube_os.rename(columns = {'os': "dimension_value"})
        df_cube_os['dimension'] = 'os'
        return df_cube_os
    
    @task()
    def transform_gender(df_cube):
        df_cube_gender = df_cube[['event_date', 'gender', 'views', 'likes', 'messages_received', 'messages_sent',
                              'users_received','users_sent']]\
            .groupby(['event_date', 'gender'])\
            .sum()\
            .reset_index()
        df_cube_gender = df_cube_gender.rename(columns = {'gender': "dimension_value"})
        df_cube_gender['dimension'] = 'gender'
        return df_cube_gender
    
    @task()
    def transform_age(df_cube):
        df_cube_age = df_cube[['event_date', 'age', 'views', 'likes', 'messages_received', 'messages_sent',
                               'users_received','users_sent']]\
            .groupby(['event_date', 'age'])\
            .sum()\
            .reset_index()
        df_cube_age = df_cube_age.rename(columns = {'age': "dimension_value"})
        df_cube_age['dimension'] = 'age'
        return df_cube_age
    
    @task
    def tranform_final_table(df_cube_os, df_cube_gender, df_cube_age):
        context = get_current_context()
        ds = context['ds']
        final_table = pd.concat([df_cube_os, df_cube_gender, df_cube_age], ignore_index=True)
        final_table['event_date'] = pd.to_datetime(final_table['event_date']).dt.date
        return final_table[['event_date', 'dimension', 'dimension_value', 'views', 'likes', 
                         'messages_received','messages_sent','users_received','users_sent']]
    
    @task
    def create_table():
        query_test = '''CREATE TABLE IF NOT EXISTS test.ehozhay_lesson_7
                    (event_date Date,
                    dimension String,
                    dimension_value String,
                    views UInt64,
                    likes UInt64,
                    messages_received UInt64,
                    messages_sent UInt64,
                    users_received UInt64,
                    users_sent UInt64)
                    ENGINE = MergeTree()
                    ORDER BY (event_date, dimension, dimension_value)'''
        ph.execute(query_test, connection=connection_test)
    
    @task
    def load_to_clickhouse(final_table):
        ph.to_clickhouse(df=final_table, table="ehozhay_lesson_7", 
                    index=False, connection=connection_test)
    
    df_feed_users = extract_feed_users()
    df_message_users = extract_message_users()
    df_message_receivers = extract_message_receivers()
    df_cube = transform_merge_tables(df_feed_users, df_message_users, df_message_receivers)
    df_cube_os = transform_os(df_cube)
    df_cube_gender = transform_gender(df_cube)
    df_cube_age = transform_age(df_cube)
    final_table = tranform_final_table(df_cube_os, df_cube_gender, df_cube_age)
    new_table = create_table()
    new_table >> load_to_clickhouse(final_table)

khozhay_dag_etl_pipline_with_table = khozhay_dag_etl_pipline_with_table()
        
