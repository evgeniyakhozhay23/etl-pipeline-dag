from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


def ch_get_df(query='Select 1', host='http://clickhouse.lab.karpov.courses:8123', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result


default_args = {
    'owner': 'e.hozhaj',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.combine(datetime.now()-timedelta(days=1), datetime.min.time()),
}

schedule_interval = '30 16 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def khozhay_dag_etl_first():
    
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
    
    @task()
    def transform_merge_tables(df_feed_users, df_message_users, df_message_receivers):
        df_message = pd.merge(df_message_users, df_message_receivers, left_on='user_id', right_on='receiver_id', how='left')
        df_message = df_message.drop(columns=['receiver_id'])
        df_cube = pd.merge(df_feed_users, df_message, on=['user_id', 'event_date', 'os', 'gender', 'age'], how='left')
        df_cube['messages_received'] = df_cube['messages_received'].fillna(0).astype('int')
        df_cube['messages_sent'] = df_cube['messages_sent'].fillna(0).astype('int')
        df_cube['users_received'] = df_cube['users_received'].fillna(0).astype('int')
        df_cube['users_sent'] = df_cube['users_sent'].fillna(0).astype('int')
        return df_cube
    
    @task()
    def transform_os(df_cube):
        df_cube_os = df_cube[['event_date', 'os', 'user_id', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received',
                              'users_sent']]\
            .groupby(['event_date', 'os'])\
            .sum()\
            .reset_index()
        return df_cube_os
    
    @task()
    def transform_gender(df_cube):
        df_cube_gender = df_cube[['event_date', 'gender', 'user_id', 'views', 'likes', 'messages_received', 'messages_sent',
                              'users_received','users_sent']]\
            .groupby(['event_date', 'gender'])\
            .sum()\
            .reset_index()
        return df_cube_gender
    
    @task()
    def transform_age(df_cube):
        df_cube_age = df_cube[['event_date', 'age', 'user_id', 'views', 'likes', 'messages_received', 'messages_sent',
                               'users_received','users_sent']]\
            .groupby(['event_date', 'age'])\
            .sum()\
            .reset_index()
        return df_cube_age
    @task
    def load(df_cube_os, df_cube_gender, df_cube_age):
        context = get_current_context()
        ds = context['ds']
        print(f'Metrics per os for {ds}')
        print(df_cube_os.to_csv(index=False, sep='\t'))
        print(f'Metrics per gender for {ds}')
        print(df_cube_gender.to_csv(index=False, sep='\t'))
        print(f'Likes per age for {ds}')
        print(df_cube_age.to_csv(index=False, sep='\t'))
        
    df_feed_users = extract_feed_users()
    df_message_users = extract_message_users()
    df_message_receivers = extract_message_receivers()
    df_cube = transform_merge_tables(df_feed_users, df_message_users, df_message_receivers)
    df_cube_os = transform_os(df_cube)
    df_cube_gender = transform_gender(df_cube)
    df_cube_age = transform_age(df_cube)
    load(df_cube_os, df_cube_gender, df_cube_age)

khozhay_dag_etl_first = khozhay_dag_etl_first() 
        
