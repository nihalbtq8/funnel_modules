import pandas as pd
from pandas.io.sql import SQLTable
from datetime import date, timedelta

''' This function modifies pandas builtin to_sql.__execute_insert\
 method for one insert statement for whole dataset instead of one\
 for each row. Change this for big datasets for exponential time \
 decrease in most cases. Put this right after pandas import.
'''
def _execute_insert(self, conn, keys, data_iter):
    print("Using monkey-patched _execute_insert")
    data = [dict((k, v) for k, v in zip(keys, row)) for row in data_iter]
    conn.execute(self.insert_statement().values(data))

SQLTable._execute_insert = _execute_insert

from sqlalchemy import create_engine

private_key = '/home/gpadmin/bigquery_dump/boutiqaat_GA.json'


query_last_day = """select DATE(TIMESTAMP_MICROS(event_timestamp)) event_date,extract(hour from TIMESTAMP_MICROS(event_timestamp)) event_hour, extract(minute from TIMESTAMP_MICROS(event_timestamp)) event_minute, device.category as platform, device.operating_system as os, count(distinct user_pseudo_id) as users, sum(case when event_name = 'add_to_cart' then 1 end) added_to_cart, sum(case when event_name ='checkout_progress' then 1 end) checkout_progress, count(case when event_name='ecommerce_purchase' then 1 end ) transactions from `boutiqaat-online-shopping.analytics_151427213.events_intraday_20181205` where DATE(TIMESTAMP_MICROS(event_timestamp)) = '""" + str(date.today()) + """' group by 1,2,3,4,5;"""



###read_gbq method can be accessed only if pandas-gbq package is installed.
df_ = pd.read_gbq(query_last_day, private_key=private_key, dialect='standard')


##cleaning
df_ = df_.sort_values(by=['event_date', 'event_hour', 'event_minute'])
#df_ = df_.reset_index(drop=True).rename_axis("id").reset_index()
#df_['event_date'] = df_['event_date'].apply(lambda x: x[0:4]+'-'+x[4:6]+'-'+x[6:])
df_.event_date=df_.event_date.astype(str) 

##insertion. 
sql_uri = 'postgresql+psycopg2://gpadmin:btq6@localhost:6432/bq'
engine = create_engine(sql_uri)
conn = engine.connect()

table_name = 'funnel_minute_intraday'

df_.to_sql(table_name, conn, chunksize=20000, index = False, if_exists='append')

