import pandas as pd
from datetime import date, timedelta, datetime
from sqlalchemy import create_engine

private_key = '/home/gpadmin/bigquery_dump/boutiqaat_GA.json'

query = """select DATE(TIMESTAMP_MICROS(event_timestamp)) event_date,extract(hour from TIMESTAMP_MICROS(event_timestamp)) event_hour, extract(minute from TIMESTAMP_MICROS(event_timestamp)) event_minute, device.category as platform, \
device.operating_system as os, count(distinct user_pseudo_id) as users, sum(case when event_name = 'add_to_cart' then 1 end) \
added_to_cart, sum(case when event_name ='checkout_progress' then 1 end) checkout_progress, count(case when event_name = \
'ecommerce_purchase' then 1 end ) transactions from `boutiqaat-online-shopping.analytics_151427213.events_""" + ''.join(str((datetime.utcnow()-timedelta(days=1)).date()).split('-')) +  """` group by 1,2,3,4,5 """


###read_gbq method can be accessed only if pandas-gbq package is installed.
df_ = pd.read_gbq(query, private_key=private_key, dialect='standard')


##cleaning
df_ = df_.sort_values(by=['event_date', 'event_hour', 'event_minute'])
#df_ = df_.reset_index(drop=True).rename_axis("id").reset_index()
#df_['event_date'] = df_['event_date'].apply(lambda x: x[0:4]+'-'+x[4:6]+'-'+x[6:])


##insertion. 
sql_uri = 'postgresql+psycopg2://user:password@localhost:6432/dwh'
engine = create_engine(sql_uri)
conn = engine.connect()

#To change schema
#conn.execute('set search_path to data_lake;')


table_name = 'funnel_minute'


# insertion part . Please create the table first / change it after first insertion. Datatypes default to text when it cannot be infered.
df_.to_sql(table_name, conn, chunksize=20000, index = False, if_exists='append')




#bigquery time ---- utc
#greenplum time    -----  utc
#but new dataset in bigquery created  ------   kuwait time