import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
import datetime 
import mysql.connector

def import_data(room,date1,date2,fea="temperature"):
    # import the data
    cnxn = mysql.connector.connect(user='instahub', password='CtEJDpgL7nfPrKcFjSHP',
                                host='client-data-fetch.cxsmphqeilrp.us-east-1.rds.amazonaws.com',database='master')

    #Creating a cursor object using the cursor() method
    data = pd.read_sql_query('SELECT * from keyvalueTB',cnxn)
    df = data_process(data)
    return data_backend(df,room,date1, date2, fea)


def data_backend(df,room,date1, date2, fea="temperature"):
    date1 = datetime.datetime.strptime(date1,'%Y-%m-%d')
    date2 = datetime.datetime.strptime(date2,'%Y-%m-%d')
    if(date1 > date2 ):
        print("Start date can't be later than end date")
        return
    
    if (date2 - date1).days <= 7:
        df_sub = df[(df.date<=date2.date()) & (df.date>=date1.date()) & (df.Room_name == room)][["date","hour",fea]]
        df_agg = df_sub.groupby(['date','hour'])[fea].mean().reset_index()
        df_agg.date = df_agg.date.apply(lambda x: x.strftime('%Y-%m-%d'))
        return df_agg.values.tolist()
    elif (date2 - date1).days <= 30:
        df_sub = df[(df.date<=date2.date()) & (df.date>=date1.date()) & (df.Room_name == room)][["weekday","hour",fea]]
        dayOfWeek = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday','Saturday', 'Sunday']
        df_sub.weekday = df_sub.weekday.astype("category")
        df_sub.weekday.cat.categories = dayOfWeek
        df_agg = df_sub.groupby(['weekday','hour'])[fea].mean().reset_index()
        return df_agg.values.tolist()
    else:
        df_sub = df[(df.date<=date2.date()) & (df.date>=date1.date()) & (df.Room_name == room)][["month","hour",fea]]
        df_agg = df_sub.groupby(['month','hour'])[fea].mean().reset_index()
        return df_agg.values.tolist()


def data_process(df):
    # remove invalid data
    i = df[(df.illumW==0.0)&(df.illumB==0.0)&(df.humidity==0.0)&(df.temperature==0.0)].index
    df = df.drop(i).reset_index()
    df.drop(['ClientID', 'illumR', 'illumG', 'illumB','index'],
       axis = 1, inplace = True)
    df['Timestmp'] = pd.to_datetime(df['Timestmp'])
    df['date']=df['Timestmp'].dt.date
    df['date_hour']=df['Timestmp'].apply(lambda x: str(x.date())+'-'+str(x.hour))
    df['month'] = df['Timestmp'].dt.month
    df['weekday'] = df['Timestmp'].dt.weekday
    df['weekday_hour'] = df['Timestmp'].apply(lambda x: str(x.day_name())+'-'+str(x.hour))
    df['hour'] = df['Timestmp'].dt.hour
    
    # For weekly plot
    df['year_week'] = df['Timestmp'].apply(lambda x: str(x.year)+'-'+str(x.isocalendar()[1]))
    df['year_week_hour'] = df['Timestmp'].apply(lambda x: str(x.year)+'-'+str(x.isocalendar()[1])+'-'+str(x.hour))
    room_id = pd.DataFrame(data={'DevEUI': ['2B6F28FFFE00000A', '2B6F28FFFE00000B','2B6F28FFFE00000C',
                         '2B6F28FFFE00000D','2B6F28FFFE00000E','2B6F28FFFE00000F',
                         '2B6F28FFFE000002', '2B6F28FFFE000004', '2B6F28FFFE000006',
                         '2B6F28FFFE000008', '2B6F28FFFE000009','2B6F28FFFE000010',
                          '2B6F28FFFE000011', '2B6F28FFFE000012'], 
               'Room_name': ['133','119','HB','238','Lab','104Tern','2','3','6','1','Unknown','118Toz','224','116']})
    df = df.merge(room_id, left_on='DevEUI',right_on = 'DevEUI', how='left')
    df = df[df.Room_name.isin(['133','119','HB','238','Lab','104Tern','118Toz','224','116'])]
    df = df.drop(columns=['DevEUI', 'motion_rollvar']).reset_index(drop=True)
    return df
    