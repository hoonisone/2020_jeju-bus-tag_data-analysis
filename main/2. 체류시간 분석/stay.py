from multiprocessing import Pool
import multiprocessing
import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm
from matplotlib import pyplot as plt

# 정류장간 이동 데이터 -> 군집 간 이용 데이터 변환
def create_cluster_usage_df(station_usage_df, cluster_station_df):
    usage_selector = ['user_id', 'geton_datetime', 'geton_station_id', 'getoff_datetime', 'getoff_station_id']
    station_usage_df = station_usage_df[usage_selector]

    cluster_selector = cluster_station_df.columns

    geton_columns = {}
    getoff_columns = {}
    for column in cluster_selector:
        geton_columns[column] = "geton_"+column
        getoff_columns[column] = "getoff_"+column
        
    geton_cluster_df = cluster_station_df.rename(columns=geton_columns)
    getoff_cluster_df = cluster_station_df.rename(columns=getoff_columns)
    
    cluster_usage_df = pd.merge(station_usage_df, geton_cluster_df, on=['geton_station_id'], how="left")
    cluster_usage_df = pd.merge(cluster_usage_df, getoff_cluster_df, on=['getoff_station_id'], how="left")
    return cluster_usage_df

def get_tourist_usage_df(cluster_usage_df, user_df):
    tourist_usage_df = pd.merge(cluster_usage_df, user_df, on='user_id')
    tourist_usage_df = tourist_usage_df[tourist_usage_df['tourist'] == True]
    return tourist_usage_df

def get_stay_time_df(usage_df):
    df = usage_df.dropna()
    df = df.sort_values(['user_id', 'geton_datetime'])
    df = df.reset_index().rename(columns = {'index':'drop'})
    df = df.reset_index().rename(columns={'index':'geton_idx'})
    df['getoff_idx'] = df['geton_idx']+1
    getoff_df = df[['user_id', 'getoff_station_id', 'getoff_datetime', 'getoff_idx']]
    geton_df = df[['user_id', 'geton_station_id', 'geton_datetime', 'geton_idx']]
    df = pd.merge(getoff_df, geton_df, left_on = ['user_id', 'getoff_idx'], right_on=['user_id', 'geton_idx'], how='inner')
    return df

def analyze_stay_time_count(stay_time_df):
    stay_time_df['stay_time'] = stay_time_df['geton_datetime'] - stay_time_df['getoff_datetime']
    stay_time_df['stay_time'] = stay_time_df['stay_time'].apply(lambda x : x.seconds//60)
    stay_time = stay_time_df['stay_time'].value_counts()
    stay_time = pd.DataFrame(stay_time).reset_index().rename(columns={'index':'stay_time', 'stay_time':'num'})
    stay_time = stay_time.sort_values(by='stay_time')
    return stay_time

def draw_stay_time_count(x, y):
    plt.rcParams["figure.figsize"] = (15,4)
    plt.plot(x, y)
    plt.title('체류시간 별 이용자 수')
    plt.xlabel("체류 시간")
    plt.ylabel("이용자 수")
    plt.show()