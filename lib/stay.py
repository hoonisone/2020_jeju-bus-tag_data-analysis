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
    return cluster_usage_df[['user_id', 'geton_datetime', 'geton_cluster_id', 'getoff_datetime', 'getoff_cluster_id']]

def fillter_usage_df(usage_df, user_df, tourist=True):
    seletor = usage_df.columns
    usage_df = pd.merge(usage_df, user_df, on='user_id')
    usage_df = usage_df[usage_df['tourist'] == tourist]
    usage_df = usage_df[seletor]
    return usage_df

def get_walk_df(df):
    df = df.sort_values(['user_id', 'geton_datetime'])
    df = df.reset_index().rename(columns = {'index':'drop'})
    df = df.reset_index().rename(columns={'index':'geton_idx'})
    df['getoff_idx'] = df['geton_idx']+1
    getoff_df = df[['user_id', 'getoff_cluster_id', 'getoff_datetime', 'getoff_idx']]
    geton_df = df[['user_id', 'geton_cluster_id', 'geton_datetime', 'geton_idx']]
    df = pd.merge(getoff_df, geton_df, left_on = ['user_id', 'getoff_idx'], right_on=['user_id', 'geton_idx'], how='inner')
    seletor = list(df.columns)
    seletor.remove("geton_idx")
    seletor.remove("getoff_idx")
    return df[seletor]

def analyze_walk_time_count(df):
    df['stay_time'] = df['geton_datetime'] - df['getoff_datetime']
    df['stay_time'] = df['stay_time'].apply(lambda x : x.seconds//60)
    stay_time = df['stay_time'].value_counts()
    stay_time = pd.DataFrame(stay_time).reset_index().rename(columns={'index':'stay_time', 'stay_time':'num'})
    stay_time = stay_time.sort_values(by='stay_time')
    return stay_time

def draw_walk_time_count(x, y):
    plt.rcParams["figure.figsize"] = (15,4)
    plt.plot(x, y)
    plt.title('체류시간 별 이용자 수')
    plt.xlabel("체류 시간")
    plt.ylabel("이용자 수")
    plt.show()
    
def set_position_columns(usage_df, cluster_df):
    selector = ['cluster_id', 'cluster_longitude', 'cluster_latitude']
    cluster_df = cluster_df[selector]
    geton_cluster_renamer = {}
    getoff_cluster_renamer = {}
    for column in selector:
        geton_cluster_renamer[column] = "geton_"+column
        getoff_cluster_renamer[column] = "getoff_"+column
    
    geton_cluster_df = cluster_df.rename(columns = geton_cluster_renamer)
    getoff_cluster_df = cluster_df.rename(columns = getoff_cluster_renamer)
    
    usage_df = pd.merge(usage_df, geton_cluster_df, on="geton_cluster_id")
    usage_df = pd.merge(usage_df, getoff_cluster_df, on="getoff_cluster_id")
    return usage_df

def set_dist(cluster_df, x1, y1, x2, y2, longitude = "cluster_longitude", latitude = "cluster_latitude"):
    selector = list(cluster_df.columns)
    cluster_df['dist1_x'] = (cluster_df[longitude] - x1)**2
    cluster_df['dist1_y'] = (cluster_df[latitude] - y1)**2
    cluster_df['dist2_x'] = (cluster_df[longitude] - x2)**2
    cluster_df['dist2_y'] = (cluster_df[latitude] - y2)**2

    cluster_df['dist1'] = (cluster_df['dist1_x'] + cluster_df['dist1_y'])**(1/2)
    cluster_df['dist2'] = (cluster_df['dist2_x'] + cluster_df['dist2_y'])**(1/2)
    cluster_df['dist'] = cluster_df['dist1'] + cluster_df['dist2']
    cluster_df['dist'] = cluster_df['dist']*6500000/360
    cluster_df['dist'] = cluster_df['dist'].apply(lambda x : int(x))
    selector.append("dist")
    cluster_df = cluster_df[selector]
    cluster_df = cluster_df.sort_values(by="dist")
    return cluster_df