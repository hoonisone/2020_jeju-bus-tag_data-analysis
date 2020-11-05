import datetime
import pandas as pd
from tqdm import tqdm
from pyarrow import csv

# 입력 파일 패쓰 리스트 생성
def make_input_path(start_date, end_date):
    root_path = "C:/tb_bus_user_usage"
    base_name = "tb_bus_user_usage_"
    extender = ".csv"
    path_list = []
    
    for day in range((end_date-start_date).days+1):
        date = start_date + datetime.timedelta(days = day)
        date = str(date)
        y = date[2:4]
        m = date[5:7]
        d = date[8:10]
        file_path = root_path+"/"+base_name+y+m+d+extender
        path_list.append(file_path)
        
    return path_list

# 컬럼 정리
user_id = 'user_id'
base_date = 'base_date'
geton_datetime = 'geton_datetime'
geton_station_name = 'geton_stataion_name'
geton_station_longitude = 'geton_station_longitude'
geton_station_latitude = 'geton_station_latitude'
getoff_datetime = 'getoff_datetime'
getoff_station_name = 'getoff_station_name'
getoff_station_longitude = 'getoff_station_longitude'
getoff_station_latitude = 'getoff_station_latitude'
user_count = 'user_count' 

# 전체 이용 데이터 로드
def load_total_usage_data(input_path_list):
    usage_df = pd.read_csv(input_path_list[0], low_memory=False, encoding = "cp949") #, dtype=dtype)
    for file_path in tqdm(input_path_list[1:]):
        temp_df = pd.read_csv(file_path, low_memory=False, encoding = "cp949") #, dtype=dtype)
        usage_df = pd.concat([usage_df, temp_df], sort=False, ignore_index=True)
        
    usage_df = usage_df[usage_df["geton_station_longitude"].notnull()]
    usage_df = usage_df[usage_df["geton_station_latitude"].notnull()]
    
    # datetime64로 형 변환 # M[base_date] = pd.to_datetime(M[base_date], format='%Y%m%d')
    usage_df[geton_datetime] = pd.to_datetime(usage_df[geton_datetime], format='%Y%m%d%H%M%S')
    usage_df[getoff_datetime] = pd.to_datetime(usage_df[getoff_datetime], format='%Y%m%d%H%M%S')
    
    return usage_df

def insert_tourist_column(usage_df, user_df):
    return pd.merge(usage_df, user_df, on = "user_id")

# def preprocessing_missing_data_from_usage_df(usage_df):
#     usage_df = usage_df[usage_df["geton_station_longitude"].notnull()]
#     usage_df = usage_df[usage_df["geton_station_latitude"].notnull()]
#     return usage_df

   
# load data
def create_station_df():
    station_df = pd.read_csv("station_list.csv", encoding = "cp949")
    return station_df

def create_cluster_df():
    cluster_df = pd.read_csv("cluster_list.csv", encoding = "cp949")    
    return cluster_df


def create_user_df():
    user_df = pd.read_csv("user_list.csv", encoding = "cp949")
    return user_df
                          
# create df joining df
def create_clustered_usage_df(usage_df, cluster_df):
    # extract necessary columns
    usage_df_columns = ["user_id", "geton_station_id", "getoff_station_id"]
    usage_df = usage_df[usage_df_columns]
    
    # extract necessary columns
    cluster_columns = ["station_id", "cluster_group", "cluster_target", "cluster_x", "cluster_y"]
    cluster_df = cluster_df[cluster_columns]
    
    # create new column names
    cluster_columns.remove("station_id")
    new_geton_columns = {}
    new_getoff_columns = {}
    for column in cluster_columns:
        new_geton_columns[column] = "geton_"+column
        new_getoff_columns[column] = "getoff_"+column
        
    # merge geton station with cluster    
    geton_cluster_df = cluster_df.rename(columns = {"station_id":"geton_station_id"})
    clustered_usage_df1 = pd.merge(usage_df, geton_cluster_df, how="left", on = "geton_station_id")
    clustered_usage_df1 = clustered_usage_df1.rename(columns = new_geton_columns)
    
    # merge getoff station with cluster
    getoff_cluster_df = cluster_df.rename(columns = {"station_id":"getoff_station_id"})
    clustered_usage_df2 = pd.merge(clustered_usage_df1, getoff_cluster_df, how="left", on = "getoff_station_id")
    clustered_usage_df2 = clustered_usage_df2.rename(columns = new_getoff_columns)
    
    result = clustered_usage_df2.reindex(columns=["user_id", 
                                                  "geton_station_id", "geton_cluster_group", "geton_cluster_target",
                                                  "geton_cluster_x", "geton_cluster_y",
                                                  "getoff_station_id", "getoff_cluster_group", "getoff_cluster_target",
                                                  "getoff_cluster_x", "getoff_cluster_y"])
    return result

def create_clustered_station_df(station_df, cluster_df):
    # extract necessary columns in station_df
    station_df_columns = ["station_id",
                          "citizen_user_count", "tourist_user_count", "total_user_count",
                          "citizen_tag_count", "tourist_tag_count", "total_tag_count"]    
    station_df = station_df[station_df_columns]

    
    # extract necessary columns in cluster_df
    cluster_df_columns = ["cluster_group", "cluster_target", "station_id", "cluster_x", "cluster_y"]
    cluster_df = cluster_df[cluster_df_columns]

    
    # merging station and cluster
    merged_df = pd.merge(station_df, cluster_df, on="station_id")   
    
    
    # create clustered_location_df from merged_df
    clustered_location_df_columns = ["cluster_group", "cluster_target", "cluster_x", "cluster_y"]
    clustered_location_df = merged_df[clustered_location_df_columns].drop_duplicates(clustered_location_df_columns)
    
    
    # create clustered_usage_df
    clustered_usage_df = merged_df.drop(["station_id", "cluster_x", "cluster_y"], axis = 1)
    clustered_usage_df = clustered_usage_df.groupby(["cluster_group", "cluster_target"]).sum()
    
    
    # create clustered_station_df merging usage and location df
    clustered_station_df = pd.merge(clustered_location_df, clustered_usage_df, on=["cluster_group", "cluster_target"])
    return clustered_station_df

