import datetime
import pandas as pd
from tqdm import tqdm
import lib.work as WORK

# 입력 파일 패쓰 리스트 생성
def make_input_path(start_date, end_date):
    root_path = "data/usage"
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

# 전체 이용 데이터 로드
def load_total_usage_df(input_path_list):
    usage_df = pd.read_csv(input_path_list[0], low_memory=False, encoding = "cp949") #, dtype=dtype)
    for file_path in tqdm(input_path_list[1:]):
        temp_df = pd.read_csv(file_path, low_memory=False, encoding = "cp949") #, dtype=dtype)
        usage_df = pd.concat([usage_df, temp_df], sort=False, ignore_index=True)
        
    usage_df = usage_df[usage_df["geton_station_longitude"].notnull()]
    usage_df = usage_df[usage_df["geton_station_latitude"].notnull()]
    
    # datetime64로 형 변환 # M[base_date] = pd.to_datetime(M[base_date], format='%Y%m%d')
    usage_df['geton_datetime'] = pd.to_datetime(usage_df['geton_datetime'], format='%Y%m%d%H%M%S')
    usage_df['getoff_datetime'] = pd.to_datetime(usage_df['getoff_datetime'], format='%Y%m%d%H%M%S')
    return usage_df

def parallel_load_total_usage_df(input_path_list, core = 8):
    return WORK.parallelize(load_total_usage_df, input_path_list, core)