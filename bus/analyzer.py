import os
import datetime
import requests
import numpy as np
import pandas as pd
import multiprocessing
from tqdm import tqdm
from pyarrow import csv
from matplotlib import pyplot as plt
from sklearn.cluster import DBSCAN
import xml.etree.ElementTree as elemTree

# 입력 파일 패쓰 리스트 생성
def make_input_path(start_date, end_date):
    root_path = "D:/workspace/Bus Project/data/usage"
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

# 정류장 추출
def analyze_usage(usage_df):
    usage_df = usage_df[["geton_station_id", "user_count"]].groupby("geton_station_id").sum()
    usage_df.index.name = "geton_station_id"
    usage_df = usage_df.rename(columns={"user_count":"usage"})
    return usage_df

def create_station_df(usage_df):
    geton_station_columns = ['geton_station_id', 'geton_stataion_name', 'geton_station_longitude', 'geton_station_latitude']
    getoff_station_columns = ['getoff_station_id', 'getoff_station_name', 'getoff_station_longitude', 'getoff_station_latitude']
    
    station_columns = ['station_id', 'station_name', 'station_longitude', 'station_latitude']
    
    geton_rename_dict = {}
    getoff_rename_dict = {}
    for i, column in enumerate(station_columns):
        geton_rename_dict[geton_station_columns[i]] = column
        getoff_rename_dict[getoff_station_columns[i]] = column
    
    geton_station_df = usage_df[geton_station_columns].drop_duplicates().rename(columns = geton_rename_dict)
    getoff_station_df = usage_df[getoff_station_columns].drop_duplicates().rename(columns = getoff_rename_dict)
    
    station_df = pd.concat([geton_station_df, getoff_station_df]).drop_duplicates()
    station_id_df = station_df[['station_id']].drop_duplicates()
    station_df = pd.merge(station_id_df, station_df, how ='right')
    
    station_df = station_df.dropna()
    
    # 예외처리 - 하나의 id에 대하여 여러 경도, 위도 존재
    id_count_df = pd.DataFrame(station_df['station_id'].value_counts()).reset_index().rename(columns = {'index':'station_id', 'station_id':'count'})
    exception_df = id_count_df[id_count_df['count'] >1]
    exception_id_list = list(exception_df['station_id'])
    for exception_id in exception_id_list:
        temp_df = station_df[station_df['station_id'] == exception_id]
        station_df = station_df[station_df['station_id'] != exception_id]
        station_df = pd.concat([station_df, temp_df.head(1)])
    ######################################################################                            
    return station_df

# 정류장 주소 추가
def get_address(loc_x, loc_y, min_x = 126.531891, min_y = 33.399409, key = "E20F6493-C13D-3F6F-AC90-D5BB2F239901"):
    loc_x = round(float(loc_x), 7)
    loc_y = round(float(loc_y), 7)
    url_form = "http://api.vworld.kr/req/address?service=address&request=getAddress&version=2.0&crs=epsg:4326&point={},{}&format=xml&type=both&zipcode=true&simple=false&key={}"
    url = url_form.format(loc_x, loc_y, key)
    response = requests.get(url)
    tree = elemTree.fromstring(response.text)
    branch = ""
    try:
#     road = tree.find("result").find("item[2]").find("text").text
        branch = tree.find("result").find("item[1]").find("text").text
    except:
#         print("현재 좌표: ",(loc_x, loc_y), "주소 없음")
        x_left = str(int(loc_x))
        y_left = str(int(loc_y))
        
        
        if min_x < loc_x:
            x_right = str((int(loc_x*100000)-2)%100000)
        else:
            x_right = str((int(loc_x*100000)+2)%100000)
            
        if min_y < loc_y:
            y_right = str((int(loc_y*100000)-2)%100000)
        else:
            y_right = str((int(loc_y*100000)+2)%100000)
            
        loc_x = float(x_left+"."+x_right)
        loc_y = float(y_left+"."+y_right)
        return get_address(loc_x, loc_y)
            
    return branch

def set_address_column(df):
    df = pd.DataFrame.copy(df)
    df["station_address"] = ""
    for i in tqdm(df.index):
        df.loc[i, "station_address"] = get_address(float(df.loc[i, "station_longitude"]), float(df.loc[i, "station_latitude"]))
    return df

# 이용자 추출
def create_user_df(usage_df):    # 유저 목록 생성
    return usage_df[["user_id"]].drop_duplicates()

# 이용기간 및 이용 날짜 분석
def analyze_usage_date(user_df, usage_df):
    grouped = usage_df[["user_id", "base_date"]].drop_duplicates().groupby(by=["user_id"], as_index=False)
    first_date_df = grouped.min().rename(columns = {"base_date" : "first_date"})
    last_date_df = grouped.max().rename(columns = {"base_date" : "last_date"})
    use_days_df= grouped.count().rename(columns = {"base_date" : "use_days"})
    user_df = pd.merge(user_df, first_date_df, on="user_id")
    user_df = pd.merge(user_df, last_date_df, on="user_id")
    user_df = pd.merge(user_df, use_days_df, on="user_id")
    user_df['first_date'] = user_df['first_date'].apply(lambda x : datetime.datetime.strptime(str(x), "%Y%m%d").date()) 
    user_df['last_date'] = user_df['last_date'].apply(lambda x : datetime.datetime.strptime(str(x), "%Y%m%d").date())
    user_df["period"] = user_df["last_date"] - user_df["first_date"]
    user_df["period"] = user_df["period"].apply(lambda x : int(str(x).split(" ")[0])+1)
    return user_df

# 버스 이용량 분석
def analyze_usage_num(user_df, usage_df):
    grouped = usage_df[["user_id", "base_date"]].groupby(by=["user_id"], as_index=False)
    count_df = grouped.count().rename(columns = {"base_date" : "usage"})
    user_df = pd.merge(user_df, count_df, on="user_id")
    return user_df

# 이용 비율 분석(%)
def analyze_usage_ratio(user_df):
    user_df['usage_ratio'] = user_df['use_days'].apply(lambda x : float(x))
    user_df['usage_ratio'] = user_df['usage_ratio']/user_df["period"]
    user_df['usage_ratio'] = user_df['usage_ratio'].apply(lambda x : int(x*100))
    return user_df

# 첫 승차 정류장과 마지막 하차 정류장 분석
def analyze_start_end(user_df, usage_df):
    grouped = usage_df.sort_values(['geton_datetime']).groupby(by=["user_id"], as_index=False)
    first = grouped.first()
    last = grouped.last()
    first['first_station'] = first['geton_stataion_name'].apply(lambda x : "airport" if str(x).find("공항") != -1 else "harbor" if (
                                                             bool(str(x).find("여객터미널") != -1)| 
                                                             bool(str(x).find("여객선")     != -1)|
                                                             bool(str(x).find("제6부두")    != -1)|
                                                             bool(str(x).find("제4부두")    != -1)|
                                                             bool(str(x).find("임항로")     != -1)|
                                                             bool(str(x).find("제주해양경찰서") != -1)) else "other")
    last['last_station'] = last['getoff_station_name'].apply(lambda x : "airport" if str(x).find("공항") != -1 else "harbor" if (
                                                             bool(str(x).find("여객터미널") != -1)| 
                                                             bool(str(x).find("여객선")     != -1)|
                                                             bool(str(x).find("제6부두")    != -1)|
                                                             bool(str(x).find("제4부두")    != -1)|
                                                             bool(str(x).find("임항로")     != -1)|
                                                             bool(str(x).find("제주해양경찰서")!= -1))  else "other")
    first2 = first[["user_id", "first_station"]]
    last2 = last[["user_id", "last_station"]]
    flag_df = pd.merge(first2, last2, on = "user_id")
    flag_df["both"] = (flag_df["first_station"]!='other') & (flag_df["last_station"]!='other')
    flag_df["first"] = (flag_df["first_station"]!='other') & (flag_df["last_station"]=='other')
    flag_df["last"] = (flag_df["first_station"]=='other') & (flag_df["last_station"]!='other')
    flag_df["neither"] = (flag_df["first_station"]=='other') & (flag_df["last_station"]=='other')
    flag_df = flag_df.reset_index()
    flag_df = flag_df[['user_id', 'first_station', 'last_station', 'both', 'first', 'last', 'neither']]
    user_df = pd.merge(user_df, flag_df)
    return user_df

# 관광객 추출
def extract_used_station(user_df, usage_df, station_df, case):
    user_df = user_df[user_df[case] == True]
    user_list = list(user_df['user_id'])
    usage_df = usage_df.query('{} in {}'.format("user_id", user_list))
    geton_station = pd.DataFrame(usage_df['geton_station_id'].value_counts()).reset_index().rename( columns = {'index':'station_id', 'geton_station_id': 'geton_usage'} )
    getoff_station = pd.DataFrame(usage_df['getoff_station_id'].value_counts()).reset_index().rename( columns = {'index':'station_id', 'getoff_station_id': 'getoff_usage'} )
    
    station_df = pd.merge(station_df, geton_station, on='station_id')
    station_df = pd.merge(station_df, getoff_station, on='station_id')
    
    station_df['total_usage'] = station_df['geton_usage'] + station_df['getoff_usage']
    
    return station_df

# case1~2에서 추출한 관광 관련 정류장 방문 횟수 분석
def analyze_tour_station_visit(user_df, usage_df, tour_station_df):
    # 기존에 해당 컬럼이 존재한다면 삭제
    if "tour_visit" in user_df.columns:
        del user_df["tour_visit"]
    
    tour_station_df = tour_station_df[tour_station_df['tour_station'] == True]
    tour_station_df = tour_station_df[tour_station_df['total_usage'] >= 150]
    tour_station_id_list = list(tour_station_df['station_id'])

    usage_df = usage_df[usage_df["geton_station_id"].apply(lambda x : True if x in tour_station_id_list else False)]
    usage_df = usage_df[usage_df["getoff_station_id"].apply(lambda x : True if x in tour_station_id_list else False)]

    geton_visit_df = usage_df[['user_id', 'geton_station_id']].rename(columns = {"geton_station_id":"tour_visit"})
    getoff_visit_df = usage_df[['user_id', 'getoff_station_id']].rename(columns = {"getoff_station_id":"tour_visit"})

    tour_visit_df = pd.concat([geton_visit_df, getoff_visit_df]).drop_duplicates()
    tour_visit_df = tour_visit_df.groupby(by="user_id").count()

    user_df = pd.merge(user_df, tour_visit_df[["tour_visit"]], on="user_id", how="outer").fillna(0)
    return user_df

def analyze_case(user_df):
    cases = []
    
    # 공항 이용 유형 고려
    both = user_df[user_df["both"]]
    first = user_df[user_df["first"]]
    last = user_df[user_df["last"]]
    neither = user_df[user_df["neither"]]
    
    # 관광 정류장 방문횟수 고려
    cases.append(both[both["tour_visit"] >= 1])
    cases.append(both[both["tour_visit"] <  1])
    
    cases.append(first[first["tour_visit"] >= 3])
    cases.append(first[first["tour_visit"] <  3])
    
    cases.append(last[last["tour_visit"] >= 3])
    cases.append(last[last["tour_visit"] <  3])
    
    cases.append(neither[neither["tour_visit"] >= 4])
    cases.append(neither[neither["tour_visit"] <  4])
    
    for i in range(0, 8):
        cases[i]["case"] = i+1
    
    user_df = pd.concat(cases)
    return user_df

def extract_tourist(user_df, case, period, usage_ratio):
    select = user_df.columns
    
    # 케이스 추출
    user_df = user_df[user_df["case"] == case]
    # 이용 기간 고려
    user_df = user_df[(period[0] <= user_df["period"]) & (user_df["period"] <= period[1])]
    # 이용 비율 고려
    user_df = user_df[user_df['usage_ratio'] >= usage_ratio]
    return user_df

def analyze_tourist(user_df):
    tourist_df_list = []
    tourist_df_list.append(extract_tourist(user_df, 1, (2, 15), 60))
    tourist_df_list.append(extract_tourist(user_df, 2, (2, 15), 70))
    tourist_df_list.append(extract_tourist(user_df, 3, (2, 15), 70))
    tourist_df_list.append(extract_tourist(user_df, 5, (2, 15), 80))
    tourist_df_list.append(extract_tourist(user_df, 7, (2, 15), 90))
    
    tourist_df = pd.concat(tourist_df_list)
    tourist_id_list = list(tourist_df["user_id"])
    
    user_df['tourist'] = user_df["user_id"].apply(lambda x : True if x in tourist_id_list else False)
    
    return user_df

# 정류장 분석
def analyze_station_usage(station_df, usage_df, user_df):
    usage_df = usage_df[['user_id','geton_station_id', 'getoff_station_id']]
    
    merged_df = pd.merge(usage_df, user_df, on="user_id")
    tourist_usage_df = merged_df[merged_df['tourist'] == True]
    regident_usage_df = merged_df[merged_df['tourist'] == False]
    
    tourist_geton_df = pd.DataFrame(tourist_usage_df["geton_station_id"].value_counts()).reset_index().rename(columns = {"index":"station_id", "geton_station_id":"tour_geton_usage"})
    regident_geton_df = pd.DataFrame(regident_usage_df["geton_station_id"].value_counts()).reset_index().rename(columns = {"index":"station_id", "geton_station_id":"regident_geton_usage"})
    
    tourist_getoff_df = pd.DataFrame(tourist_usage_df["getoff_station_id"].value_counts()).reset_index().rename(columns = {"index":"station_id", "getoff_station_id":"tour_getoff_usage"})
    regident_getoff_df = pd.DataFrame(regident_usage_df["getoff_station_id"].value_counts()).reset_index().rename(columns = {"index":"station_id", "getoff_station_id":"regident_getoff_usage"})


    station_df = pd.merge(station_df, tourist_geton_df, on="station_id", how="outer").fillna(0)
    station_df = pd.merge(station_df, regident_geton_df, on="station_id", how="outer").fillna(0)
    station_df = pd.merge(station_df, tourist_getoff_df, on="station_id", how="outer").fillna(0)
    station_df = pd.merge(station_df, regident_getoff_df, on="station_id", how="outer").fillna(0)
    
    station_df['total_usage'] = station_df['tour_geton_usage'] + station_df['tour_getoff_usage'] + station_df['regident_geton_usage'] + station_df['regident_getoff_usage']
    station_df = station_df.sort_values(by="total_usage", ascending = False)
    return station_df

# 시각화
def draw_period_user(plt, user_df, title=""):
    data = user_df['period'].value_counts().reset_index().sort_values('index')
    data_x = data['index']
    data_y = data['period']
    
    plt.bar(data_x, data_y)
    if title == "":
        plt.title('방문 기간별 이용자 수', fontsize=30)
    else:
        plt.title(title, fontsize=30)
    plt.xlabel('방문 기간', fontsize = 30)
    plt.ylabel('이용자 수', fontsize = 30)
    return plt

def draw_period_user_per_case(user_df):
    plt.figure(figsize=(16, 35))

    for i in range(8):
        plt.subplot(8, 1, 1+i)
        case_df = user_df[user_df["case"] == 1+i]
        title = "case{} (total {}명)".format(1+i, len(case_df))
        draw_period_user(plt, case_df, title=title)
    plt.suptitle("방문 기간별 이용자 수", fontsize=30, y = 1.02)
    plt.tight_layout()  
    plt.show()
def draw_usage_ratio_analysis_graph(user_df):
    # 데이터 분석
    columns = ['both', 'first', 'last', 'neither']
    ratio_values = {}
    count_values = {}
    for column in columns:
        count_values[column] = []
        ratio_values[column] = []
    
    window_size = 10
    for i in range(0, 101, window_size):
        sample_df = user_df[(i <=user_df["usage_ratio"]) & (user_df["usage_ratio"] <= i+window_size)]
        total_count = len(sample_df)
        for column in columns:
            count = len(sample_df[sample_df[column] == True])
            count_values[column].append(count)
            ratio_values[column].append(count/total_count)
    
    #그래프 그리기
    plt.figure(figsize=(16, 30))
    
    #그래프 - 전체 그래프
    plt.subplot(5, 2, 1)
    for column in columns:
        plt.plot(list(range(0, 101, window_size)), count_values[column])
    plt.legend(columns, loc = 0)
    plt.title('이용 비율별 이용자 수({})'.format(column))
    plt.xlabel("이용자")
    plt.ylabel("유형 비율")

    plt.subplot(5, 2, 2)
    for column in columns:
        plt.plot(list(range(0, 101, window_size)), ratio_values[column])
    plt.legend(columns, loc = 0)
    plt.title('이용 비율 별({})'.format(column))
    plt.xlabel("이용 비율")
    plt.ylabel("유형 비율")
    
    #그래프 - 유형별 그래프
    for idx, column in enumerate(columns):
        plt.subplot(5, 2, 2*idx+3)
        plt.plot(list(range(0, 101, window_size)), count_values[column])
        plt.legend(columns, loc = 0)
        plt.title('이용 비율별 이용자 수({})'.format(column))
        plt.xlabel("이용 비율")
        plt.ylabel("이용자 수")

        plt.subplot(5, 2, 2*idx+4)
        plt.plot(list(range(0, 101, window_size)), ratio_values[column])
        plt.legend(columns, loc = 0)
        plt.title('이용 비율 별({})'.format(column))
        plt.xlabel("이용 비율")
        plt.ylabel("유형 비율")
    plt.tight_layout() 
    return plt

def draw_tour_visit_analysis_graph(user_df):
    # 데이터 분석
    columns = ['both', 'first', 'last', 'neither']
    ratio_values = {}
    count_values = {}
    for column in columns:
        count_values[column] = []
        ratio_values[column] = []

    max_visit = int(user_df["tour_visit"].max())
    for i in range(max_visit):
        sample_df = user_df[(i <=user_df["tour_visit"]) & (user_df["tour_visit"] <= i+1)]
        total_count = len(sample_df)
        for column in columns:
            count = len(sample_df[sample_df[column] == True])
            count_values[column].append(count)
            ratio_values[column].append(count/total_count)
    
    # 그래프 그리기
    plt.figure(figsize=(16, 30))
    
    # 그래프 - 전체 그래프
    plt.subplot(5, 2, 1)
    for idx, column in enumerate(columns):
        plt.plot(list(range(0, max_visit, 1)), count_values[column])
        plt.legend(columns, loc = 0)
        plt.title('방문한 관광 정류장별 이용자 수({})'.format("total"))
        plt.xlabel("방문한 관광 정류장 수")
        plt.ylabel("이용자 수") 
        
    # 그래프 - 유형별 그래프
    plt.subplot(5, 2, 2)
    for idx, column in enumerate(columns):
        plt.plot(list(range(0, max_visit, 1)), ratio_values[column])
        plt.legend(columns, loc = 0)
        plt.title('방문한 관광 정류장별 이용자 비율({})'.format("total"))
        plt.xlabel("방문한 관광 정류장 수")
        plt.ylabel("유형 비율")
        
    for idx, column in enumerate(columns):
        
        plt.subplot(5, 2, 2*(idx)+3)
        plt.plot(list(range(0, max_visit, 1)), count_values[column])
        plt.legend([columns[idx]], loc = 0)
        plt.title('방문한 관광 정류장별 이용자 수({})'.format(column))
        plt.xlabel("방문한 관광 정류장 수")
        plt.ylabel("이용자 수")

        plt.subplot(5, 2, 2*(idx)+4)
        plt.plot(list(range(0, max_visit, 1)), ratio_values[column])
        plt.legend([columns[idx]], loc = 0)
        plt.title('방문한 관광 정류장별 이용자 비율({})'.format(column))
        plt.xlabel("방문한 관광 정류장 수")
        plt.ylabel("유형 비율")
    plt.tight_layout() 
    return plt

def draw_period_analysis_graph(user_df):
    wedgeprops={'width': 0.65, 'edgecolor': 'w', 'linewidth': 5}

    columns = ['both', 'first', 'last', 'neither']
    values = [[], [], [], []]
    period = 90
    term = 1
    for period in list(range(1, period+1, term)):
        user_df2 = user_df[(period-1 < user_df['period']) & (user_df['period'] <= period)]
        for i in range(4):
            sum = len(user_df2)
            values[i].append(len(user_df2[user_df2[columns[i]]])/sum)

    plt.rcParams["figure.figsize"] = (15,4)
    for i in list(range(4)):
        plt.plot(list(range(1, period+1, term)), values[i])

    plt.legend(columns)
    plt.title('방문 기간별, 공항 및 항만 이용 유형별 비율')
    plt.xlabel("방문기간")
    plt.ylabel("이용비율")
    plt.xticks(list(range(0, period+1, 5)))
    plt.show()
    return plt

def show_od_pattern(user_df, usage_df, num):
    id = user_df.iloc[num, 0]
    select = ["base_date", "geton_datetime", "geton_stataion_name", "getoff_datetime", "getoff_station_name", "user_count"]
    od_df = usage_df.query('user_id == "{}"'.format(id)).sort_values('geton_datetime')
    return od_df[select]

# load data
def load_user_df():
    user_df = pd.read_csv("data/analysis/user_df.csv", encoding = "cp949")
    return user_df

def load_station_df():
    station_df = pd.read_csv("data/analysis/station_df.csv", encoding = "cp949")
    return station_df

def load_cluster_df():
    cluster_df = pd.read_csv("data/analysis/cluster_df.csv", encoding = "cp949")    
    return cluster_df

def load_cluster_station_df():
    user_df = pd.read_csv("data/analysis/cluster_station_df.csv", encoding = "cp949")
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