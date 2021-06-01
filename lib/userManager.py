import numpy as np
import pandas as pd
import datetime

def create_user_df(usage_df):    # 유저 목록 생성
    user_df = usage_df[["user_id"]].drop_duplicates()
    user_df.reset_index(inplace = True)
    del user_df["index"]
    return user_df

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

def analyze_usage_num(user_df, usage_df):
    grouped = usage_df[["user_id", "base_date"]].groupby(by=["user_id"], as_index=False)
    count_df = grouped.count().rename(columns = {"base_date" : "usage"})
    user_df = pd.merge(user_df, count_df, on="user_id")
    return user_df

def analyze_usage_ratio(user_df):
    user_df['usage_ratio'] = user_df['use_days'].apply(lambda x : float(x))
    user_df['usage_ratio'] = user_df['usage_ratio']/user_df["period"]
    user_df['usage_ratio'] = user_df['usage_ratio'].apply(lambda x : int(x*100))
    return user_df

def analyze_first_last_station_type(user_df, usage_df, station_df, first_column, last_column, case_column):
    #기존에 컬럼 값이 존재하면 삭제
    column_list = [first_column, last_column, case_column]
    for column in column_list:
        if column in user_df.columns:
            del user_df[column]
            
    # 출입정류장 추출
    airport_station_id_list = list(station_df[station_df["airport_flag"]]["station_id"])
    harbor_station_id_list  = list(station_df[station_df["harbor_flag"]]["station_id"])
    
    # user_id별 최초, 최종 이용 정류장 추출
    # 최종 하차의 경우 결측치는 -1로 대체
    grouped_usage_df = usage_df.sort_values(["geton_datetime"]).groupby(by=["user_id"], as_index=False)    
    first_usage_df = grouped_usage_df.first()[["user_id", "geton_station_id"]]
    last_usage_df = grouped_usage_df.last()[["user_id", "getoff_station_id"]].fillna(-1)
   

    # 첫 승차 및 최종 하차 정류장의 공항 또는 항만 여부 판단
    first_usage_df["first"] = first_usage_df["geton_station_id"].apply(lambda x : "airport" if np.float(x) in airport_station_id_list
                                                                                    else "harbor" if x in harbor_station_id_list
                                                                                    else "other")
    last_usage_df["last"] = last_usage_df["getoff_station_id"].apply(lambda x : "airport" if x in airport_station_id_list
                                                                                    else "harbor" if x in harbor_station_id_list
                                                                                    else "no_tag" if x == -1
                                                                                    else "other")
    
    first_usage_df = first_usage_df.copy()[["user_id", "first"]]
    last_usage_df = last_usage_df.copy()[["user_id", "last"]]
    
    flag_df = pd.merge(first_usage_df, last_usage_df, on = "user_id")
    # 첫 승차와 마지막 하차 정류장의 출입정류장 여부 판단
    flag_df["f"] = (flag_df["first"]=='airport') | (flag_df["first"]=='harbor')
    flag_df["l"] = (flag_df["last"]=='airport') | (flag_df["last"]=='harbor')
    
    flag_df["case"] = flag_df[["f", "l"]].apply(lambda x : "both" if x[0] & x[1] 
                                                else "first" if x[0] 
                                                else "last"  if x[1]
                                                else "neither", axis = 1)
    del flag_df["f"]
    del flag_df["l"]
    
    user_df = pd.merge(user_df, flag_df, on="user_id")
    return user_df

# 출입정류장 이용 횟수 분석
def analyze_visit_count(user_df, usage_df, target_station_df, result_column):
    # 속성 이름이 이미 쓰이고 있다면 제거
    if result_column in user_df.columns:
        del user_df[result_column]
    
    #필요한 속성만 추출
    target_station_df = target_station_df.copy()[["station_id"]]
    geton_df = usage_df.copy()[["user_id", "geton_station_id"]]
    getoff_df = usage_df.copy()[["user_id", "getoff_station_id"]].dropna()# 하차 데이터에 대해선 결측치 제거
    geton_df.rename(columns = {"geton_station_id":"station_id"}, inplace = True)
    getoff_df.rename(columns = {"getoff_station_id":"station_id"}, inplace = True)
    
    #승하차 정보 통합
    usage_df = pd.concat([geton_df, getoff_df])
    
    #중복 제거(동일 정류장에 대해 여러 번 방문한 경우 중복 처리)
    usage_df.drop_duplicates(inplace=True)
    
    #타켓 정류장 방문 데이터 추출
    usage_df = pd.merge(usage_df, target_station_df, on="station_id")
    
    #이용자별 방문 타켓 정류장 수 카운트
    usage_df = usage_df.groupby(by="user_id").count().reset_index().rename(columns={"station_id":result_column})
    
    # 이용자의 이용한 타켓 정류장 수 속성 삽입
    user_df = pd.merge(user_df, usage_df, on="user_id", how="outer")
    user_df.fillna(0, inplace=True)
    user_df[result_column] = user_df[result_column].apply(lambda x : int(x))
    return user_df

def extract_tourist(user_df, case, period, min_usage_ratio, min_tour_station_count):
    select = user_df.columns
    
    # 케이스 추출
    user_df = user_df[user_df["case"] == case]

    # 이용 기간 고려
    user_df = user_df[(period[0] <= user_df["period"]) & (user_df["period"] <= period[1])]
    
    # 이용 비율 고려 => 이용 비율은 관광객 여부에 큰 관계 없음
#     user_df = user_df[user_df['usage_ratio'] >= min_usage_ratio]
    
    # 방문한 관광 정류장 수 고려
    user_df = user_df[user_df["tour_count"] >= min_tour_station_count]
    
    return user_df

def analyze_and_insert_tourist_column(user_df, column_name = "result_column"):    
    if column_name in user_df.columns:
        del user_df[column_name]
    
    tourist_df_list = []

    tourist_df_list.append(extract_tourist(user_df, "both",    (2, 10), 70, 1))
    tourist_df_list.append(extract_tourist(user_df, "first",   (2, 10), 70, 3))
    tourist_df_list.append(extract_tourist(user_df, "last",    (2, 10), 80, 3))
    tourist_df_list.append(extract_tourist(user_df, "neither", (2, 10), 90, 4))

    tourist_df = pd.concat(tourist_df_list)[["user_id"]]
    tourist_df["tourist"] = True
    
    user_df = pd.merge(user_df, tourist_df, on = "user_id", how="outer").fillna(False)
    
    return user_df

def show_user_analyze(user_df):
    total_count = len(user_df)
    tourist_count = len(user_df[user_df["tourist"] == True])
    print("* Total ")
    print("  - %-7s: %d people"% ("User", total_count))
    print("  - %-7s: %d people"% ("Tourist", tourist_count))
    print("  - %-7s: %.1f%%" % ("Ratio", (tourist_count/total_count*100)))
    print("\n")
    
    case_list = ["both", "first", "last", "neither"]
    for case in case_list:
        df = user_df[user_df["case"] == case]
        total_count = len(df)
        tourist_count = len(df[df["tourist"] == True])
        print("*", case,)
        print("  - %-7s: %d people"% ("User", total_count))
        print("  - %-7s: %d people"% ("Tourist", tourist_count))
        print("  - %-7s: %.1f%%" % ("Ratio", (tourist_count/total_count*100)))
        print("\n")
    
    print("* airport user: %6d people"% (len(user_df[user_df["airport_count"]>=1])))
    print("* harbor user : %6d people"% (len(user_df[user_df["harbor_count"]>=1])))






