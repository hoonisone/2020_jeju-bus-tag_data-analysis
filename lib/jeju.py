import numpy as np
import pandas as pd

# 제주 관광객 수 데이터
TOURIST_NUM_STATS_DATA = {"2019_Jun":{"domestic_tourist":1155020, "foreign_tourist":152197}, 
                          "2019_Jul":{"domestic_tourist":1157447, "foreign_tourist":152629},
                          "2019_Aug":{"domestic_tourist":1243132, "foreign_tourist":178323}}


# 제주 관광객 체류 기간 데이터
STAY_PERIOD_STATS_DATA = {"period":  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                          "domestic_tourist":[0.003, 0.057, 0.618, 0.238, 0.045, 0.013, 0.015, 0.003, 0.001, 0.001, 0.006, 0, 0, 0, 0],
                          "foreign_tourist": [0.006, 0.066, 0.280, 0.296, 0.217, 0.055, 0.035, 0.019, 0.006, 0.004, 0.016, 0, 0, 0, 0]}  

# 기타 데이터(버스 이용량, )
DOMESTIC_BUS_USAGE_RATIO = 0.032
FOREIGN_BUS_USAGE_RATIO  = 0.211
TAG_UTILIZATION = 0.847

def get_jeju_tourist_num_df(tourist_num_stats_data, domestic_bus_usage_ratio, foreign_bus_usage_ratio):
    df = np.transpose(pd.DataFrame(tourist_num_stats_data))

    df["total_tourist"] = df["domestic_tourist"] + df["foreign_tourist"]
    

    df["domestic_bus_tourist"] = (df["domestic_tourist"] * domestic_bus_usage_ratio).apply(lambda x : int(x))
    df["foreign_bus_tourist"] = (df["foreign_tourist"] * foreign_bus_usage_ratio).apply(lambda x : int(x))
    df["total_bus_tourist"] = df["domestic_bus_tourist"] + df["foreign_bus_tourist"]
    return df

def get_jeju_stay_period_df(stay_period_stats_data, user_num_stats_df):
    df = pd.DataFrame(stay_period_stats_data)
    # 내국인, 외국인 관광객 비율 추출
    domestic = user_num_stats_df["domestic_bus_tourist"].sum()
    foreign = user_num_stats_df["foreign_bus_tourist"].sum()
    total = domestic + foreign
    domestic_ratio = domestic/total
    foreign_ratio = foreign/total
    df["total_tourist"] = df["domestic_tourist"]*domestic_ratio + df["foreign_tourist"]*foreign_ratio
    return df



