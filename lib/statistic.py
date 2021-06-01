import numpy as np
import pandas as pd


#확률 분포 통계 용 함수
def get_mean_from_ratio_df_by_one_column(df, expected_column, ratio_column):
    df = df.copy()
    df["E * P"] = df[expected_column]*df[ratio_column]
    return df["E * P"].sum()

def get_variance_from_ratio_df_by_one_column(df, expected_column, ratio_column):
    df = df.copy()
    df["E * E"] = df[expected_column] * df[expected_column]
    mean = get_mean_from_ratio_df_by_one_column(df, expected_column, ratio_column)
    return df["E * E"].mean() - mean
    
def get_std_diviation_from_ratio_df_by_one_column(df, expected_column, ratio_column):
    variance = get_variance_from_ratio_df_by_one_column(df, expected_column, ratio_column)
    return variance**(0.5)

def get_stats_from_ratio_df_by_one_column(df, expected_column, ratio_column):
    df = df.copy()
    df["E * P"] = df[expected_column]*df[ratio_column]
    df["E * E"] = df[expected_column] * df[expected_column]
    mean = df["E * P"].sum()
    variance = df["E * E"].mean() - mean
    std_diviation = variance**(0.5)
    return mean, variance, std_diviation
    
def get_stats_df_from_ratio_df_by_all_columns(df, expected_column):
    ratio_columns = list(df.columns)
    ratio_columns.remove(expected_column)
    result_df = pd.DataFrame(columns = ["mean", "variancd", "std_divation"])
    for ratio_column in ratio_columns:
        m, v, s_d = get_stats_from_ratio_df_by_one_column(df, expected_column, ratio_column)
        result_df.loc[ratio_column] = [m, v, s_d]

    return result_df

#df에서 target_column각각에 대한 수를 result_column으로 생성하여 반환
def get_count_df(df, target_column, result_column):
    df = df.copy()[[target_column]]
    df[result_column] = 1
    df = df.groupby(by=target_column).count().reset_index()
    return df

# target컬럼 요소들에 대해 분포도를 구하고 result_column으로 rename한 df 반환
def get_ratio_df_from_one_df(df, target_column, result_column):
    df = df.copy()[[target_column]]
    df[result_column] = 1
    df = df.groupby(by=target_column).sum().reset_index()
    total = df[result_column].sum()
    df[result_column] = df[result_column]/total
    return df

# df_list의 모든 df에 대해한 비율 df를 반환
# df_list의 모든 요소는 target_column을 포하해야한다.
def get_ratio_df_from_all_df(df_list, target_column, result_columns):
    result_df = pd.DataFrame(columns = [target_column])
    for i, df in enumerate(df_list):
        temp_df = get_ratio_df_from_one_df(df, target_column, result_columns[i])
        result_df = pd.merge(result_df, temp_df, on=target_column, how="outer")
    
    result_df.fillna(0, inplace=True)
    return result_df

# 두 분포도의 유사성 계산
def how_much_overlap(df, a_column, b_column):
    df = df.copy()
    df["min"] = df[[a_column, b_column]].apply(lambda x : x[0] if x[0] < x[1] else x[1], axis = 1)
    return df["min"].sum()
