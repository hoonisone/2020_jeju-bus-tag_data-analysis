import numpy as np
import pandas as pd

"""
* function: target_df에 속하였는지 여부를 df에 result_column으로 삽입
"""
def insert_flag_column(df, target_df, id_column, result_column):
    target_df[result_column] = True
    target_df = target_df[[id_column, result_column]]
    if result_column in df.columns:
        del df[result_column]
    df = pd.merge(df, target_df, on=id_column, how="outer").fillna(False)
    return df