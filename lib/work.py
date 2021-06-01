import numpy as np
import pandas as pd
from multiprocessing import Pool

def parallelize(func, df, core = 8):
    df_split = np.array_split(df, core)
    pool = Pool(core)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df