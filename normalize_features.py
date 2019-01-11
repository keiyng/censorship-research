import os
import re
import pandas as pd
import numpy as np
import file_location

def compute_zscores(directory):
    non_features = ['idx', 'content', 'topic', 'class']

    for f in os.listdir(os.fsencode(directory)):
        if f.endswith(b'.csv'):
            file_name = f.decode('utf-8')
            df = pd.read_csv(directory + '/' + f.decode('utf-8'))
            df_features = pd.DataFrame()

            cols = list(df.columns)

            for non_feature in non_features:
                cols.remove(non_feature)

            for col in cols:
                col_z =  col + '_z'
                df_features[col_z] = (df[col] - df[col].mean()) / df[col].std(ddof=0)

            for non_feature in non_features:
                df_features[non_feature] = df[non_feature]

            print(df_features.shape)
            print(df_features.columns.values)


# df_cen = pd.read_csv(file_location.features + '/scrapped_all_censored.csv')
# df_uncen = pd.read_csv(file_location.features + '/scrapped_all_uncensored.csv')
# print(df_cen.describe())
# print(df_cen.mean())
# print(df_cen.sample())
# print(df_cen.corr())
# print(df_cen.shape)
# print(df_cen.columns.values)
# print(df_cen.tail())