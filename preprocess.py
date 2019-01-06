import os
import re
import jieba
import pandas as pd
import file_location



def add_topic_and_class_column(directory):

    for f in os.listdir(os.fsencode(directory)):
        if f.endswith(b'.csv'):
            file_name = f.decode('utf-8')
            df = pd.read_csv(directory + '/' + file_name)
            if f.endswith(b'_censored.csv') and f.startswith(b'scrapped'):          
                topic = file_name[9:-13]
                df['topic'] = topic
                df['class'] = 'censored'

            elif f.endswith(b'_uncensored.csv') and f.startswith(b'scrapped'):
                topic = file_name[9:-15]
                df['topic'] = topic
                df['class'] = 'uncensored' 

            df.to_csv(directory + '/' + file_name, index=False)   
            print('{}: {}'.format(file_name, df.shape))


def merge_csvs(directory):
    censored_df = []
    uncensored_df = []
    for f in os.listdir(os.fsencode(directory)):
        if f.endswith(b'.csv'):
            file_name = f.decode('utf-8')
            df = pd.read_csv(directory + '/' + file_name)
            if f.endswith(b'_censored.csv'):
                censored_df.append(df)
            elif f.endswith(b'_uncensored.csv'):
                uncensored_df.append(df)    

    censored_merged_df = pd.concat(censored_df, sort=False)
    uncensored_merged_df = pd.concat(uncensored_df, sort=False)

    censored_merged_df.to_csv(directory + '/' + file_name[:8] + '_all_censored.csv', index=False)
    uncensored_merged_df.to_csv(directory + '/' + file_name[:8] + '_all_uncensored.csv', index=False)

    print(censored_merged_df.shape)
    print(uncensored_merged_df.shape)




def remove_duplicates(directory):
    for f in os.listdir(os.fsencode(directory)):
        if f.endswith(b'.csv'):
            file_name = f.decode('utf-8')
            if 'all' in file_name:
                df = pd.read_csv(directory + '/' + file_name)
                print('{} before: {}'.format(file_name, df.shape))
                ## look at the duplicates if necessary
                # duplicates = pd.concat(dup for _, dup in df.groupby('content') if len(dup) > 1)
                df_dropped = df.drop_duplicates(subset='content')
                print('{} after: {}'.format(file_name, df_dropped.shape))
                df_dropped.to_csv(directory + '/' + file_name, index=False)




