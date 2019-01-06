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


def clean_data(directory):
    for f in os.listdir(os.fsencode(directory)):
        if f.endswith(b'.csv'):
            cleaned = []
            file_name = f.decode('utf-8')
            df = pd.read_csv(directory + '/' + f.decode('utf-8'))

            for index, data in df.iterrows():
                data['content'] = re.sub(r'\n+', ' ', data['content']) ## newline
                data['content'] = re.sub(r'\r', ' ', data['content']) ## linebreak
                data['content'] = re.sub(r'收起全文d', '', data['content']) ## collapse
                data['content'] = re.sub(r'(//)', '', data['content']) ## slashes
                data['content'] = re.sub(r'(@.+?)[:：;]+', ' ', data['content']) ## tagged user reply
                data['content'] = re.sub(r'(@.+?)\s+', ' ', data['content']) ## tagged user
                data['content'] = re.sub(r'(@.+?)$', ' ', data['content']) ## end of text
                data['content'] = re.sub(r'(转发微博)', '', data['content']) ## retweet
                data['content'] = re.sub(r'(转：)', '', data['content']) ## reblog
                data['content'] = re.sub(r'#', ' ', data['content']) ## hashtag
                data['content'] = re.sub(r'(→_→)', '', data['content']) ## arrows
                data['content'] = re.sub(r'(回复)', '', data['content']) ## reply
                data['content'] = re.sub(r'(网页链接)', '', data['content']) ## link
                data['content'] = re.sub(r'\[.+?\]', '', data['content']) ## emoticon text
                data['content'] = re.sub(r'', ' ', data['content']) ## special symbol
                data['content'] = data['content'].strip()
                
                cleaned.append(data)

            df_cleaned = pd.DataFrame(cleaned)
            df_cleaned.to_csv(directory + '/' + file_name, index=False)


def remove_nan_and_short_rows(directory):
    for f in os.listdir(os.fsencode(directory)):
        if f.endswith(b'.csv'):
            file_name = f.decode('utf-8')
            df = pd.read_csv(directory + '/' + f.decode('utf-8'))
            print('before {}'.format(df.shape))
            for index, data in df.iterrows():
                if type(data['content']) is not str or len(data['content']) <= 5:
                    df.drop(index, inplace=True)
            print('after {}'.format(df.shape))
            df.to_csv(directory + '/' + file_name, index=False)



