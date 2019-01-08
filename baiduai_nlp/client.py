import os
import sys
sys.path.append("/Users/Kei/OneDrive/scripts")
import pandas as pd
import keys
import paths
from aip import AipNlp

client = AipNlp(keys.APP_ID, keys.API_KEY, keys.SECRET_KEY)


def sentiment_analysis(directory):
    for f in os.listdir(os.fsencode(directory)):
        if f.endswith(b'_uncensored.csv'):
            file_name = f.decode('utf-8')
            df = pd.read_csv(directory + '/' + f.decode('utf-8'))

            print(df.shape)
            print('*'*20)
            
            pos = []
            neg = []
            sentiment = []
            for index, data in df.iterrows():
                ## convert invalid characters
                data['content'] = data['content'].encode("gbk", "ignore")
                data['content'] = data['content'].decode("gbk")
                result = client.sentimentClassify(data['content'])
                ## monitor progress
                print(index)
                try:
                    pos.append(result['items'][0]['positive_prob'])
                    neg.append(result['items'][0]['negative_prob'])
                    sentiment.append(result['items'][0]['sentiment'])
                except:
                    pos.append(0)
                    neg.append(0)
                    sentiment.append(0)

            df["pos_sent"] = pos
            df["neg_sent"] = neg
            df["sentiment"] = sentiment

            print(df.shape)

            df.to_csv(directory + '/' + 'sent_' + file_name, index=False)

sentiment_analysis(paths.features)
