import os
import re
import pandas as pd
import numpy as np
import file_location

def extract_semantic_classes(directory):

    sem_dict = {}

    with open(file_location.semantics_dict, encoding="utf-8") as source:
        for line in source:
            line = line.split()
            if line[0][0] not in sem_dict.keys():
                sem_dict[line[0][0]] = []
            for value in line[1:]:
                sem_dict[line[0][0]].append(value)

    for f in os.listdir(os.fsencode(directory)):
        if f.endswith(b'_uncensored.csv'):
            file_name = f.decode('utf-8')
            df = pd.read_csv(directory + '/' + f.decode('utf-8'))
            print(df.shape)

            groups_list = []

            for index, data in df.iterrows():
                ## monitor progress
                print(index)
                groups = set()
                for word in data["content"].split(" "):
                    for key, value in sem_dict.items():
                        if word in value:
                            groups.update(key[0])

                    if len(groups) == 12:
                        break
                groups_list.append(len(groups))

            df["semantic_classes"] = groups_list
            print(df.columns.values)
            print(df.head())
            print(df.shape)
            df.to_csv(directory + '/' + file_name, index=False)
