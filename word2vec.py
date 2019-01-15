import re
import os
import numpy as np
import pandas as pd
from gensim.models import Word2Vec, KeyedVectors
import file_location


def load_word2vec_model():
    return Word2Vec.load(file_location.word2vec_model)

def load_data():
    non_words = re.compile(r'\W')
    data_list = df['content'].values.tolist()
    final_data_list = []

    for data in data_list:
        words_list = []
        data = data.split(' ')
        for word in data:
            if word != '' and not (non_words.match(word)):
                words_list.append(word)
        final_data_list.append(words_list)
        
    return final_data_list

def get_vectors_average():
    vector = []
    vectors_average = []

    oov_counter = 0
    word_counter = 0
    for item in final_data_list:
        for word in item:
            try:
                vector.append(model.wv[word])
                word_counter += 1
            except KeyError:
                # keeps count of OOV
                oov_counter += 1
                continue
        if len(vector) == 0:
            vectors_average.append(np.zeros(200))
        else:
            vectors_average.append(np.mean(vector, axis=0))
        vector = []
    
    print('oov_counter:', oov_counter)
    print('word_counter:', word_counter)
    total = oov_counter + word_counter
    print('total words:', total)
    percent = oov_counter / total * 100
    print('oov %:', percent)

    df = pd.DataFrame(vectors_average, columns=list(range(1, 201)))
    df.to_csv(directory + '/w2v_' + file_name, index=False)

    return vectors_average

if __name__ == '__main__':
    directory = file_location.jed_features
    model = load_word2vec_model()

    for f in os.listdir(os.fsencode(directory)):
        if f.endswith(b'.csv'):
            file_name = f.decode('utf-8')
            df = pd.read_csv(directory + '/' + file_name)
            print(df.shape)
            final_data_list = load_data()
            get_vectors_average()
