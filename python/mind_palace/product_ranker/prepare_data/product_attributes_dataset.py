import numpy as np
import pandas as pd

import mind_palace.product_ranker.constants as CONST

"""
    Takes the output of prepare_product_attributes and wraps it with a tensorflow Dataset.
    returns tuples of integer representation for each attribute.
"""

def read_integerized_attributes(attributes, attributes_path, index_field):
    num_defaults = len(CONST.DEFAULT_DICT_KEYS)
    col_types = dict(zip(attributes, [np.int32 for _ in range(len(attributes))]))
    df = pd.read_csv(attributes_path, sep="\t", index_col=index_field, usecols=attributes, dtype=col_types)
    index_attribute = attributes[0]
    df[index_attribute] = df.index
    df=df[attributes]
    max_val = max(df[index_attribute])
    df.drop_duplicates(inplace=True)
    df.reindex(range(max_val), fill_value = -1)
    num_attributes = len(attributes)
    for i in range(num_defaults) :
        df.loc[i] = np.ones(num_attributes) * i
    df.sort_values(index_attribute, inplace=True)
    matrix = df.as_matrix()
    return matrix.astype(int)

if __name__ == '__main__' :

    attributes_path = "/home/thejus/workspace/learn-cascading/data/product-attributes-integerized.MOB.large.search"
    attributes = ["productId", "brand", "vertical"]

    all_data =  read_integerized_attributes(attributes, attributes_path, "productId")
    for i in range(len(all_data)) :
        pid_data = all_data[i][0]
        if pid_data == -1 :
            print i
    print all_data[1]
    print all_data[56]
    print all_data[99]
    print all_data[108]
    print all_data[1000]
    print all_data[0]
    print all_data[1]
    print all_data[2]
    print all_data[3]
