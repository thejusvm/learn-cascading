import tensorflow as tf
import json
import time
from mind_palace.DictIntegerizer import DictIntegerizer
import glob
import sys
import numpy as np
import pandas as pd
import mind_palace.product_ranker.constants as CONST

"""
    Takes the output of prepare_product_attributes and wraps it with a tensorflow Dataset.
    returns tuples of integer representation for each attribute.
"""

deafult_unavaileble_index = CONST.DEFAULT_DICT_KEYS.index(CONST.MISSING_DATA_TEXT)

def map_to_ints(attribute_dicts, values) :
    ints = []
    for i in range(len(attribute_dicts)) :
        attribute_dict = attribute_dicts[i] #type: DictIntegerizer
        value = values[i]
        ints.append(attribute_dict.get(value))
    return ints

def not_unavailable_product(x):
    return x[0] != deafult_unavaileble_index



class ProductAttributesDataset:

    def __init__(self, attribute_names, batch_size = None, shuffle = False, repeat = False):
        self.filenames = tf.placeholder(tf.string, shape=[None])
        self.dataset = tf.contrib.data.Dataset.from_tensor_slices(self.filenames)
        self.dataset = self.dataset.flat_map(
            lambda filename: (
                tf.contrib.data.TextLineDataset(filename)))
        num_attributes = len(attribute_names)
        split_output_type = [tf.int64 for _ in range(num_attributes)]
        self.dataset = self.dataset.map(lambda line : tf.py_func(lambda x : map(int, x.split('\t')[:num_attributes]), [line], split_output_type))

        if repeat :
            self.dataset = self.dataset.repeat()

        if shuffle:
            self.dataset = self.dataset.shuffle(buffer_size=10000)

        if batch_size is not None :
            self.dataset = self.dataset.batch(batch_size)

        self.iterator = self.dataset.make_initializable_iterator()
        self.next_element = self.iterator.get_next()

    def initialize_iterator(self, sess, attributes_path):
        sess.run(self.iterator.initializer, feed_dict={self.filenames : attributes_path})

def integerized_attributes(attributes, attributes_path, index_field):
    index_field_attributes_index = attributes.index(index_field)
    num_defaults = len(CONST.DEFAULT_DICT_KEYS)
    df = pd.read_csv(attributes_path, sep="\t", index_col=index_field_attributes_index, header=None, names=attributes[1:])
    index_attribute = attributes[0]
    df[index_attribute] = df.index
    df=df[attributes]
    max_val = max(df[index_attribute])
    df.drop_duplicates(inplace=True)
    df.reindex(range(max_val), fill_value=-1)
    for i in range(num_defaults) :
        num_attributes = len(attributes)
        df.loc[i] = np.ones(num_attributes) * i
    df.sort_values(index_attribute, inplace=True)
    return df.as_matrix()

if __name__ == '__main__' :

    attributes_path = "/home/thejus/workspace/learn-cascading/data/product-attributes-integerized.MOB.large.search"
    attributes = ["productId", "brand", "vertical"]

    all_data =  integerized_attributes(attributes, attributes_path, "productId")
    for i in range(len(all_data)) :
        pid_data = all_data[i][0]
        if pid_data == -1 :
            print ": " + i
    print all_data[1]
    print all_data[56]
    print all_data[99]
    print all_data[108]
    print all_data[1000]
    print all_data[0]
    print all_data[1]
    print all_data[2]
    print all_data[3]
