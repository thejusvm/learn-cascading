import glob
import tensorflow as tf
import time
import pandas as pd
import numpy as np
import sys

from multiprocessing import Pool
from contextlib import closing
from functools import partial

import mind_palace.product_ranker.constants as CONST


def dense_split(tensor, delimiter="\t"):
    tensor = tf.expand_dims(tensor, 0)
    tensor = tf.string_split(tensor, delimiter=delimiter)
    tensor = tf.sparse_tensor_to_dense(tensor, default_value="")
    tensor = tf.squeeze(tensor, axis=0)
    return tensor

def _parse_feature(feature_tensor) :
    feature_tensor = dense_split(feature_tensor, ",")
    feature_tensor = tf.string_to_number(feature_tensor, out_type=tf.int64)
    return feature_tensor

def _parse_function(feature_indices, row):
    row_split = dense_split(row)
    result = [row_split[i] for i in feature_indices]
    return tuple([_parse_feature(x) for x in result])


def read_feature_names(train_path):
    with open(train_path[0], "r") as reader:
        return reader.readline().rstrip('\n').split("\t");

def to_feed_dict(feature_names, df):
    feature_values = []
    for feature_name in feature_names:
        feature_value = df[feature_name].as_matrix()
        length = len(max(feature_value, key=len))
        padded_feature_value = np.array([xi + [0] * (length - len(xi)) for xi in feature_value])
        feature_values.append(padded_feature_value)
    return tuple(feature_values)

def split_cast(data) :
    return map(int, data.split(","))

def df_to_feed_dict(feature_names, sub_df):
    sub_df = sub_df.applymap(split_cast)
    feed_dict = to_feed_dict(feature_names, sub_df)
    return feed_dict

def file_to_feed_dict(feature_names, ctr_data_path):
    df = pd.read_csv(ctr_data_path, sep="\t", dtype=str)
    feed_dict = df_to_feed_dict(feature_names, df)
    return feed_dict

class Inmemory_ClickstreamDataset :

    def __init__(self, attributes, ctr_data_path, shuffle=True, batch_size=None):
        self.ctr_data_path = ctr_data_path

        feature_names = read_feature_names(ctr_data_path)
        num_features = len(feature_names)

        start = time.time()

        print ctr_data_path
        with closing(Pool(processes=2)) as pool:
            feed_dicts = pool.map(partial(file_to_feed_dict, feature_names), ctr_data_path)

        zipped_dicts = zip(*feed_dicts)
        feed_dict = tuple([np.concatenate(d) for d in zipped_dicts])

        self.dataset = tf.contrib.data.Dataset.from_tensor_slices(feed_dict)

        print "processing input file took : " + str(time.time() - start)
        print "-------------------------------"

        if shuffle :
            self.dataset = self.dataset.shuffle(buffer_size=100000)
        if batch_size != None :
            padding_value = CONST.DEFAULT_DICT_KEYS.index(CONST.PAD_TEXT)
            padding_values = tuple([tf.constant(padding_value, dtype=tf.int64) for _ in range(num_features)])
            padded_shapes = tuple([[None] for _ in range(num_features)])
            self.dataset = self.dataset.padded_batch(batch_size, padded_shapes = padded_shapes, padding_values=padding_values)

        self.iterator = self.dataset.make_initializable_iterator()
        self.get_next = self.iterator.get_next()



    def initialize_iterator(self, sess):
        sess.run(self.iterator.initializer)



if __name__ == '__main__' :

    path = glob.glob("/Users/thejus/workspace/learn-cascading/data/sessionexplode-2017-0801.1000.tt/train/part-0000*")
    sess = tf.Session()
    attributes = ["productId", "brand", "vertical"]
    # attributes = ["productId"]

    dataset = Inmemory_ClickstreamDataset(attributes, path, batch_size=2, shuffle=False)
    dataset.initialize_iterator(sess)
    get_next = dataset.get_next
    print sess.run(get_next)
    print sess.run(get_next)
