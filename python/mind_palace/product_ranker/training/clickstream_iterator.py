import tensorflow as tf
import time
import json
import numpy as np
import mind_palace.product_ranker.constants as CONST
from mind_palace.commons.helpers import logBreak
import glob
from functools import partial
from product_attributes_dataset import integerized_attributes
import sys
import os
from enhance_clickstream import generate_feature_names

# def _deserialize_function(_example_protos):
#     return [tf.deserialize_many_sparse(_example_proto, tf.int64) for _example_proto in _example_protos]


def _parse_function(feature_names, features, example_proto):
    parsed_features = tf.parse_single_example(example_proto, features)
    # return parsed_features[feature_names[0]]
    return tuple([parsed_features[feature_name].values for feature_name in feature_names])
    # return tuple([tf.sparse_tensor_to_dense(parsed_features[feature_name]) for feature_name in feature_names])
    # return tf.sparse_tensor_to_dense(parsed_features[feature_names[0]])

class ClickstreamDataset :

    def __init__(self, attributes, shuffle=True, batch_size=None):
        self.filenames = tf.placeholder(tf.string, shape=[None])
        self.dataset = tf.contrib.data.TFRecordDataset(self.filenames)
        feature_names = generate_feature_names(attributes)
        features = dict([[feature_name, tf.VarLenFeature(dtype=tf.int64)] for feature_name in feature_names])
        self.dataset = self.dataset.map(lambda row : _parse_function(feature_names, features, row))

        if shuffle :
            self.dataset = self.dataset.shuffle(buffer_size=100000)
        if batch_size != None :
            padding_value = CONST.DEFAULT_DICT_KEYS.index(CONST.PAD_TEXT)
            num_features = len(feature_names)
            padding_values = tuple([tf.constant(padding_value, dtype=tf.int64) for _ in range(num_features)])
            padded_shapes = tuple([[None] for _ in range(num_features)])
            self.dataset = self.dataset.padded_batch(batch_size, padded_shapes = padded_shapes, padding_values=padding_values)

        self.iterator = self.dataset.make_initializable_iterator()
        self.get_next = self.iterator.get_next()

    def initialize_iterator(self, sess, ctr_data_path):
        sess.run(self.iterator.initializer, feed_dict={self.filenames : ctr_data_path})

if __name__ == '__main__' :

    path = glob.glob("/home/thejus/workspace/learn-cascading/data/sessionExplodeWithAttributes-201708.MOB.large.search.tfr/part-*")
    sess = tf.Session()
    attributes = ["productId", "brand", "vertical"]
    attributes = ["productId"]
    dataset = ClickstreamDataset(attributes, batch_size=10)
    dataset.initialize_iterator(sess, path)
    get_next = dataset.get_next
    print get_next
    print sess.run(get_next)
