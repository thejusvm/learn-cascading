import glob
import json
import numpy as np
import os
import tensorflow as tf
import time
from functools import partial

import mind_palace.product_ranker.constants as CONST
from mind_palace.commons.helpers import logBreak
from mind_palace.product_ranker.prepare_data.product_attributes_dataset import read_integerized_attributes

"""
    This file processes the output of integerize_clickstream to generate tfRecords file,
    which becomes the input to for train_v2.
    This file apart from creating tfRecords, also uses integerize_product_attributes to generate negative samples.
"""

def int_json(s) :
    loads = json.loads(s)
    return np.array(loads, dtype=int)
    # return map(lambda x : int(x), loads)

def handle_padding(data, underlying_array) :
    datalen = len(data)
    num = min(datalen, len(underlying_array))
    underlying_array[:num] = data[:num]
    return underlying_array

def handle_negatives(negatives, negative_random):
    merged = handle_padding(negatives, negative_random)
    return merged


def handle_clicks(clicks, num_click_context, pad_int):
    clicks_padded = np.ones(num_click_context, dtype = np.int) * pad_int
    merged = handle_padding(clicks, clicks_padded)
    return merged

def filter_min_context_click(min_click_count, line) :
    line_split =  line.split('\t')
    click_data = json.loads(line_split[2])
    allow = len(click_data) >= min_click_count
    return allow

def _parse_line(num_attributes, num_negative_samples, product_to_attributes, line) :
    line_split =  line.split('\t')
    return_features = []
    # start = time.clock()
    random_ints = np.random.randint(len(product_to_attributes), size=num_negative_samples)
    negative_samples = product_to_attributes[random_ints, :]
    # print "fetch one random sample batch : " + str(time.clock() - start)

    for counter in range(num_attributes) :
        num_fields_per_attribute = len(CONST.OUTPUTS_PER_ATTRIBUTE)
        positive = int(line_split[num_fields_per_attribute * counter + 0])
        negatives = int_json(line_split[num_fields_per_attribute * counter + 1])
        clicks = int_json(line_split[num_fields_per_attribute * counter + 2])
        negatives = handle_negatives(negatives, negative_samples[:, counter])
        pad_index = CONST.DEFAULT_DICT_KEYS.index(CONST.PAD_TEXT)
        # clicks = handle_clicks(clicks, num_click_context, pad_index)
        return_features += [[positive],  negatives, clicks]
    return return_features

class ClickstreamDataset :

    def __init__(self,
                 num_attributes,
                 num_negative_samples = 20,
                 min_click_context = 0,
                 batch_size = None,
                 shuffle = True,
                 product_to_attributes = None):
        self.filenames = tf.placeholder(tf.string, shape=[None])
        self.dataset = tf.contrib.data.Dataset.from_tensor_slices(self.filenames)
        self.dataset = self.dataset.flat_map(
            lambda filename: (
                tf.contrib.data.TextLineDataset(filename)
                    .skip(1)))


        self.output_type = [tf.int64 for _ in range(num_attributes * 3)]
        self.dataset = self.dataset.filter(lambda line :
                                 tf.py_func(partial(filter_min_context_click, min_click_context), [line], [tf.bool]))
        self.dataset = self.dataset.map(lambda line : tuple(tf.py_func(partial(_parse_line, num_attributes, num_negative_samples, product_to_attributes), [line],
                                                             self.output_type)))

        if shuffle :
            self.dataset = self.dataset.shuffle(buffer_size=10000)
        if batch_size != None :
            self.dataset = self.dataset.batch(batch_size)

        self.iterator = self.dataset.make_initializable_iterator()
        self.get_next = self.iterator.get_next()

    def initialize_iterator(self, sess, ctr_data_path):
        sess.run(self.iterator.initializer, feed_dict={self.filenames : ctr_data_path})

def generate_feature_names(attributes):
    feature_suffixes = ["_positive_input", "_negative_input", "_click_input"]
    features = []
    for attribute in attributes :
        for feature_suffix in feature_suffixes :
            features.append(attribute + feature_suffix)

    return features

def add_to_record(record, feature_name, feature_value) :
    record.features.feature[feature_name].int64_list.value.extend(feature_value)

def enhance_clickstream(attributes, integerized_product_attributes_path, integerized_ctr_data_path, output_path) :
    features_names = generate_feature_names(attributes)
    num_features = len(features_names)

    product_to_attributes = read_integerized_attributes(attributes, integerized_product_attributes_path, attributes[0])
    dataset = ClickstreamDataset(num_attributes=len(attributes),
                                 product_to_attributes=product_to_attributes,
                                 shuffle=False)

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    sess = tf.Session()
    counter = 0
    for file in integerized_ctr_data_path :
        print "processing file : " + file
        dataset.initialize_iterator(sess, [file])
        output_file = output_path + "/part-" + str(counter)
        writer = tf.python_io.TFRecordWriter(output_file)
        print "writing to file : " + output_file
        row_counter = 0
        start = time.clock()
        while True :
            try:
                record = tf.train.Example()
                row = sess.run(dataset.get_next)
                [add_to_record(record, features_names[i], row[i]) for i in range(num_features)]
                writer.write(record.SerializeToString())
                if row_counter % 1000 == 0 :
                    print "done processing : " + str(row_counter)
                row_counter += 1
            except tf.errors.OutOfRangeError:
                break
        print "processed file in " + str(time.clock() - start)
        counter += 1
        logBreak()


if __name__ == '__main__' :

    attributes = ["productId", "brand", "vertical"]
    data_path = glob.glob("/home/thejus/workspace/learn-cascading/data/sessionExplodeWithAttributes-201708.MOB.smaller.int/integerized_clickstream/part-*")
    product_attributes_path = "/home/thejus/workspace/learn-cascading/data/sessionExplodeWithAttributes-201708.MOB.smaller.int/integerized_attributes"
    output_path = "/home/thejus/workspace/learn-cascading/data/sessionExplodeWithAttributes-201708.MOB.smaller.int/test_enhance"


    enhance_clickstream(attributes, product_attributes_path, data_path, output_path)