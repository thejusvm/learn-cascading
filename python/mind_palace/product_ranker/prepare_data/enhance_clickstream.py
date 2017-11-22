import glob
import json
import numpy as np
import os
import pandas as pd
import tensorflow as tf
import time
import datetime
from contextlib import closing
from functools import partial
from multiprocessing import Pool

import mind_palace.product_ranker.constants as CONST
from mind_palace.commons.helpers import logBreak
from mind_palace.product_ranker.commons import generate_feature_names
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

def handle_negatives_row(negatives_string, product_to_attributes, num_negative_samples):
    num_products = len(product_to_attributes)
    random_ints = np.random.randint(num_products, size=num_negative_samples)
    negative_samples = product_to_attributes[random_ints, :]
    merged = []
    for i in range(len(negatives_string)) :
        negative = int_json(negatives_string[i])
        padded_negatives = handle_padding(negative, negative_samples[:, i])
        merged.append(padded_negatives)
    return merged

def filter_min_context_click(min_click_count, line) :
    line_split =  line.split('\t')
    click_data = json.loads(line_split[2])
    allow = len(click_data) >= min_click_count
    return allow

def add_to_record(record, feature_name, feature_value) :
    record.features.feature[feature_name].int64_list.value.extend(feature_value)

def to_tfr_example(row, features_names) :
    record = tf.train.Example()
    num_features = len(features_names)
    [add_to_record(record, features_names[i], row[i]) for i in range(num_features)]
    return record.SerializeToString()

def get_processed_data_frame(input_file,
                             attributes,
                             product_to_attributes,
                             train_test_split_date,
                             num_negative_samples = 20,
                             min_click_context = 0) :

    df = pd.read_csv(input_file, sep ="\t")

    positive_features = generate_feature_names(attributes, [CONST.POSITIVE_COL_PREFIX])
    for positive_feature in positive_features :
        df[positive_feature] = df[positive_feature].apply(lambda x : [x])

    negative_features = generate_feature_names(attributes, [CONST.NEGATIVE_COL_PREFIX])
    handle_negs_lambda = lambda x: handle_negatives_row(x, product_to_attributes, num_negative_samples)
    df[negative_features] = df[negative_features].apply(handle_negs_lambda, axis=1)

    click_features = generate_feature_names(attributes, [CONST.CLICK_COL_PRERFIX])
    for click_feature in click_features :
        df[click_feature] = df[click_feature].apply(int_json)

    all_features = generate_feature_names(attributes, CONST.TRAINING_COL_PREFIXES)
    df["tfr"] = df[all_features].apply(lambda x : to_tfr_example(x, all_features), axis=1)

    train_test_split_timestamp = time.mktime(datetime.datetime.strptime(train_test_split_date, "%Y-%m-%d").timetuple())
    train_test_split_timestamp = train_test_split_timestamp * 1000

    train_df = df[df["timestamp"] < train_test_split_timestamp]
    test_df = df[df["timestamp"] > train_test_split_timestamp]

    return train_df["tfr"], test_df["tfr"]

def write_df_toFile(df, output_file):
    writer = tf.python_io.TFRecordWriter(output_file)
    print "writing to file : " + output_file
    start = time.time()
    for row in df:
        writer.write(row)
    print "wrote file in " + str(time.time() - start)

def enhance_file(attributes, product_to_attributes, train_test_split_date, io):
    file, train_output_file, test_output_file = io
    print "processing file : " + file
    start = time.time()
    train_df, test_df = get_processed_data_frame(file, attributes, product_to_attributes, train_test_split_date)
    print "file to processed dataframe in : " + str(time.time() - start)
    write_df_toFile(train_df, train_output_file)
    write_df_toFile(train_df, test_output_file)
    logBreak()


def enhance_clickstream(attributes, integerized_product_attributes_path, integerized_ctr_data_path, train_output_path, test_output_path, train_test_split_date, num_parallel = 1) :

    product_to_attributes = read_integerized_attributes(attributes, integerized_product_attributes_path, attributes[0])

    if not os.path.exists(train_output_path):
        os.makedirs(train_output_path)
    if not os.path.exists(test_output_path):
        os.makedirs(test_output_path)

    input_output = [[integerized_ctr_data_path[i], train_output_path + "/part-" + str(i), test_output_path + "/part-" + str(i)] for i in range(len(integerized_ctr_data_path))]
    with closing(Pool(processes=num_parallel)) as pool:
        pool.map(partial(enhance_file, attributes, product_to_attributes, train_test_split_date), input_output)


if __name__ == '__main__' :

    attributes = ["productId", "brand", "vertical"]
    data_path = glob.glob("/home/thejus/workspace/learn-cascading/data/sessionExplodeWithAttributes-201708.MOB.smaller.int/integerized_clickstream/part-*")
    product_attributes_path = "/home/thejus/workspace/learn-cascading/data/sessionExplodeWithAttributes-201708.MOB.smaller.int/integerized_attributes"
    output_path = "/home/thejus/workspace/learn-cascading/data/sessionExplodeWithAttributes-201708.MOB.smaller.int/test_enhance"


    enhance_clickstream(attributes, product_attributes_path, data_path, output_path)