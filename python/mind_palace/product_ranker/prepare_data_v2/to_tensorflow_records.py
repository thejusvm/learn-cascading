import time
import json
import numpy as np
import glob
import os
import tensorflow as tf
import pandas as pd
import sys

from contextlib import closing
from functools import partial
from multiprocessing import Pool

import mind_palace.product_ranker.constants as CONST
from mind_palace.commons.helpers import logBreak
from mind_palace.product_ranker.commons import generate_feature_names

def int_json(s):
    loads = json.loads(s)
    return np.array(loads, dtype=int)

def add_to_record(record, feature_name, feature_value) :
    record.features.feature[feature_name].int64_list.value.extend(feature_value)

def to_tfr_example(row, features_names) :
    record = tf.train.Example()
    num_features = len(features_names)
    [add_to_record(record, features_names[i], row[i]) for i in range(num_features)]
    return record.SerializeToString()

def write_df_toFile(df, output_file):
    writer = tf.python_io.TFRecordWriter(output_file)
    print "writing to file : " + output_file
    start = time.time()
    for row in df:
        writer.write(row)
    print "wrote file in " + str(time.time() - start)

def get_processed_data_frame(input_file, attributes) :
    df = pd.read_csv(input_file, sep ="\t")
    feature_names = generate_feature_names(attributes, CONST.TRAINING_COL_PREFIXES)
    for feature_name in feature_names:
        df[feature_name] = df[feature_name].apply(int_json)
    df["tfr"] = df[feature_names].apply(lambda x: to_tfr_example(x, feature_names), axis=1)
    return df["tfr"]

def write_df_toFile(df, output_file):
    writer = tf.python_io.TFRecordWriter(output_file)
    print "writing to file : " + output_file
    start = time.time()
    for row in df:
        writer.write(row)
    print "wrote file in " + str(time.time() - start)

def enhance_file(attributes, io):
    input_file, output_file = io
    print "processing file : " + input_file
    start = time.time()
    df = get_processed_data_frame(input_file, attributes)
    print "file to processed dataframe in : " + str(time.time() - start)
    write_df_toFile(df, output_file)
    logBreak()

def enhance_clickstream(attributes, ctr_data_path, output_path, num_parallel=-1) :

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    input_output = [[ctr_data_path[i], output_path + "/part-" + str(i)] for i in range(len(ctr_data_path))]

    with closing(Pool(processes=num_parallel)) as pool:
        pool.map(partial(enhance_file, attributes), input_output)

def process_dir(attributes, ctr_data_path, output_path, cxt) :
    input_path = glob.glob(ctr_data_path + "/" + cxt + "/part-*")
    output_path = output_path + "/" + cxt
    enhance_clickstream(attributes, input_path, output_path)


if __name__ == '__main__' :

    attributes = ["productId", "brand", "ideal_for", "type", "color", "pattern", "occasion", "fit", "fabric", "vertical"]
    data_path = sys.argv[1] #"/Users/thejus/workspace/learn-cascading/data/sessionexplode-2017-0801.1000.final"
    output_path = sys.argv[2] #"/Users/thejus/workspace/learn-cascading/data/sessionexplode-2017-0801.1000.tfr"

    process_dir(attributes, data_path, output_path, "train")
    process_dir(attributes, data_path, output_path, "test")
