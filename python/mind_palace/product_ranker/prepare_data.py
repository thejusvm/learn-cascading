import glob
import json
import pandas as pd
import time
import numpy as np
from sklearn.model_selection import train_test_split
from mind_palace.DictIntegerizer import DictIntegerizer
import cPickle as pickle
import os
import sys
import mind_palace.product_ranker.constants as CONST


def logBreak() :
    print "------------------------------------------"

def integerize(attributes, attribute_dicts, products_attributes) :
    attributes_integerized = []
    for attribute in attributes :
        attribute_dict = attribute_dicts[attribute]
        if attribute in products_attributes :
            attribute_val = products_attributes[attribute]
        else :
            attribute_val = CONST.MISSING_DATA_TEXT
        attribute_integerized = attribute_dict.get(attribute_val)
        attributes_integerized.append(attribute_integerized)
    return attributes_integerized


def get_exploded_columns(keys, field_name):
    return map(lambda x : field_name + "_" + x, keys)

def geneate_key(key_prefix, attribute):
    return key_prefix + "_" + attribute

def add_to_row(row, attributes, attribute_vals, key_prefix):
    for i in range(len(attributes)) :
        attribute = attributes[i]
        if len(attribute_vals) != 0 :
            attribute_val = attribute_vals[i]
        else :
            attribute_val = []
        row[geneate_key(key_prefix, attribute)] = attribute_val


def cross_attribute_prefix(attributes, key_prefixes) :
    keys = []
    for attribute in attributes :
        for key_prefix in key_prefixes :
            keys.append(geneate_key(key_prefix, attribute))
    return keys

def integerize_single_val_column(df, column_name, new_column_prefix, attributes, attribute_dicts) :
    integerize_single = lambda x: integerize(attributes, attribute_dicts, json.loads(x))
    integerized_cols = df[column_name].apply(integerize_single)
    for i in range(len(attributes)) :
        attribute = attributes[i]
        df[geneate_key(new_column_prefix, attribute)] =  integerized_cols.apply(lambda x : json.dumps(x[i]))

def integerize_multi_val_column(df, column_name, new_column_prefix, attributes, attribute_dicts) :
    integerize_multiple = lambda y: np.array(map(lambda x: integerize(attributes, attribute_dicts, x), json.loads(y))).T
    integerized_cols = df[column_name].apply(integerize_multiple)
    for i in range(len(attributes)) :
        attribute = attributes[i]
        df[geneate_key(new_column_prefix, attribute)] =  integerized_cols.apply(lambda x : json.dumps(x[i].tolist() if len(x) > 0 else []))

def process_row(df, attributes, attribute_dicts):
    integerize_single_val_column(df, "positiveProducts", "positive", attributes, attribute_dicts)
    integerize_multi_val_column(df, "negativeProducts", "negative", attributes, attribute_dicts)
    integerize_multi_val_column(df, "pastClickedProducts", "clicked", attributes, attribute_dicts)
    integerize_multi_val_column(df, "pastBoughtProducts", "bought", attributes, attribute_dicts)

def process_file(data_path,
                 attributes,
                 attribute_dicts,
                 min_click_context = 0):
    df = pd.read_csv(data_path, sep="\t")
    start = time.clock()
    process_row(df, attributes, attribute_dicts)
    necessaryKeys = cross_attribute_prefix(attributes, CONST.OUTPUTS_PER_ATTRIBUTE)
    data = df[necessaryKeys]
    print "time taken by data preprocess : " + str(time.clock() - start)
    return data

def get_attributedict_path(data_path):
    return data_path + "/productdict.pickle"

def get_train_path(data_path):
    return data_path + "/train.tsv"

def get_test_path(data_path):
    return data_path + "/test.tsv"

def get_attributedict(data_path) :
    with open(data_path, 'rb') as handle:
        return pickle.load(handle)

def new_dictintegerizer(attribute, deafult_dicy_keys):
    dict_i = DictIntegerizer(default=deafult_dicy_keys, name=attribute)
    return dict_i

def prepare_data(raw_data_path,
                 processed_data_path,
                 attributes,
                 default_dict_keys = None):
    attribute_dicts = {}
    for attribute in attributes :
        attributedict = new_dictintegerizer(attribute, default_dict_keys)
        attribute_dicts[attribute] = attributedict

    filenames = glob.glob(raw_data_path)
    start = time.clock()
    counter = 0
    for in_file in filenames:
        logBreak()
        print "start file processing : " + in_file + ", with dict size : " + str(map(str, attribute_dicts.values()))
        pd = process_file(in_file, attributes, attribute_dicts)
        print "end file processing : " + in_file + ", in " + str(time.clock() - start)
        out_file = processed_data_path + "/part-" + str(counter)
        print out_file
        start = time.clock()
        pd.to_csv(out_file, sep ="\t", index=False)
        print "dumped content of " + in_file + " to " + out_file + " in " + str(time.clock() - start)
        counter += 1
        logBreak()

    return attribute_dicts


if __name__ == '__main__' :
    raw_data_path = "/home/thejus/workspace/learn-cascading/data/sessionExplodeWithAttributes-201708.MOB.large" + "/part-*"
    processed_data_path = "/home/thejus/workspace/learn-cascading/data/sessionExplodeWithAttributes-201708.MOB.large.processed"
    os.makedirs(processed_data_path)

    attributes = ["productId", "brand", "vertical"]

    dicts = prepare_data(raw_data_path, processed_data_path, attributes, default_dict_keys=CONST.DEFAULT_DICT_KEYS)
    product_dict_file = get_attributedict_path(processed_data_path)

    start = time.clock()
    with open(product_dict_file, 'w+b') as handle:
        pickle.dump(dicts, handle, protocol=pickle.HIGHEST_PROTOCOL)

    print "pickled attribute dicts into " + product_dict_file + " in " + str(time.clock() - start)
    logBreak()