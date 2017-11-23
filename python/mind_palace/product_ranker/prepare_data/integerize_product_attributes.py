import glob
import tensorflow as tf
import sys
import time
import os
from functools import partial

from mind_palace.commons.helpers import logBreak
import mind_palace.product_ranker.constants as CONST
from mind_palace.DictIntegerizer import DictIntegerizer
from mind_palace.product_ranker.commons import write_attribute_dicts, init_attribute_dicts
import pandas as pd


"""
    Converts a file containing product attributes and integerizes it.
    Each row of the file represents a product with all its attributes tab seperated.
    Dumps out a file containg the integer mapping for each attribute, tab seperated.
    Creates new dictIntegerizer per attribute and dumps it into a file as well
"""

def is_file_empty(file_name):
    return os.stat(file_name).st_size == 0

def integerize_product_attributes(attributes, attributes_path, output_path, attribute_dict_path) :
    attributes_paths = glob.glob(attributes_path)
    attribute_dicts = init_attribute_dicts(attributes, default_dict_keys=CONST.DEFAULT_DICT_KEYS)


    first_iter = True
    for attributes_path in attributes_paths:
        start = time.time()
        if is_file_empty(attributes_path) :
            continue
        print "Processing attribute path : " + attributes_path
        df = pd.read_csv(attributes_path, sep="\t")
        for attribute in attributes:
            df[attribute] = df[attribute].apply(lambda x:  attribute_dicts[attribute].get(x))

        if first_iter:
            first_iter = False
            file_permission = "w+"
            header = True
        else :
            file_permission = "a"
            header = False

        with open(output_path, file_permission) as f:
            df.to_csv(f, sep="\t", index=False, header=header)
        print "dumped integerized attributes in ", time.time() - start, ", output file size : ", os.stat(output_path).st_size
        logBreak()

    write_attribute_dicts(attribute_dict_path, attribute_dicts)
    return attribute_dicts

if __name__ == '__main__' :

    attribute_dict_path = "/Users/thejus/workspace/learn-cascading/data/productdict.pickle"
    attributes_path = "/Users/thejus/workspace/learn-cascading/data/product-attributes.MOB/part-*"
    output_path = "/Users/thejus/workspace/learn-cascading/data/product-attributes-integerized.MOB.large.search"
    attributes = ["productId", "brand", "ideal_for", "vertical"]

    integerize_product_attributes(attributes, attributes_path, output_path, attribute_dict_path)

