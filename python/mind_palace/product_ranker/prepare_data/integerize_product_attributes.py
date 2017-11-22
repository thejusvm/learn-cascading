import glob
import tensorflow as tf
from functools import partial

import mind_palace.product_ranker.constants as CONST
from mind_palace.DictIntegerizer import DictIntegerizer
from mind_palace.product_ranker.commons import write_attribute_dicts, init_attribute_dicts

"""
    Converts a file containing product attributes and integerizes it.
    Each row of the file represents a product with all its attributes tab seperated.
    Dumps out a file containg the integer mapping for each attribute, tab seperated.
    Uses a pickled DictIntegerizer for source of integer mappings

    TODO : should be able to create its own instance of dictIntegerizer
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



class PrepareAttributesDataset:

    def __init__(self, attribute_names, batch_size = None, attribute_dicts = None, filter_unavailable = False):
        self.filenames = tf.placeholder(tf.string, shape=[None])
        self.dataset = tf.contrib.data.Dataset.from_tensor_slices(self.filenames)
        self.dataset = self.dataset.flat_map(
            lambda filename: (
                tf.contrib.data.TextLineDataset(filename)
                    .skip(1)))
        num_attributes = len(attribute_names)
        split_output_type = [tf.string for _ in range(num_attributes)]
        self.dataset = self.dataset.map(lambda line : tf.py_func(lambda x : x.split('\t')[:num_attributes], [line], split_output_type))

        if attribute_dicts is not None :
            attribute_dicts = map(lambda x : attribute_dicts[x], attribute_names)
            map_int_output_type = [tf.int64 for _ in range(num_attributes)]
            map_fn = lambda *row: tf.py_func(partial(map_to_ints, attribute_dicts), row, map_int_output_type)
            self.dataset = self.dataset.map(map_fn)
            if filter_unavailable :
                filter_fn = lambda *row: tf.py_func(not_unavailable_product, row, [tf.bool])
                self.dataset = self.dataset.filter(filter_fn)

        if batch_size is not None :
            self.dataset = self.dataset.batch(batch_size)

        self.iterator = self.dataset.make_initializable_iterator()
        self.next_element = self.iterator.get_next()

    def initialize_iterator(self, sess, attributes_path):
        sess.run(self.iterator.initializer, feed_dict={self.filenames : attributes_path})

def integerize_product_attributes(attributes, attributes_path, output_path, attribute_dict_path) :
    attributes_path = glob.glob(attributes_path)
    attribute_dicts = init_attribute_dicts(attributes, default_dict_keys=CONST.DEFAULT_DICT_KEYS)
    features = PrepareAttributesDataset(attributes, attribute_dicts=attribute_dicts, filter_unavailable=True)

    sess = tf.Session()
    sess.run(features.iterator.initializer, feed_dict={features.filenames : attributes_path})

    with open(output_path, mode="w+b") as writer :
        flush_counter = 0
        writer.write("\t".join(attributes) + '\n')
        while True :
            try :
                data = sess.run(features.next_element)
                writer.write("\t".join(map(str, data.tolist())) + '\n')
                flush_counter += 1
                if flush_counter >= 100000:
                    writer.flush()
                    flush_counter=0
            except tf.errors.OutOfRangeError:
                break
        writer.flush()
        writer.close()

    write_attribute_dicts(attribute_dict_path, attribute_dicts)
    return attribute_dicts

if __name__ == '__main__' :

    attribute_dict_path = "/Users/thejus/workspace/learn-cascading/data/productdict.pickle"
    attributes_path = "/Users/thejus/workspace/learn-cascading/data/product-attributes.MOB/part-*"
    output_path = "/Users/thejus/workspace/learn-cascading/data/product-attributes-integerized.MOB.large.search"
    attributes = ["productId", "brand", "ideal_for", "vertical"]

    integerize_product_attributes(attributes, attributes_path, output_path, attribute_dict_path)

