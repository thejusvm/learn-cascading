import tensorflow as tf
import json
from mind_palace.DictIntegerizer import DictIntegerizer
import glob
import numpy as np
import mind_palace.product_ranker.constants as CONST

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

def integerized_attributes(attributes, attributes_path, num_rows, index_field):
    index_field_attributes_index = attributes.index(index_field)
    features = ProductAttributesDataset(attributes)
    sess = tf.Session()
    sess.run(features.iterator.initializer, feed_dict={features.filenames : attributes_path})
    num_attributes = len(attributes)
    all_data = np.ones(shape=[num_rows, num_attributes], dtype=int) * -1
    for i in range(len(CONST.DEFAULT_DICT_KEYS)) :
        all_data[i] = np.ones(num_attributes, dtype=int) * i
    while True :
        try :
            data = sess.run(features.next_element)
            index_field_int = data[index_field_attributes_index]
            if index_field_int != -1 :
                all_data[index_field_int] = data
        except tf.errors.OutOfRangeError:
            break
    return all_data

if __name__ == '__main__' :

    attributes_path = glob.glob("/home/thejus/workspace/learn-cascading/data/product-attributes-integerized.MOB.large.search")
    attributes = ["productId", "brand", "vertical"]

    all_data =  integerized_attributes(attributes, attributes_path, 7756, "productId")
    for i in range(len(all_data)) :
        pid_data = all_data[i][0]
        if pid_data == -1 :
            print i
    print all_data[56]
    print all_data[99]
    print all_data[108]
    print all_data[1000]
    print all_data[0]
    print all_data[1]
    print all_data[2]
    print all_data[3]
