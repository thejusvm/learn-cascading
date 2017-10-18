import tensorflow as tf
import cPickle as pickle
from functools import partial
from mind_palace.DictIntegerizer import DictIntegerizer
import glob
import numpy as np
import mind_palace.product_ranker.constants as CONST


def map_to_ints(attribute_dicts, values) :
    ints = []
    for i in range(len(attribute_dicts)) :
        attribute_dict = attribute_dicts[i] #type: DictIntegerizer
        value = values[i]
        ints.append(attribute_dict.only_get(value))
    return ints


class ProductAttributesDataset:

    def __init__(self, attribute_names, batch_size = None, shuffle = False, attribute_dicts = None):
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
            # map_fn = lambda *row: tuple(tf.py_func(partial(map_to_ints, attribute_dicts), row, map_int_output_type))
            map_fn = lambda *row: tf.py_func(partial(map_to_ints, attribute_dicts), row, map_int_output_type)
            self.dataset = self.dataset.map(map_fn)

        if batch_size is not None :
            self.dataset = self.dataset.batch(batch_size)

        if shuffle:
            self.dataset = self.dataset.shuffle(buffer_size=10000)

        self.iterator = self.dataset.make_initializable_iterator()
        self.next_element = self.iterator.get_next()

def integerized_attributes(attributes, attribute_dict, attributes_path, index_field = None):
    index_field_attributes_index = attributes.index(index_field)
    attributes_path = glob.glob(attributes_path)
    features = ProductAttributesDataset(attributes, attribute_dicts=attribute_dict)
    sess = tf.Session()
    sess.run(features.iterator.initializer, feed_dict={features.filenames : attributes_path})

    index_attribute_dict = attribute_dict[index_field] #type: DictIntegerizer
    num_attributes = len(attributes)
    all_data = np.ones(shape=[index_attribute_dict.currentCount, num_attributes], dtype=int) * -1
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

    attribute_dict_path = "/home/thejus/workspace/learn-cascading/data/sessionExplodeWithAttributes-201708.MOB.smaller.processed/productdict.pickle"
    attributes_path = "/home/thejus/workspace/learn-cascading/data/product-attributes.MOB/part-*"
    attributes = ["productId", "brand", "vertical"]

    with open(attribute_dict_path, 'rb') as handle:
        attribute_dict = pickle.load(handle)

    all_data =  integerized_attributes(attributes, attribute_dict, attributes_path, index_field ="productId")
    for i in range(len(all_data)) :
        pid_data = all_data[i][0]
        if pid_data == -1 :
            print i
    print all_data[56]
    print all_data[0]
    print all_data[1]
    print all_data[2]
    print all_data[3]
