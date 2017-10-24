import tensorflow as tf
import time
import json
import numpy as np
import mind_palace.product_ranker.constants as CONST
from functools import partial

def int_json(s) :
    loads = json.loads(s)
    return map(lambda x : int(x), loads)

def handle_padding(data, underlying_array) :
    datalen = len(data)
    num = min(datalen, len(underlying_array))
    underlying_array[:num] = data[:num]
    return underlying_array

def handle_negatives(negatives, negative_random):
    merged = handle_padding(negatives, negative_random)
    return merged


def handle_clicks(clicks, num_click_context, pad_int, default_click_index=-1):
    clicks_padded = np.ones(num_click_context, dtype = np.int) * pad_int
    if not clicks:
        if default_click_index is not -1 :
            clicks = [default_click_index]
    merged = handle_padding(clicks, clicks_padded)
    return merged

def filter_min_context_click(min_click_count, line) :
    line_split =  line.split('\t')
    click_data = json.loads(line_split[2])
    allow = len(click_data) >= min_click_count
    return allow

def _parse_line(trainCxt, attributes_config, product_to_attributes, line) :
    line_split =  line.split('\t')
    return_features = []
    num_attributes = len(attributes_config)

    # start = time.clock()
    if trainCxt.negative_samples_source == 'random' :
        negative_samples = np.ndarray([num_attributes, trainCxt.num_negative_samples], dtype=int)
        for i in range(num_attributes):
            negative_samples[i] = np.random.randint(attributes_config[i].vocab_size, size = (trainCxt.num_negative_samples))
        negative_samples = negative_samples.T
    else :
        random_ints = np.random.randint(len(product_to_attributes), size=trainCxt.num_negative_samples)
        negative_samples = product_to_attributes[random_ints, :]
    # print "fetch one random sample batch : " + str(time.clock() - start)

    for counter in range(num_attributes) :
        num_fields_per_attribute = len(CONST.OUTPUTS_PER_ATTRIBUTE)
        positive = int(line_split[num_fields_per_attribute * counter + 0])
        negatives = int_json(line_split[num_fields_per_attribute * counter + 1])
        clicks = int_json(line_split[num_fields_per_attribute * counter + 2])
        negatives = handle_negatives(negatives, negative_samples[:, counter])
        pad_index = CONST.DEFAULT_DICT_KEYS.index(CONST.PAD_TEXT)
        if trainCxt.model_config.enable_default_click :
            default_click_index = CONST.DEFAULT_DICT_KEYS.index(CONST.DEFAULT_CLICK_TEXT)
        else:
            default_click_index = -1
        clicks = handle_clicks(clicks, trainCxt.num_click_context, pad_index, default_click_index)
        return_features += [positive,  negatives, clicks]
    return return_features

class ClickstreamDataset :

    def __init__(self,
                 train_cxt,
                 min_click_context = 0,
                 batch_size = None,
                 shuffle = True,
                 product_to_attributes = None):
        modelconf = train_cxt.model_config
        self.filenames = tf.placeholder(tf.string, shape=[None])
        self.dataset = tf.contrib.data.Dataset.from_tensor_slices(self.filenames)
        self.dataset = self.dataset.flat_map(
            lambda filename: (
                tf.contrib.data.TextLineDataset(filename)
                    .skip(1)))

        num_attributes = len(modelconf.attributes_config)
        self.output_type = [tf.int64 for _ in range(num_attributes * 3)]
        self.dataset = self.dataset.filter(lambda line :
                                 tf.py_func(partial(filter_min_context_click, min_click_context), [line], [tf.bool]))
        self.dataset = self.dataset.map(lambda line : tuple(tf.py_func(partial(_parse_line, train_cxt, modelconf.attributes_config, product_to_attributes), [line],
                                                             self.output_type)))

        if shuffle :
            self.dataset = self.dataset.shuffle(buffer_size=10000)
        if batch_size != None :
            self.dataset = self.dataset.batch(batch_size)

        self.iterator = self.dataset.make_initializable_iterator()
        self.get_next = self.iterator.get_next()

    def initialize_iterator(self, sess, ctr_data_path):
        sess.run(self.iterator.initializer, feed_dict={self.filenames : ctr_data_path})