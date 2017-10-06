import cPickle as pickle
import glob
import json
import numpy as np
import os
import pandas as pd
import tensorflow as tf
import time
from sklearn.model_selection import train_test_split
import trainingcontext as tc
from trainingcontext import trainingcontext
from prepare_data import get_train_path, get_test_path, get_productdict_path


def int_json(s) :
    loads = json.loads(s)
    return map(lambda x : int(x), loads)

def _parse_line(line) :
     positive, negatives, context_clicks =  line.split('\t')
     negatives = int_json(negatives)
     clicks = int_json(context_clicks)

     if not clicks :
         clicks = [1]
     if not negatives :
         negatives = [1]

     return [int(positive), negatives, clicks]

def prepare_data(train_cxt) :
    """@type train_cxt: trainingcontext"""

    # filenames = tf.placeholder(tf.string, shape=[None])
    filenames=[train_cxt.test_path]
    filename_queue = tf.train.string_input_producer(filenames)
    reader = tf.TextLineReader()
    key, value = reader.read(filename_queue)
    col1, col2, col3= tf.decode_csv(value, [[0], [0], [0]],field_delim='\t')

    # dataset = tf.contrib.data.Dataset.from_tensor_slices(filenames)
    # dataset = dataset.flat_map(
    #     lambda filename: (
    #         tf.contrib.data.TextLineDataset(filename)
    #             .skip(1)))
    # dataset = dataset.map(lambda line : tuple(tf.py_func(_parse_line, [line], [tf.int64, tf.int64, tf.int64])))
    # dataset = dataset.shuffle(buffer_size=10000)
    # dataset = dataset.batch(trainCxt.batch_size)
    # dataset = dataset.repeat()

    with tf.Session() as sess:
        coord = tf.train.Coordinator()
        threads = tf.train.start_queue_runners(coord=coord)
        # iterator = dataset.make_initializable_iterator()
        # sess.run(iterator.initializer, feed_dict={filenames: [train_cxt.train_path]})

        # next_element = iterator.get_next()

        # sess.run(iterator.initializer)
        for i in range(2):
            try :
                # feed= {filenames: [train_cxt.test_path]}
                feed= {}
                v1 = sess.run(col1, feed_dict = feed)
                print v1
                print "-----"
            except tf.errors.OutOfRangeError:
                break

        coord.request_stop()
        coord.join(threads)




if __name__ == '__main__' :
    timestamp = time.localtime()
    currentdate = time.strftime('%Y%m%d-%H-%M-%S', timestamp)

    trainCxt = tc.trainingcontext()
    trainCxt.data_path = "/home/thejus/workspace/learn-cascading/data/sessionExplode-201708.MOB.processed.small"
    trainCxt.model_dir = "saved_models/run." + currentdate
    trainCxt.summary_dir = "/tmp/sessionsimple." + currentdate
    trainCxt.num_epochs = 10
    trainCxt.min_click_context = 2
    trainCxt.save_model = True
    trainCxt.save_model_on_epoch = False
    trainCxt.date = currentdate
    trainCxt.timestamp = timestamp
    trainCxt.publish_summary = True
    trainCxt.num_negative_samples = 20
    trainCxt.batch_size = 1

    trainCxt.train_path = get_train_path(trainCxt.data_path)
    trainCxt.test_path = get_test_path(trainCxt.data_path)
    trainCxt.productdict_path = get_productdict_path(trainCxt.data_path)

    # modelconf = modelconfig("softmax_model" , None, 50)
    # # modelconf.layer_count = [1024, 512, 256]
    # modelconf.use_context = True
    # modelconfig.reuse_context_dict = True
    # trainCxt.model_config = modelconf
    prepare_data(trainCxt)

