import cPickle as pickle
import json
import numpy as np
import os
import tensorflow as tf
import time
from functools import partial
import glob

import  trainingcontext as tc
from mind_palace.product_ranker.models import model_factory as mf
from mind_palace.product_ranker.models.model import model
from mind_palace.product_ranker.models.modelconfig import modelconfig
from mind_palace.product_ranker.prepare_data import get_train_path, get_test_path, get_productdict_path, get_productdict
from trainingcontext import trainingcontext



def int_json(s) :
    loads = json.loads(s)
    return map(lambda x : int(x), loads)

def _handle_shit(x, y, z) :
    return x, y, z


def handle_padding(data, underlying_array) :
    datalen = len(data)
    num = min(datalen, len(underlying_array))
    underlying_array[:num] = data[:num]
    return underlying_array

def handle_negatives(negatives, num_negatives, vocab_size):
    negatives_padded = np.random.randint(vocab_size, size = (num_negatives))
    merged = handle_padding(negatives, negatives_padded)
    return merged


def hanle_clicks(clicks, num_click_context, pad_int):
    clicks_padded = np.ones(num_click_context, dtype = np.int) * pad_int
    merged = handle_padding(clicks, clicks_padded)
    return merged


def _parse_line(train_cxt, line) :
     positive, negatives, context_clicks =  line.split('\t')
     negatives = int_json(negatives)
     clicks = int_json(context_clicks)
     negatives = handle_negatives(negatives, trainCxt.num_negative_samples, train_cxt.model_config.vocabulary_size)
     clicks = hanle_clicks(clicks, trainCxt.num_click_context, trainCxt.model_config.pad_index)
     return [int(positive), negatives, clicks]

def logBreak() :
    print "------------------------------------------"

def print_dict(dict_1) :
    for key in dict_1:
        print str(key) + " : " + str(dict_1.get(key))


def train(train_cxt) :
    """@type traincxt: trainingcontext"""

    logBreak()
    print "Using train context : "
    print_dict(trainCxt.__dict__)
    logBreak()
    print "Using model config : "
    print_dict(modelconf.__dict__)
    logBreak()

    ################################### Start model building

    productdict = get_productdict(train_cxt.productdict_path)
    modelconfig.pad_index = productdict.get(trainCxt.pad_text)
    if trainCxt.default_click_text is not None :
        modelconfig.default_click_index = productdict.get(trainCxt.default_click_text)

    modelconf.vocabulary_size = productdict.dictSize()
    mod = mf.get_model(modelconf) #type: model

    sess = tf.Session()
    sess.run(tf.global_variables_initializer())

    loss_summary = tf.summary.scalar("loss", mod.loss())
    test_loss_summary = tf.summary.scalar("test_loss", mod.loss())

    summaries = mod.test_summaries()
    test_summaries = [test_loss_summary]
    for summary in summaries :
        test_summary = tf.summary.scalar("test_" + summary[0], summary[1])
        test_summaries.append(test_summary)
    merged_summary = tf.summary.merge(test_summaries)

    summary_writer = None
    if trainCxt.publish_summary :
        summary_writer = tf.summary.FileWriter(trainCxt.summary_dir, sess.graph)

    ################################### End model building

    ################################### Saving dict to file

    if trainCxt.save_model :
        os.makedirs(trainCxt.model_dir)
        nn_model_dir = trainCxt.getNnDir()
        train_context_model_dir = trainCxt.getTrainCxtDir()

        with open(train_context_model_dir, 'w+b') as handle:
            pickle.dump(trainCxt, handle, protocol=pickle.HIGHEST_PROTOCOL)

        print "saved trainCxt into " + trainCxt.model_dir
        logBreak()
    ################################### End saving dict to file

    ################################### Start model training

    saver = tf.train.Saver()

    filenames = tf.placeholder(tf.string, shape=[None])
    dataset = tf.contrib.data.Dataset.from_tensor_slices(filenames)
    dataset = dataset.flat_map(
        lambda filename: (
            tf.contrib.data.TextLineDataset(filename)
                .skip(1)))
    dataset = dataset.map(lambda line : tuple(tf.py_func(partial(_parse_line, train_cxt), [line], [tf.int64, tf.int64, tf.int64])))
    test_dataset = dataset
    dataset = dataset.shuffle(buffer_size=10000)
    dataset = dataset.batch(trainCxt.batch_size)

    feed_keys = mod.place_holders()

    test_feed = None
    if summary_writer is not None :
        test_dataset = test_dataset.batch(train_cxt.max_test_size) # hack to reuse the same code as training. no method in Dataset to say batch all in one go
        test_iterator = test_dataset.make_initializable_iterator()
        sess.run(test_iterator.initializer, feed_dict={filenames: train_cxt.test_path})
        test_next_element = test_iterator.get_next()
        test_processed_data = sess.run(test_next_element)
        print "test set size : " + str(len(test_processed_data[0]))
        logBreak()
        test_feed = dict(zip(feed_keys, test_processed_data))


    print "model training started"


    counter = 0
    for epoch in range(trainCxt.num_epochs) :
        print "epoch : " + str(epoch)
        iterator = dataset.make_initializable_iterator()
        sess.run(iterator.initializer, feed_dict={filenames: train_cxt.train_path})

        next_element = iterator.get_next()
        while True :
            try :
                processed_data = sess.run(next_element)
                feed = dict(zip(feed_keys, processed_data))
                _, loss_val, summary = sess.run([mod.minimize_step(), mod.loss(), loss_summary], feed_dict=feed)
                if summary_writer is not None :
                    summary_writer.add_summary(summary, counter)

                if summary_writer is not None and test_feed is not None and counter % trainCxt.test_summary_publish_iters == 0 :
                    all_summary = sess.run(merged_summary, feed_dict = test_feed)
                    summary_writer.add_summary(all_summary, counter)

                if trainCxt.save_model and trainCxt.save_model_num_iter != None and counter % trainCxt.save_model_num_iter == 0:
                    saver.save(sess, nn_model_dir + ".counter" , global_step = counter)
                    print "saved nn model on counter " + str(counter) + " into : " + nn_model_dir

                counter = counter + 1
            except tf.errors.OutOfRangeError:
                break

        ################################### Saving model to file
        if trainCxt.save_model_on_epoch and trainCxt.save_model :
            saver.save(sess, nn_model_dir + ".epoch", global_step = epoch)
            print "saved nn on epoch " + str(epoch) + "model into : " + nn_model_dir
            logBreak()
            ################################### End model to file

    if trainCxt.save_model :
        nn_model_dir = trainCxt.model_dir + '/nn'
        saver.save(sess, nn_model_dir)
        print "saved nn model into : " + nn_model_dir
        logBreak()

    print "model training ended"
    logBreak()

    ################################### End model training

if __name__ == '__main__' :
    timestamp = time.localtime()
    currentdate = time.strftime('%Y%m%d-%H-%M-%S', timestamp)

    trainCxt = tc.trainingcontext()
    trainCxt.data_path = "/home/thejus/workspace/learn-cascading/data/sessionExplode-201708.MOB.processed"
    trainCxt.model_dir = "saved_models/run." + currentdate
    trainCxt.summary_dir = "/tmp/sessionsimple." + currentdate
    trainCxt.num_epochs = 25
    trainCxt.min_click_context = 0
    trainCxt.save_model = True
    trainCxt.save_model_on_epoch = False
    trainCxt.date = currentdate
    trainCxt.timestamp = timestamp
    trainCxt.publish_summary = True
    trainCxt.num_negative_samples = 20
    trainCxt.test_size = 0.1

    dataFiles = glob.glob(trainCxt.data_path + "/part-*")
    numFiles = len(dataFiles)
    trainSize = int(numFiles * (1 - trainCxt.test_size))
    trainCxt.train_path = dataFiles[:trainSize]
    trainCxt.test_path = dataFiles[trainSize:]

    trainCxt.productdict_path = get_productdict_path(trainCxt.data_path)

    modelconf = modelconfig("softmax_model" , 1000, 50)
    # modelconf.layer_count = [1024, 512, 256]
    modelconf.use_context = True
    modelconfig.reuse_context_dict = True
    trainCxt.model_config = modelconf
    train(trainCxt)

