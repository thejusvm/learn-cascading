import cPickle as pickle
import json
import numpy as np
import os
import tensorflow as tf
import time
from functools import partial
import glob
import sys
from click_through_dataset import ClickThroughDataSet

from mind_palace.product_ranker.models import model_factory as mf
from mind_palace.product_ranker.models.model import model
from mind_palace.product_ranker.models.softmax_model import softmax_model
from mind_palace.product_ranker.models.modelconfig import modelconfig, AttributeConfig
from mind_palace.product_ranker.integerize_clickstream import get_attributedict_path, get_attributedict
from mind_palace.product_ranker.training.trainingcontext import trainingcontext
from product_attributes_dataset import ProductAttributesDataset


def logBreak() :
    print "------------------------------------------"

def print_dict(dict_1) :
    for key in dict_1:
        print str(key) + " : " + str(dict_1.get(key))


def train(train_cxt) :
    """@type traincxt: trainingcontext"""

    modelconf = trainCxt.model_config
    logBreak()
    print "Using train context : "
    print_dict(trainCxt.__dict__)
    logBreak()
    print "Using model config : "
    print_dict(modelconf.__dict__)
    logBreak()

    ################################### Start model building

    attribute_dicts = get_attributedict(train_cxt.attributedict_path)
    for attribute_config in modelconf.attributes_config :
        attribute_name = attribute_config.name
        attribute_dict = attribute_dicts[attribute_name]
        attribute_config.vocab_size = attribute_dict.dictSize()

    mod = mf.get_model(modelconf) #type: model
    logBreak()

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
        print "creating summary writer, publishing summaries to : " + train_cxt.summary_dir
        logBreak()
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

    num_attributes = len(modelconf.attributes_config)
    attributes = map(lambda x : x.name, modelconf.attributes_config)
    attributes_dataset = ProductAttributesDataset(attributes,
                                                  batch_size=trainCxt.num_negative_samples,
                                                  repeat=True,
                                                  shuffle=True)
    attributes_dataset.initialize_iterator(sess, trainCxt.product_attributes_path)

    # HACK : Using extremely large batch_size, to reuse the same code as training. no method in Dataset to say batch all in one go
    test_dataset = ClickThroughDataSet(train_cxt, min_click_context=train_cxt.min_click_context, batch_size=train_cxt.max_test_size,
                                       shuffle=False, sess=sess, attributes_dataset=attributes_dataset)

    feed_keys = mod.place_holders()

    test_feed = None
    if summary_writer is not None :
        print "test data generation started"
        start = time.clock()
        test_dataset.initialize_iterator(sess, ctr_data_path=train_cxt.test_path)
        test_processed_data = sess.run(test_dataset.get_next)
        print "test set size : " + str(len(test_processed_data[0])) + " in " + str(time.clock() - start)
        logBreak()
        test_feed = dict(zip(feed_keys, test_processed_data))

    print "model training started"

    dataset = ClickThroughDataSet(train_cxt, min_click_context=train_cxt.min_click_context, batch_size=train_cxt.batch_size,
                                       shuffle=False, sess=sess, attributes_dataset=attributes_dataset)

    counter = 0
    for epoch in range(trainCxt.num_epochs) :
        print "epoch : " + str(epoch)
        dataset.initialize_iterator(sess,train_cxt.train_path)

        while True :
            try :
                processed_data = sess.run(dataset.get_next)
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

    trainCxt = trainingcontext()
    trainCxt.timestamp = timestamp
    trainCxt.date = currentdate
    trainCxt.data_path = "/home/thejus/workspace/learn-cascading/data/sessionExplodeWithAttributes-201708.MOB.large.search"
    trainCxt.product_attributes_path = glob.glob("/home/thejus/workspace/learn-cascading/data/product-attributes-integerized.MOB.large.search")
    trainCxt.model_dir = "saved_models/run." + currentdate
    trainCxt.summary_dir = "/tmp/sessionsimple." + currentdate
    trainCxt.test_size = 0.03
    trainCxt.num_epochs = 25
    trainCxt.num_negative_samples = 20
    trainCxt.min_click_context = 0
    trainCxt.publish_summary = True
    trainCxt.save_model = True
    trainCxt.save_model_num_iter = 1000

    dataFiles = glob.glob(trainCxt.data_path + "/part-*")
    numFiles = len(dataFiles)
    trainSize = int(numFiles * (1 - trainCxt.test_size))
    trainCxt.train_path = dataFiles[:trainSize]
    trainCxt.test_path = dataFiles[trainSize:]
    trainCxt.negative_samples_source = "random"

    trainCxt.attributedict_path = get_attributedict_path(trainCxt.data_path)

    modelconf = modelconfig("softmax_model")
    # modelconf.layer_count = [1024, 512, 256]
    modelconf.use_context = True
    modelconf.enable_default_click = False
    modelconf.reuse_context_dict = False
    # modelconf.attributes_config = [AttributeConfig("productId", 30), AttributeConfig("brand", 15), AttributeConfig("vertical", 5)]
    modelconf.attributes_config = [AttributeConfig("productId", 50)]

    trainCxt.model_config = modelconf
    train(trainCxt)

