import cPickle as pickle
import glob
import os
import tensorflow as tf
import time
import sys

import argparse

from mind_palace.product_ranker.models import model_factory as mf
from mind_palace.product_ranker.models.model import model
from mind_palace.product_ranker.models.modelconfig import modelconfig, AttributeConfig
from mind_palace.product_ranker.prepare_data.clickstream_iterator import ClickstreamDataset
from mind_palace.product_ranker.prepare_data.integerize_clickstream import get_attributedict_path, get_attributedict
from mind_palace.product_ranker.training.trainingcontext import trainingcontext, getTraningContextDir
from mind_palace.product_ranker.prepare_data.dataprep_flow import get_attributedicts_path, get_trainingdata_path, get_integerized_attributes_path

"""
    generates training data with
        * random negative samples padded
        * click context padded with appropriate CONST.PAD_TEXT index
    Operates on top of output of integerize_clickstream
"""

def logBreak() :
    print "------------------------------------------"

def print_dict(dict_1) :
    for key in dict_1:
        print str(key) + " : " + str(dict_1.get(key))

def save_traincxt(trainCxt) :
    train_context_model_dir = trainCxt.getTrainCxtDir()
    with open(train_context_model_dir, 'w+b') as handle:
        pickle.dump(trainCxt, handle, protocol=pickle.HIGHEST_PROTOCOL)

def train(train_cxt) :
    """@type train_cxt: trainingcontext"""

    modelconf = trainCxt.model_config
    logBreak()
    print "Using train context : "
    print_dict(trainCxt.__dict__)
    logBreak()
    print "Using model config : "
    print_dict(modelconf.__dict__)
    logBreak()


    ################################### Prepareing datasets
    attributes = map(lambda x : x.name, modelconf.attributes_config)
    dataset = ClickstreamDataset(attributes, batch_size=train_cxt.batch_size, shuffle=True)
    test_dataset = ClickstreamDataset(attributes, batch_size=train_cxt.batch_size, shuffle=False)
    ################################### Start model building

    attribute_dicts = get_attributedict(train_cxt.attributedict_path)
    for attribute_config in modelconf.attributes_config :
        attribute_name = attribute_config.name
        attribute_dict = attribute_dicts[attribute_name]
        attribute_config.vocab_size = attribute_dict.dictSize()

    mod = mf.get_model(modelconf) #type: model
    logBreak()

    mod.feed_input(dataset.feature_names, dataset.get_next)
    loss = mod.loss()
    minimize_step = mod.minimize_step()
    loss_summary = tf.summary.scalar("loss", loss)

    mod.feed_input(test_dataset.feature_names, test_dataset.get_next)
    test_metric_nodes = mod.test_summaries()

    sess = tf.Session()
    sess.run(tf.global_variables_initializer())

    summary_writer = None
    if trainCxt.publish_summary :
        print "creating summary writer, publishing summaries to : " + train_cxt.summary_dir
        logBreak()
        summary_writer = tf.summary.FileWriter(trainCxt.summary_dir, sess.graph)

    ################################### End model building

    ################################### Saving dict to file

    if trainCxt.save_model :
        if not os.path.exists(trainCxt.model_dir):
            os.makedirs(trainCxt.model_dir)
        nn_model_dir = trainCxt.getNnDir()
        save_traincxt(trainCxt)

        print "saved trainCxt into " + trainCxt.model_dir
        logBreak()
    ################################### End saving dict to file

    ################################### Start model training

    saver = tf.train.Saver()
    if os.path.exists(trainCxt.model_dir):
        nn_dir = tf.train.latest_checkpoint(trainCxt.model_dir)
        if nn_dir is not None :
            saver.restore(sess, nn_dir)
            print "restoring model from : " + nn_dir
        else:
            print "no model to restore from : " + trainCxt.model_dir
    logBreak()

    if trainCxt.restore_model_path and os.path.isdir(trainCxt.restore_model_path) :
        restore_nn_dir = tf.train.latest_checkpoint(trainCxt.restore_model_path)
        print "restoring tf model from : " + restore_nn_dir
        saver.restore(sess, restore_nn_dir)
        logBreak()

    num_batches_per_latency_track = int(trainCxt.latency_track_iters / trainCxt.batch_size)
    print "model training started"

    elapsed_time = 0
    for epoch in range(trainCxt.num_epochs) :
        print "epoch : " + str(epoch)
        dataset.initialize_iterator(sess, train_cxt.train_path)
        epoch_start = time.time()
        while True :
            try :
                start = time.time()
                _, loss_val, summary = sess.run([minimize_step, loss, loss_summary])
                elapsed_time += time.time() - start
                # print str(trainCxt.train_counter) + " processing one batch took : " + str(elapsed_time)
                if summary_writer is not None :
                    summary_writer.add_summary(summary, trainCxt.train_counter)
                    if trainCxt.train_counter % num_batches_per_latency_track == 0 :
                        latency_summary = tf.Summary(value=[tf.Summary.Value(tag="per_"+str(trainCxt.latency_track_iters)+"_latency", simple_value=elapsed_time)])
                        summary_writer.add_summary(latency_summary, trainCxt.train_counter * trainCxt.batch_size)
                        elapsed_time = 0

                if summary_writer is not None and trainCxt.train_counter % trainCxt.test_summary_publish_iters == 0 :
                    test_dataset.initialize_iterator(sess, train_cxt.test_path)
                    start = time.time()
                    test_metric_names = ["test_" + x[0] + "_1" for x in test_metric_nodes]
                    test_metrics_ops = [x[1] for x in test_metric_nodes]
                    test_batch_counter = 0
                    test_metric_aggregated = [0 for _ in range(len(test_metric_names))]
                    while True:
                        try :
                            test_metrics_batch = sess.run(test_metrics_ops)
                            test_metric_aggregated = map(lambda x, y : x + y, test_metric_aggregated, test_metrics_batch)
                            test_batch_counter += 1
                        except tf.errors.OutOfRangeError:
                            break
                    test_metric_mean = [x/test_batch_counter for x in test_metric_aggregated]
                    test_summary_values = map(lambda x, y : tf.Summary.Value(tag=x, simple_value=y), test_metric_names, test_metric_mean)
                    test_metric_summary = tf.Summary(value=test_summary_values)
                    summary_writer.add_summary(test_metric_summary, trainCxt.train_counter)
                    num_test_records = test_batch_counter * trainCxt.batch_size
                    print str(trainCxt.train_counter) + " processing test batch of size " + str(num_test_records) + " records took  : " + str(time.time() - start)

                if trainCxt.save_model and trainCxt.save_model_num_iter != None and trainCxt.train_counter % trainCxt.save_model_num_iter == 0:
                    saver.save(sess, nn_model_dir + ".counter" , global_step = trainCxt.train_counter)
                    save_traincxt(trainCxt)
                    print "saved nn model on counter " + str(trainCxt.train_counter) + " into : " + nn_model_dir

                trainCxt.train_counter = trainCxt.train_counter + 1
            except tf.errors.OutOfRangeError:
                break
        print "processing epoch took : " + str(time.time() - epoch_start)

        ################################### Saving model to file
        if trainCxt.save_model_on_epoch and trainCxt.save_model :
            saver.save(sess, nn_model_dir + ".epoch", global_step = epoch)
            save_traincxt(trainCxt)
            print "saved nn on epoch " + str(epoch) + "model into : " + nn_model_dir
            logBreak()
        ################################### End model to file

    if trainCxt.save_model :
        nn_model_dir = trainCxt.model_dir + '/nn'
        saver.save(sess, nn_model_dir)
        save_traincxt(trainCxt)
        print "saved nn model into : " + nn_model_dir
        logBreak()

    print "model training ended"
    logBreak()

    ################################### End model training

if __name__ == '__main__' :

    trainCxt = trainingcontext()
    parser = argparse.ArgumentParser()
    train_cxt_dict = trainCxt.__dict__
    for train_key in train_cxt_dict :
        parser.add_argument("--" + train_key, type=type(train_cxt_dict[train_key]))
    args = parser.parse_args()

    for arg in args.__dict__:
        arg_val = args.__dict__[arg]
        if arg_val != None:
            trainCxt.__dict__[arg] = arg_val
            print arg, arg_val

    if not (trainCxt.input_path or trainCxt.restore_model_path) :
        print "provide one of the two options --input_path | --restore_model_path"
        sys.exit(0)

    if trainCxt.restore_model_path :
        dir = getTraningContextDir(trainCxt.restore_model_path)
        print "loading training context : " + dir
        with open(dir, 'rb') as handle:
            trainCxt = pickle.load(handle)
    else :
        trainCxt.data_path = get_trainingdata_path(trainCxt.input_path)
        trainCxt.attributedict_path = get_attributedicts_path(trainCxt.input_path)
        trainCxt.product_attributes_path = get_integerized_attributes_path(trainCxt.input_path)
        trainCxt.model_dir = "saved_models/run." + trainCxt.date
        trainCxt.summary_dir = "summary/sessionsimple." + trainCxt.date

        dataFiles = sorted(glob.glob(trainCxt.data_path + "/part-*"))
        numFiles = len(dataFiles)
        trainSize = int(numFiles * (1 - trainCxt.test_size))
        trainCxt.train_path = dataFiles[:trainSize]
        trainCxt.test_path = dataFiles[trainSize:]

        modelconf = modelconfig("softmax_model")
        modelconf.use_context = True
        modelconf.enable_default_click = False
        modelconf.reuse_context_dict = False
        # modelconf.attributes_config = [AttributeConfig("productId", 30), AttributeConfig("brand", 15), AttributeConfig("vertical", 5)]
        modelconf.attributes_config = [AttributeConfig("productId", 30), AttributeConfig("brand", 15)]
        # modelconf.attributes_config = [AttributeConfig("productId", 50)]
        trainCxt.model_config = modelconf

    train(trainCxt)

