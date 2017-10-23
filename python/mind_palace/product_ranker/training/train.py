import cPickle as pickle
import numpy as np
import os
import tensorflow as tf
import time

import  trainingcontext as tc
from mind_palace.product_ranker.models import model_factory as mf
from mind_palace.product_ranker.models.model import model
from mind_palace.product_ranker.models.modelconfig import modelconfig
from mind_palace.product_ranker.integerize_clickstream import preparedata
from trainingcontext import trainingcontext


def process_negative_samples(var_len_negative_samples, vocabulary_size, max_num_negative_samples) :
    batch_size = np.shape(var_len_negative_samples)[0]
    negative_samples_data = np.random.randint(vocabulary_size, size = (batch_size, max_num_negative_samples))
    for i in range(batch_size) :
        numsamples = len(var_len_negative_samples[i])
        iter_len = min(numsamples, max_num_negative_samples)
        for j in range(iter_len) :
            negative_samples_data[i][j] = var_len_negative_samples[i][j]
    return negative_samples_data

def process_past_click(var_len_past_click, max_num_click_context, pad_int) :
    batch_size = np.shape(var_len_past_click)[0]
    negative_samples_data = np.ones([batch_size, max_num_click_context]) * pad_int
    for i in range(batch_size) :
        numclicks = len(var_len_past_click[i])
        iter_len = min(numclicks, max_num_click_context)
        for j in range(iter_len) :
            index = numclicks - 1 - j
            negative_samples_data[i][j] = var_len_past_click[i][index]
    return negative_samples_data

def splitIO(batch, trainCxt) :

    positive_samples = batch[:, 0]

    var_len_negative_samples = batch[:, 1]
    negative_samples = process_negative_samples(var_len_negative_samples, trainCxt.model_config.vocabulary_size, trainCxt.num_negative_samples)

    var_len_past_click = batch[:, 2]
    click_context = process_past_click(var_len_past_click, trainCxt.num_click_context, trainCxt.model_config.pad_index)

    return positive_samples, negative_samples, click_context

def get_feeddict(batch, mod, trainCxt):
    process_data = splitIO(batch, trainCxt)
    feed_keys = mod.place_holders()
    feed = dict(zip(feed_keys, process_data))
    return feed

def iterate_minibatches(inputs, batchsize, shuffle=False):
    if shuffle:
        indices = np.arange(inputs.shape[0])
        np.random.shuffle(indices)
    for start_idx in range(0, inputs.shape[0] - batchsize + 1, batchsize):
        if shuffle:
            excerpt = indices[start_idx:start_idx + batchsize]
        else:
            excerpt = slice(start_idx, start_idx + batchsize)
        yield inputs[excerpt]


def logBreak() :
    print "------------------------------------------"


def print_dict(dict_1) :
    for key in dict_1:
        print str(key) + " : " + str(dict_1.get(key))

def run_train(trainCxt) :
    """:type trainCxt : trainingcontext"""
    modelconf = trainCxt.model_config #type: modelconfig
    logBreak()
    print "Using train context : "
    print_dict(trainCxt.__dict__)
    logBreak()
    print "Using model config : "
    print_dict(modelconf.__dict__)
    logBreak()

    productdict, trainFrame, testFrame = preparedata(trainCxt.data_path, pad_text=trainCxt.pad_text,
                                           default_click_text= trainCxt.default_click_text,
                                           test_size= trainCxt.test_size,
                                           min_click_context= trainCxt.min_click_context)
    train = trainFrame.as_matrix()
    test = testFrame.as_matrix()

    modelconfig.pad_index = productdict.get(trainCxt.pad_text)
    if trainCxt.default_click_text is not None :
        modelconfig.default_click_index = productdict.get(trainCxt.default_click_text)

    ################################### Start model building

    modelconf.vocabulary_size = productdict.dictSize()
    mod = mf.get_model(modelconf) #type: model

    sess = tf.Session()
    sess.run(tf.global_variables_initializer())

    # if trainCxt.init_pad_to_zeros :
    #     sess.run(mod.embedding_dict()[productdict.getdefaultindex()].assign(tf.zeros([modelconf.embedding_size])))

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
    # test_summary_writer = tf.summary.FileWriter("/tmp/test-cdm-v1-" + timestamp, sess.graph)

    ################################### End model building

    ################################### Saving dict to file
    if trainCxt.save_model :
        os.makedirs(trainCxt.model_dir)
        # os.mkdir(trainCxt.model_dir, 0777)
        nn_model_dir = trainCxt.getNnDir()
        product_dict_model_dir = trainCxt.getProductDictDir()
        train_context_model_dir = trainCxt.getTrainCxtDir()

        with open(product_dict_model_dir, 'w+b') as handle:
            pickle.dump(productdict, handle, protocol=pickle.HIGHEST_PROTOCOL)
        with open(train_context_model_dir, 'w+b') as handle:
            pickle.dump(trainCxt, handle, protocol=pickle.HIGHEST_PROTOCOL)

        print "saved productdict and trainCxt into " + trainCxt.model_dir
        logBreak()
    ################################### End saving dict to file

    ################################### Start model training

    print "model training started"

    saver = tf.train.Saver()

    counter = 0
    for epoch in range(trainCxt.num_epochs) :
        print "epoch : " + str(epoch)
        for batch in iterate_minibatches(train, trainCxt.batch_size, shuffle=True):
            feed = get_feeddict(batch, mod, trainCxt)
            _, loss_val, summary = sess.run([mod.minimize_step(), mod.loss(), loss_summary], feed_dict=feed)
            # print loss_val
            if summary_writer is not None :
                summary_writer.add_summary(summary, counter)

            if summary_writer is not None and counter % trainCxt.test_summary_publish_iters == 0 :
                feed = get_feeddict(test, mod, trainCxt)
                all_summary = sess.run(merged_summary, feed_dict = feed)
                summary_writer.add_summary(all_summary, counter)

            if trainCxt.save_model and trainCxt.save_model_num_iter != None and counter % trainCxt.save_model_num_iter == 0:
                saver.save(sess, nn_model_dir + ".counter" , global_step = counter)
                print "saved nn model on counter " + str(counter) + " into : " + nn_model_dir

            counter = counter + 1

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
    trainCxt.data_path = "/home/thejus/workspace/learn-cascading/data/sessionExplode-201708.MOB" + "/part-000*"
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

    modelconf = modelconfig("softmax_model" , None, 50)
    # modelconf.layer_count = [1024, 512, 256]
    modelconf.use_context = True
    modelconfig.reuse_context_dict = True
    trainCxt.model_config = modelconf

    run_train(trainCxt)
