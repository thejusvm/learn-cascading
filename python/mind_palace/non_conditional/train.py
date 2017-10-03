import cPickle as pickle
import glob
import json
import numpy as np
import pandas as pd
import tensorflow as tf
import time
import sys
import os
from max_margin_model import max_margin_model
# from trainingcontext import  trainingcontext
import  trainingcontext as tc
from modelconfig import modelconfig

from sklearn.model_selection import train_test_split
from mind_palace.DictIntegerizer import DictIntegerizer

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
    batch_size = np.shape(batch)[0]

    positive_samples = np.reshape(batch[:, 0], [batch_size, 1])

    var_len_negative_samples = batch[:, 1]
    negative_samples = process_negative_samples(var_len_negative_samples, trainCxt.model_config.vocabulary_size, trainCxt.num_negative_samples)

    var_len_past_click = batch[:, 2]
    click_context = process_past_click(var_len_past_click, trainCxt.num_click_context, trainCxt.model_config.pad_index)

    return positive_samples, negative_samples, click_context

def get_feeddict(batch, md, trainCxt):
    process_data = splitIO(batch, trainCxt)
    feed_keys = [md.positive_samples, md.negative_samples, md.click_context_samples]
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

def prepareData():
    filenames = glob.glob(trainCxt.data_path)

    list_ = []
    for file_ in filenames:
        df = pd.read_csv(file_, sep="\t")
        list_.append(df)
    df = pd.concat(list_)

    productdict = DictIntegerizer(default = trainCxt.pad_text)
    integerize = lambda x : productdict.get(x)
    df["positiveProductsInt"] = df["positiveProducts"].apply(integerize)

    def jsonandintegerize(jString) :
        strList = json.loads(jString)
        return map(integerize, strList)

    df = df[df["pastClickedProducts"].apply(lambda x: len(json.loads(x)) > trainCxt.min_click_context)]

    # jsonListCols = ["negativeProducts", "pastClickedProducts", "pastBoughtProducts"]
    jsonListCols = ["negativeProducts", "pastClickedProducts"]
    for col in jsonListCols :
        df[str(col + "Int")] = df[col].map(jsonandintegerize)


    # raw_data = df[["positiveProductsInt", "negativeProductsInt", "pastClickedProductsInt", "pastBoughtProductsInt"]]
    raw_data = df[["positiveProductsInt", "negativeProductsInt", "pastClickedProductsInt"]]
    trainFrame, testFrame = train_test_split(raw_data, test_size = trainCxt.test_size)
    train = trainFrame.as_matrix()
    test = testFrame.as_matrix()


    click_len = raw_data["pastClickedProductsInt"].map(lambda x : len(x))
    print "histogram of num context clicks : "
    print click_len.value_counts()
    logBreak()

    print "data prep done"
    logBreak()

    return productdict, train, test

def print_dict(dict_1) :
    for key in dict_1:
        print str(key) + " : " + str(dict_1.get(key))

def run_train(trainCxt) :
    modelconf = trainCxt.model_config
    logBreak()
    print "Using train context : "
    print_dict(trainCxt.__dict__)
    logBreak()
    print "Using model config : "
    print_dict(modelconf.__dict__)
    logBreak()

    productdict, train, test = prepareData()

    ################################### Start model building

    modelconf.vocabulary_size = productdict.dictSize()
    md = max_margin_model(modelconf)

    sess = tf.Session()
    sess.run(tf.global_variables_initializer())

    if trainCxt.init_pad_to_zeros :
        sess.run(md.embeddings_dict[productdict.getdefaultindex()].assign(tf.zeros([modelconf.embedding_size])))

    loss_summary = tf.summary.scalar("loss", md.loss)

    test_loss_summary = tf.summary.scalar("test_loss", md.loss)
    test_accuracy_summary = tf.summary.scalar("test_accuracy", md.accuracy)
    test_prec_summary = tf.summary.scalar("test_prec_1", md.prec_1)

    # merged_summary = tf.summary.merge_all()
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
            feed = get_feeddict(batch, md, trainCxt)
            _, loss_val, summary = sess.run([md.train_step, md.loss, loss_summary], feed_dict=feed)
            # print loss_val
            if summary_writer is not None :
                summary_writer.add_summary(summary, counter)


            if summary_writer is not None and counter % trainCxt.test_summary_publish_iters == 0 :
                feed = get_feeddict(test, md, trainCxt)
                s1, s2, s3 = sess.run([test_loss_summary, test_accuracy_summary, test_prec_summary], feed_dict = feed)
                summary_writer.add_summary(s1, counter)
                summary_writer.add_summary(s2, counter)
                summary_writer.add_summary(s3, counter)

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
    trainCxt.data_path = "/home/thejus/workspace/learn-cascading/data/sessionExplode-201708.MOB" + "/part-00000"
    trainCxt.model_dir = "saved_models/run." + currentdate
    trainCxt.summary_dir = "/tmp/sessionsimple." + currentdate
    trainCxt.num_epochs = 10
    trainCxt.min_click_context = 2
    trainCxt.save_model = True
    trainCxt.save_model_on_epoch = False
    trainCxt.date = currentdate
    trainCxt.timestamp = timestamp
    trainCxt.publish_summary = True
    trainCxt.num_negative_samples = 30

    modelconf = modelconfig(None, 100)
    modelconf.layer_count = [1024, 512, 256]
    modelconf.use_context = False
    trainCxt.model_config = modelconf

    run_train(trainCxt)




