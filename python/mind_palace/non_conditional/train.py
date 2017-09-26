import cPickle as pickle
import glob
import json
import numpy as np
import pandas as pd
import tensorflow as tf
import time
import sys
from model import model

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

def splitIO(batch, md) :
    batch_size = np.shape(batch)[0]

    positive_samples = np.reshape(batch[:, 0], [batch_size, 1])

    var_len_negative_samples = batch[:, 1]
    negative_samples = process_negative_samples(var_len_negative_samples, md.vocabulary_size, md.num_negative_samples)

    var_len_past_click = batch[:, 2]
    click_context = process_past_click(var_len_past_click, md.num_click_context, md.pad_index)

    return positive_samples, negative_samples, click_context

def get_feeddict(batch, md):
    process_data = splitIO(batch, md)
    feed_keys = [md.positive_samples, md.negative_samples, md.click_context_samples]
    feed = dict(zip(feed_keys, process_data))
    return feed


################################### Start data prep
def train(path) :
    filenames = glob.glob(path + "/part-0000*")

    # frame = pd.DataFrame()
    list_ = []
    for file_ in filenames:
        #     df = pd.read_csv(file_, index_col=None, header=0, sep="\t")
        df = pd.read_csv(file_, sep="\t")
        list_.append(df)
    df = pd.concat(list_)

    productdict = DictIntegerizer(default = "<pad>")
    integerize = lambda x : productdict.get(x)
    df["positiveProductsInt"] = df["positiveProducts"].apply(integerize)

    def jsonandintegerize(jString) :
        strList = json.loads(jString)
        return map(integerize, strList)

    # jsonListCols = ["negativeProducts", "pastClickedProducts", "pastBoughtProducts"]
    jsonListCols = ["negativeProducts", "pastClickedProducts"]
    for col in jsonListCols :
        df[str(col + "Int")] = df[col].map(jsonandintegerize)


    # raw_data = df[["positiveProductsInt", "negativeProductsInt", "pastClickedProductsInt", "pastBoughtProductsInt"]]
    raw_data = df[["positiveProductsInt", "negativeProductsInt", "pastClickedProductsInt"]]
    trainFrame, testFrame = train_test_split(raw_data, test_size=0.2)
    train = trainFrame.as_matrix()
    test = testFrame.as_matrix()

    print "data prep done"
    ################################### End data prep

    ################################### Start model building

    vocabulary_size = productdict.dictSize()
    embedding_size = 10

    md = model(vocabulary_size, embedding_size,
               pad_index=productdict.getdefaultindex(),
               use_context=True)

    sess = tf.Session()
    sess.run(tf.global_variables_initializer())

    loss_summary = tf.summary.scalar("loss", md.loss)

    test_loss_summary = tf.summary.scalar("test_loss", md.loss)
    test_accuracy_summary = tf.summary.scalar("test_accuracy", md.accuracy)
    test_prec_summary = tf.summary.scalar("test_prec_1", md.prec_1)

    # merged_summary = tf.summary.merge_all()
    timestamp = str(time.time())
    summary_writer = tf.summary.FileWriter("/tmp/cdm-v1-" + timestamp, sess.graph)
    # test_summary_writer = tf.summary.FileWriter("/tmp/test-cdm-v1-" + timestamp, sess.graph)

    ################################### End model building

    ################################### Saving dict to file
    with open('saved_models/sessionsimple-2l-productdict.pickle', 'w+b') as handle:
        pickle.dump(productdict, handle, protocol=pickle.HIGHEST_PROTOCOL)
    ################################### End saving dict to file

    ################################### Start model training

    print "model training started"

    batch_size = 500
    num_epochs = 20

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

    saver = tf.train.Saver()

    counter = 0
    for i in range(num_epochs) :
        print "epoch : " + str(i)
        for batch in iterate_minibatches(train, batch_size, shuffle=True):
            feed = get_feeddict(batch, md)
            _, loss_val, summary = sess.run([md.train_step, md.loss, loss_summary], feed_dict=feed)
            # print loss_val
            summary_writer.add_summary(summary, counter)

            if(counter % 100 == 0) :
                feed = get_feeddict(test, md)
                s1, s2, s3 = sess.run([test_loss_summary, test_accuracy_summary, test_prec_summary], feed_dict = feed)
                summary_writer.add_summary(s1, counter)
                summary_writer.add_summary(s2, counter)
                summary_writer.add_summary(s3, counter)

            counter = counter + 1

        ################################### Saving model to file
        saver.save(sess, "saved_models/sessionsimple-2l." + str(i), global_step = counter)
        ################################### End model to file
    print "model training ended"

    ################################### End model training

        # counter = 0

            # print positive_samples_data
            # print negative_samples_data
            # break
        # break

if __name__ == '__main__' :
    # path = sys.argv[0]
    path = "/home/thejus/workspace/learn-cascading/data/sessionExplode-201708.MOB"
    train(path)




