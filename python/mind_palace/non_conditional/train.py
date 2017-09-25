import cPickle as pickle
import glob
import json
import numpy as np
import pandas as pd
import tensorflow as tf
import time
import sys
from sklearn.model_selection import train_test_split

from model import getmodel
from mind_palace.DictIntegerizer import DictIntegerizer

################################### Start data prep
def train(path) :
    filenames = glob.glob(path + "/part-*")

    # frame = pd.DataFrame()
    list_ = []
    for file_ in filenames:
        #     df = pd.read_csv(file_, index_col=None, header=0, sep="\t")
        df = pd.read_csv(file_, sep="\t")
        list_.append(df)
    df = pd.concat(list_)

    productdict = DictIntegerizer()
    integerize = lambda x : productdict.get(x)
    df["positiveProductsInt"] = df["positiveProducts"].apply(integerize)

    def jsonandintegerize(jString) :
        strList = json.loads(jString)
        return map(integerize, strList)

    # jsonListCols = ["negativeProducts", "pastClickedProducts", "pastBoughtProducts"]
    jsonListCols = ["negativeProducts"]
    for col in jsonListCols :
        df[str(col + "Int")] = df[col].map(jsonandintegerize)


    # raw_data = df[["positiveProductsInt", "negativeProductsInt", "pastClickedProductsInt", "pastBoughtProductsInt"]]
    raw_data = df[["positiveProductsInt", "negativeProductsInt"]]
    trainFrame, testFrame = train_test_split(raw_data, test_size=0.2)
    train = trainFrame.as_matrix()
    ################################### End data prep

    ################################### Start model building

    vocabulary_size = productdict.dictSize()
    embedding_size = 15

    positive_samples, negative_samples, embeddings_dict, loss, train_step = getmodel(vocabulary_size, embedding_size)

    sess = tf.Session()
    sess.run(tf.global_variables_initializer())

    loss_summary = tf.summary.scalar("loss", loss)
    # merged_summary = tf.summary.merge_all()
    timestamp = str(time.time())
    summary_writer = tf.summary.FileWriter("/tmp/cdm-v1-" + timestamp, sess.graph)

    ################################### End model building

    ################################### Saving dict to file
    with open('saved_models/sessionsimple-productdict.pickle', 'wb') as handle:
        pickle.dump(productdict, handle, protocol=pickle.HIGHEST_PROTOCOL)
    ################################### End saving dict to file

    ################################### Start model training

    batch_size = 500
    num_epochs = 10

    num_negative_samples = 20

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
            positive_samples_data = np.reshape(batch[:, 0], [batch_size, 1])
            negative_samples_var_len = batch[:, 1]

            negative_samples_data = np.random.randint(vocabulary_size, size = (batch_size, num_negative_samples))

            for i in range(batch_size) :
                for j in range(len(negative_samples_var_len[i])) :
                    negative_samples_data[i][j] = negative_samples_var_len[i][j]

            feed = {positive_samples : positive_samples_data, negative_samples : negative_samples_data}
            _, loss_val, summary = sess.run([train_step, loss, loss_summary], feed_dict=feed)

            summary_writer.add_summary(summary, counter)
            counter = counter + 1
        ################################### Saving model to file
        saver.save(sess, "saved_models/sessionsimple." + str(i), global_step = counter)
        ################################### End model to file

    ################################### End model training

        # counter = 0

            # print positive_samples_data
            # print negative_samples_data
            # break
        # break

if __name__ == '__main__' :
    # path = sys.argv[0]
    path = "/home/thejus/workspace/learn-cascading/data/sessionExplode-201708.MOB"
    print "tranin with path"
    train(path)




