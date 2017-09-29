import cPickle as pickle
import tensorflow as tf
import numpy as np
from model import model, nn
from operator import itemgetter
import trainingcontext as tc
import sys

path = "saved_models/run.20170929-19-36-29"

dir = tc.getTraningContextDir(path)
trainCxt = None

with open(dir, 'rb') as handle:
    trainCxt = pickle.load(handle)

with open(trainCxt.getProductDictDir(), 'rb') as handle:
    productdict = pickle.load(handle)

vocabulary_size = productdict.dictSize()
modelconf = trainCxt.model_config

md = model(modelconf)
saver = tf.train.Saver()

def computeScore(sess, i) :
    i_embedding = tf.nn.embedding_lookup(md.embeddings_dict, i)
    with tf.variable_scope("discriminator", reuse = True):
        score = nn(modelconf, i_embedding, None)[0]
    return sess.run(score)

with tf.Session() as sess:
    saver.restore(sess, trainCxt.getNnDir())
    for i in sess.run([md.positive_score - md.negative_score], feed_dict = { md.positive_samples : [[1], [2], [3], [4]], md.negative_samples : [[1], [2], [3], [4]], md.click_context_samples : [[0]] }) :
        print i
    sys.exit(0)

with tf.Session() as sess:
    # Restore variables from disk.
    saver.restore(sess, trainCxt.getNnDir())
    products = productdict.getDict()
    productScore = []
    c = 0
    allscores = computeScore(sess, range(vocabulary_size))
    # print allscores
    for id in products :
        pindex = productdict.get(id)
        # print id + " " + str(allscores[pindex][0])
        productScore.append([id, allscores[pindex][0]])
        c = c + 1
        # if(c > 20) :
        #     break

    productScore = sorted(productScore, key=itemgetter(1), reverse=True)
    for id in productScore:
        print id[0] + " " + str(id[1])



# Check the values of the variables
