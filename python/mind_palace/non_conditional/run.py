import cPickle as pickle
import tensorflow as tf
import numpy as np
from model import model, nn
from operator import itemgetter

with open('saved_models/sessionsimple-productdict.pickle', 'rb') as handle:
    productdict = pickle.load(handle)

vocabulary_size = productdict.dictSize()
embedding_size = 15
md = model(vocabulary_size, embedding_size)
saver = tf.train.Saver()

def computeScore(sess, i) :
    i_embedding = tf.nn.embedding_lookup(md.embeddings_dict, i)
    score = nn(i_embedding)
    return sess.run(score)

with tf.Session() as sess:
    # Restore variables from disk.
    saver.restore(sess, "./saved_models/sessionsimple.19-1440")
    products = productdict.getDict()
    productScore = []
    c = 0
    allscores = computeScore(sess, range(vocabulary_size))
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
