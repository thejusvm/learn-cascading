import cPickle as pickle
import tensorflow as tf
from operator import itemgetter

import trainingcontext as tc
import model_factory as mf
import modelconfig

# import get_stats as gs

path = "saved_models/run.20171003-16-21-57"

dir = tc.getTraningContextDir(path)
trainCxt = None

with open(dir, 'rb') as handle:
    trainCxt = pickle.load(handle)

with open(trainCxt.getProductDictDir(), 'rb') as handle:
    productdict = pickle.load(handle)

vocabulary_size = productdict.dictSize()
modelconf = trainCxt.model_config # type: modelconfig

mod = mf.get_model(modelconf)
saver = tf.train.Saver()

# def computeScore(sess, i) :

    # return sess.run(score)
# with tf.Session() as sess:
#     saver.restore(sess, trainCxt.getNnDir())
#     for i in sess.run([mod.positive_score, mod. negative_score, mod.positive_score - mod.negative_score], feed_dict = { mod.positive_samples : [[1]], mod.negative_samples : [[1, 2, 3, 4]], mod.click_context_samples : [[0]] }) :
#         print i
#     sys.exit(0)

with tf.Session() as sess:
    # Restore variables from disk.
    saver.restore(sess, trainCxt.getNnDir())
    products = productdict.getDict()
    productScore = []
    c = 0
    scores = mod.score(range(vocabulary_size), None)
    allscores = sess.run(scores)
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
