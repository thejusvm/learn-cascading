import numpy as np
import tensorflow as tf

from mind_palace.product_ranker.models.max_margin_model import max_margin_model

# model_dir = ".saved_models/run.20170928-14-01-02"
# nn_model_dir = model_dir + '/nn'
# product_dict_model_dir = model_dir + '/productdict.pickle'
# train_context_model_dir = model_dir + '/train_context'

with open('saved_models/sessionsimple-2l-productdict.pickle', 'rb') as handle:
    productdict = pickle.load(handle)

init = np.ones([10, 10]) * range(10)
# print init.T
md = max_margin_model(10, 10, init_embedding = init.T)

# positive_samples, negative_samples, embeddings_dict, loss, train_step = getmodel(vocabulary_size, embedding_size)

sess = tf.Session()
sess.run(tf.global_variables_initializer())

# loss_summary = tf.summary.scalar("loss", md.loss)
# merged_summary = tf.summary.merge_all()
# timestamp = str(time.time())
# summary_writer = tf.summary.FileWriter("/tmp/cdm-v1-" + timestamp, sess.graph)

positive_samples_test = [[1.0], [4.0]]
negative_samples_test = [[0.0,0.0,0.0, 2.0, 3.0, 6.0], [0.0, 6.0, 8.0, 5.0, 7.0, 9.0]]
context = [[2.0, 4.0, 6.0, 0.0, 0.0, 0.0], [6.0, 8.0, 5.0, 7.0, 9.0, 0.0]]

# sess.run(md.embeddings_dict[0].assign(tf.zeros([md.embedding_size])))

feed = {md.positive_samples : positive_samples_test,
        md.negative_samples : negative_samples_test,
        md.click_context_samples : context}


for score in sess.run([md.positive_and_context, md.negative_and_context], feed_dict = feed):
    print score
    print "---------"

# for i in range(1000) :
#     _, _ , summary = sess.run([model.train_step, model.loss, loss_summary], feed_dict=feed)
#     #     sess.run(embeddings_dict[0].assign(tf.zeros([embedding_size])))
#     summary_writer.add_summary(summary, i)
#
# def computeScore(i) :
#     i_embedding = tf.nn.embedding_lookup(model.embeddings_dict, i)
#     score = model.nn(i_embedding)
#     return sess.run(score)
#
# for i in range(0, 9) :
#     print str(i) + " " + str(computeScore([i]))