import tensorflow as tf
import time
from sessionpredictor_model import getmodel, nn

embedding_size = 10
vocabulary_size = 10
positive_samples, negative_samples, embeddings_dict, loss, train_step = getmodel(vocabulary_size, embedding_size)


sess = tf.Session()
sess.run(tf.global_variables_initializer())

loss_summary = tf.summary.scalar("loss", loss)
# merged_summary = tf.summary.merge_all()
timestamp = str(time.time())
summary_writer = tf.summary.FileWriter("/tmp/cdm-v1-" + timestamp, sess.graph)

positive_samples_test = [[1.0], [4.0]]
negative_samples_test = [[0.0,0.0,0.0, 2.0, 3.0, 6.0], [0.0, 6.0, 8.0, 5.0, 7.0, 9.0]]

sess.run(embeddings_dict[0].assign(tf.zeros([embedding_size])))

feed = {positive_samples : positive_samples_test, negative_samples : negative_samples_test}

for i in range(1000) :
    _, _ , summary = sess.run([train_step, loss, loss_summary], feed_dict=feed)
    #     sess.run(embeddings_dict[0].assign(tf.zeros([embedding_size])))
    summary_writer.add_summary(summary, i)

def computeScore(i) :
    i_embedding = tf.nn.embedding_lookup(embeddings_dict, i)
    score = nn(i_embedding)
    return sess.run(score)

for i in range(0, 9) :
    print str(i) + " " + str(computeScore([i]))