import numpy as np
import tensorflow as tf
import sys
from mind_palace.product_ranker.modelconfig import modelconfig
from softmax_model import softmax_model



init_emb = np.ones([5, 10]) * range(10)
init_weight = np.ones([5, 10]) * range(10)
init_bias = range(10) + np.ones(10)

# print init_emb.T
# print init_weight.T

mdl_conf = modelconfig("softmax_model", vocab_size=10, embedding_size=5)
mdl_conf.use_context = True
mdl_conf.default_click_index = 1

md = softmax_model(mdl_conf, context_dict = init_emb.T, softmax_weights = init_weight.T, softmax_bias = init_bias) #type: softmax_model

sess = tf.Session()
sess.run(tf.global_variables_initializer())

sess.run(md.context_dict[1].assign(np.ones([mdl_conf.embedding_size]) * 3))

positive_samples_test = [2.0, 4.0]
negative_samples_test = [[0.0,0.0,0.0, 4.0, 3.0, 6.0], [0.0, 6.0, 8.0, 5.0, 7.0, 9.0]]
context = [[2, 4, 6, 0, 0, 0], [6, 8, 5, 7, 9, 0], [0, 0, 0, 0, 0, 0]]

feed = {md.poistive_label(): positive_samples_test,
        md.negative_label() : negative_samples_test,
        md.click_product_label() : context}

for score in sess.run([md.positive_samples], feed_dict = feed):
    print score
    print "---------"
# sess.run(md.embeddings_dict[0].assign(tf.zeros([md.embedding_size])))


pos_shit = [md.positive_samples,
            md.positive_weights,
            md.positive_bias,
            md.click_embeddings_mean,
            md.positive_logits,
            md.positive_xent]
neg_shit = [md.negative_samples,
            md.negative_weights,
            md.negative_bias,
            md.click_embeddings_mean,
            md.negative_weights_multiply_context,
            md.negative_logits,
            md.negative_xent]
