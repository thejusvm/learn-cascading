import numpy as np
import tensorflow as tf

from mind_palace.product_ranker.models.modelconfig import modelconfig, AttributeConfig, EmbeddingDicts
from mind_palace.product_ranker.models.softmax_model import softmax_model

init_emb = np.ones([5, 10]) * range(10)
init_weight = np.ones([5, 10]) * range(10)
init_bias = range(10) + np.ones(10)

# print "emb"
# print init_emb.T
# print "weight"
# print init_weight.T

mdl_conf = modelconfig("softmax_model")
mdl_conf.use_context = True
mdl_conf.enable_default_click = False
embedding_dicts = EmbeddingDicts(context_dict=init_emb.T, softmax_weights=init_weight.T, softmax_bias=init_bias)
mdl_conf.attributes_config = [AttributeConfig("pid", 5, 10, override_embeddings=embedding_dicts),
                              AttributeConfig("brand", 5, 10, override_embeddings=embedding_dicts)]

md = softmax_model(mdl_conf) #type: softmax_model

sess = tf.Session()
sess.run(tf.global_variables_initializer())

positive_samples_test = [[1], [4]]
negative_samples_test = [[0, 0, 0, 4, 3, 6], [0, 6, 8, 5, 7, 9]]
context = [[2, 3, 0, 0, 0, 0], [6, 8, 5, 7, 0, 0]]

feed_vals = [positive_samples_test, negative_samples_test, context, positive_samples_test, negative_samples_test, context]
md.feed_input(feed_vals)

ps = md.negative_sigmoid
for score in sess.run([ps.weights_cross_context_sum, ps.modified_bias, ps.logits, ps.xent]):
    print score
    print "---------"
# sess.run(md.embeddings_dict[0].assign(tf.zeros([md.embedding_size])))


# pos_shit = [md.positive_samples,
#             md.positive_weights,
#             md.positive_bias,
#             md.click_embeddings_mean,
#             md.positive_logits,
#             md.positive_xent]
# neg_shit = [md.negative_samples,
#             md.negative_weights,
#             md.negative_bias,
#             md.click_embeddings_mean,
#             md.negative_weights_multiply_context,
#             md.negative_logits,
#             md.negative_xent]
