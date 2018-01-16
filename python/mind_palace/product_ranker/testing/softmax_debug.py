import numpy as np
import tensorflow as tf
import sys

from mind_palace.product_ranker.models.modelconfig import modelconfig, AttributeConfig, EmbeddingDicts
from mind_palace.product_ranker.models.softmax_model import softmax_model
from mind_palace.product_ranker.models.softmax_model_old import softmax_model_old
from mind_palace.product_ranker.commons import generate_feature_names
from mind_palace.product_ranker.constants import TRAINING_COL_PREFIXES

init_emb = np.ones([5, 10]) * range(10)
init_weight = np.ones([5, 10]) * range(10)
init_bias = range(10) #+ np.ones(10)

init_emb_1 = np.ones([3, 10]) * range(10) + 10
init_weight_1 = np.ones([3, 10]) * range(10) + 10
init_bias_1 = range(10) + np.ones(10) * 10

# print "emb"
# print init_emb.T
# print "weight"
# print init_weight.T

mdl_conf = modelconfig("softmax_model")
mdl_conf.use_context = False
mdl_conf.probability_function = "nn"
mdl_conf.layer_count = []
mdl_conf.click_non_linearity = False
mdl_conf.enable_default_click = False
embedding_dicts = EmbeddingDicts(context_dict=init_emb.T, softmax_weights=init_weight.T, softmax_bias=init_bias)
embedding_dicts_1 = EmbeddingDicts(context_dict=init_emb_1.T, softmax_weights=init_weight_1.T, softmax_bias=init_bias_1)
mdl_conf.attributes_config = [AttributeConfig("pid", 5, 10, override_embeddings=embedding_dicts),
                              AttributeConfig("brand", 3, 10, for_ranking=True, for_regularization=True, override_embeddings=embedding_dicts_1)]
mdl_conf.regularizer_id = "pid"
md = softmax_model(mdl_conf) #type: softmax_model

sess = tf.Session()

positive_samples_test = [[1], [4]]
negative_samples_test = [[0, 0, 0, 4, 3, 6], [0, 6, 8, 5, 7, 9]]
context = [[2, 3, 0, 0, 0, 0], [6, 8, 5, 7, 0, 0]]

feed_vals = [positive_samples_test, negative_samples_test, context, positive_samples_test, negative_samples_test, context]
feature_names = generate_feature_names([x.name for x in mdl_conf.attributes_config], feature_prefixes=TRAINING_COL_PREFIXES)
# print feature_names
# sys.exit(0)

md.feed_input(feature_names, feed_vals)
sess.run(tf.global_variables_initializer())

print md.enable_regularizer
# scorre = [x[1] for x in md.score()]
# pfn = md.negative_handler.probability_fn
# scorre = [pfn.embeddings, pfn.context, pfn.layer_1, pfn.logits]
# for score in sess.run(scorre):
#     print score
#     print "---------"
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
