import numpy as np
import tensorflow as tf
import sys

from mind_palace.product_ranker.models.modelconfig import modelconfig, AttributeConfig, EmbeddingDicts
from mind_palace.product_ranker.models.softmax_model import softmax_model, nn_probability
from mind_palace.product_ranker.commons import generate_feature_names
from mind_palace.product_ranker.constants import TRAINING_COL_PREFIXES



product_emb = np.ones([9, 10]) * range(10)
product_weight = np.ones([9, 10]) * range(10)
product_bias = range(10)

brand_emb = np.ones([4, 10]) * range(10) + 10
brand_weight = np.ones([4, 10]) * range(10) + 10
brand_bias = range(10) + np.ones(10) * 10

product_emb_size = 9
brand_emb_size = 4

mdl_conf = modelconfig("softmax_model")
mdl_conf.use_context = True
mdl_conf.click_pooling = "sum"
mdl_conf.probability_function = "nn"
mdl_conf.layer_count = []
mdl_conf.click_non_linearity = False
mdl_conf.enable_default_click = False
pid_embedding_dicts = EmbeddingDicts(context_dict=product_emb.T, softmax_weights=product_weight.T, softmax_bias=product_bias)
brand_embedding_dicts = EmbeddingDicts(context_dict=brand_emb.T, softmax_weights=brand_weight.T, softmax_bias=brand_bias)
mdl_conf.attributes_config = [AttributeConfig("productId", product_emb_size, 10, override_embeddings=pid_embedding_dicts),
                              AttributeConfig("brand", brand_emb_size, 10, override_embeddings=brand_embedding_dicts)]
md = softmax_model(mdl_conf) #type: softmax_model

sess = tf.Session()

positive_samples_test = [[1], [4]]
negative_samples_test = [[0, 0, 0, 4, 3, 6], [0, 6, 8, 5, 7, 9]]
context = [[2, 3, 9, 0, 0, 0], [6, 8, 5, 7, 0, 0]]
bought = []

feed_val_single = [positive_samples_test, negative_samples_test, negative_samples_test, negative_samples_test, context, context, bought]
feed_vals = feed_val_single + feed_val_single

feature_names = generate_feature_names([x.name for x in mdl_conf.attributes_config], feature_prefixes=TRAINING_COL_PREFIXES)


md.feed_input(feature_names, feed_vals)
sess.run(tf.global_variables_initializer())

click_context_embedding_sum = np.array([[14] * product_emb_size + [44] * brand_emb_size, [26] * product_emb_size + [66] * brand_emb_size], dtype=float)
positive_product_weights = np.array([[[1] * product_emb_size + [11] * brand_emb_size], [[4] * product_emb_size + [14] * brand_emb_size] ], dtype=float)
positive_product_layer_1 = np.array([[[1] * product_emb_size + [11] * brand_emb_size + [14] * product_emb_size + [44] * brand_emb_size]
                                               , [[4] * product_emb_size + [14] * brand_emb_size + [26] * product_emb_size + [66] * brand_emb_size]]
                                           , dtype=float)

negative_product_weights = np.array([
    [
        [0] * product_emb_size + [10] * brand_emb_size,
        [0] * product_emb_size + [10] * brand_emb_size,
        [0] * product_emb_size + [10] * brand_emb_size,
        [4] * product_emb_size + [14] * brand_emb_size,
        [3] * product_emb_size + [13] * brand_emb_size,
        [6] * product_emb_size + [16] * brand_emb_size
     ],[
        [0] * product_emb_size + [10] * brand_emb_size,
        [6] * product_emb_size + [16] * brand_emb_size,
        [8] * product_emb_size + [18] * brand_emb_size,
        [5] * product_emb_size + [15] * brand_emb_size,
        [7] * product_emb_size + [17] * brand_emb_size,
        [9] * product_emb_size + [19] * brand_emb_size
     ]
], dtype=float)

negative_product_layer_1 = np.array([
    [
        [0] * product_emb_size + [10] * brand_emb_size + [14] * product_emb_size + [44] * brand_emb_size,
        [0] * product_emb_size + [10] * brand_emb_size + [14] * product_emb_size + [44] * brand_emb_size,
        [0] * product_emb_size + [10] * brand_emb_size + [14] * product_emb_size + [44] * brand_emb_size,
        [4] * product_emb_size + [14] * brand_emb_size + [14] * product_emb_size + [44] * brand_emb_size,
        [3] * product_emb_size + [13] * brand_emb_size + [14] * product_emb_size + [44] * brand_emb_size,
        [6] * product_emb_size + [16] * brand_emb_size + [14] * product_emb_size + [44] * brand_emb_size
     ],[
        [0] * product_emb_size + [10] * brand_emb_size + [26] * product_emb_size + [66] * brand_emb_size,
        [6] * product_emb_size + [16] * brand_emb_size + [26] * product_emb_size + [66] * brand_emb_size,
        [8] * product_emb_size + [18] * brand_emb_size + [26] * product_emb_size + [66] * brand_emb_size,
        [5] * product_emb_size + [15] * brand_emb_size + [26] * product_emb_size + [66] * brand_emb_size,
        [7] * product_emb_size + [17] * brand_emb_size + [26] * product_emb_size + [66] * brand_emb_size,
        [9] * product_emb_size + [19] * brand_emb_size + [26] * product_emb_size + [66] * brand_emb_size
     ]
], dtype=float)

positive_handler = md.positive_handler.probability_fn #type:nn_probability
negative_handler = md.negative_handler.probability_fn #type:nn_probability

test_key_vals = [[md.click_embedder.embeddings_sum, click_context_embedding_sum, "context_sum"],
                 [md.positive_embedder.weights, positive_product_weights, "positive_weights"],
                 [md.negative_embedder.weights, negative_product_weights, "negative_weights"],
                 [positive_handler.layer_1, positive_product_layer_1, "positive_layer_1"],
                 [negative_handler.layer_1, negative_product_layer_1, "negative_layer_1"]]

scorre = []
for [graph_node, expected_val, tag] in test_key_vals:
    val = sess.run(graph_node)
    if np.array_equal(expected_val, val):
        print "test for ", tag, " passed"
        print "---------"
    else:
        print "expected : ", expected_val
        print "but got  : ", val
        raise ValueError("test for " + tag + "failed")


