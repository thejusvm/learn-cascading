import tensorflow as tf

from mind_palace.commons.padding_handler import padding_handler
from mind_palace.product_ranker.models.modelconfig import modelconfig, AttributeConfig
from model import model
from mind_palace.product_ranker.commons import generate_feature_names
import mind_palace.product_ranker.constants as CONST
import collections
import math


def fetch_features(attribute_names, prefix, feature_names, inputs):
    prefix_feature_names = generate_feature_names(attribute_names, [prefix])
    feature_indices = [feature_names.index(i) for i in prefix_feature_names]
    features = [inputs[i] for i in feature_indices]
    return features

class softmax_model(model) :

    def __init__(self, modelConf):
        model.__init__(self, modelConf)
        self.modelConf = modelConf # type: modelconfig
        self.per_attribute_embeddings = []

        self.attributes_config = self.modelConf.attributes_config
        for attribute_config in self.attributes_config :
            attribute_embeddings = AttributeEmbeddings(modelConf, attribute_config)
            self.per_attribute_embeddings.append(attribute_embeddings)

    def feed_input(self, feature_names, inputs):
        num_attributes = len(self.attributes_config)
        attribute_names = [self.attributes_config[i].name for i in range(num_attributes)]

        click_features = fetch_features(attribute_names, CONST.CLICK_COL_PRERFIX, feature_names, inputs)
        self.click_embedder = ContextClickProductHandler(self.per_attribute_embeddings, click_features, self.model_config)
        postive_features = fetch_features(attribute_names, CONST.POSITIVE_COL_PREFIX, feature_names, inputs)
        self.positive_embedder = ScoringProductHandler(self.per_attribute_embeddings, postive_features)
        negative_features = fetch_features(attribute_names, CONST.NEGATIVE_COL_PREFIX, feature_names, inputs)
        self.negative_embedder = ScoringProductHandler(self.per_attribute_embeddings, negative_features)

        self.click_embeddings = self.click_embedder.embeddings_mean
        self.click_embeddings = tf.expand_dims(self.click_embeddings, 1)
        self.positive_weights = self.positive_embedder.weights
        self.positive_bias = self.positive_embedder.bias
        self.negative_weights = self.negative_embedder.weights
        self.negative_bias = self.negative_embedder.bias

        self.context_embedding = self.click_embeddings
        self.batch_size = tf.shape(self.positive_weights)[0]

        self.positive_handler = to_probability(self.model_config, self.positive_weights, self.positive_bias, self.context_embedding, True)
        self.negative_handler = to_probability(self.model_config, self.negative_weights, self.negative_bias, self.context_embedding, False)

        self.sigmoid_loss = (tf.reduce_sum(self.positive_handler.xent) + tf.reduce_sum(self.negative_handler.xent)) / tf.cast(self.batch_size, tf.float32)

        self.max_negative_score = tf.reduce_max(self.negative_handler.logits, reduction_indices = [1])
        self.max_negative_score = tf.reshape(self.max_negative_score, [tf.size(self.max_negative_score)])

        self.positive_score_vector = tf.reshape(self.positive_handler.logits, [tf.size(self.positive_handler.logits)])
        self.prec_vector = tf.cast(tf.greater(self.positive_score_vector, self.max_negative_score), tf.float32)
        self.prec_1 = tf.reduce_mean(self.prec_vector)

        self.less_than = tf.cast(tf.less(self.positive_handler.logits, self.negative_handler.logits), tf.float32)
        self.rank_per_record = tf.reduce_sum(self.less_than, reduction_indices = [1])
        self.rank_per_record = self.rank_per_record + 1
        self.mean_rank = tf.reduce_mean(self.rank_per_record)

        self.reciprocal_rank_per_record = 1 / tf.cast(self.rank_per_record, tf.float32)
        self.mean_reciprocal_rank = tf.reduce_mean(self.reciprocal_rank_per_record)


        self.positive_probability = tf.sigmoid(self.positive_handler.logits)
        self.positive_mean_probability = tf.reduce_mean(self.positive_probability)


    def test_summaries(self):
        return [["loss", self.sigmoid_loss],
                ["prec-1", self.prec_1],
                ["probability", self.positive_mean_probability],
                ["mean_rank", self.mean_rank],
                ["mean_reciprocal_rank", self.mean_reciprocal_rank]]

    def score(self):
        return [["score", self.positive_score_vector],
                ["xent", self.positive_handler.xent],
                ["probability", self.positive_probability]]

    def place_holders(self):
        return []

    def loss(self):
        return self.sigmoid_loss

    def minimize_step(self):
        pass
        # return self.train_step

class to_probability :
    def __init__(self, modelconfig, weights, bias, context_embedding, positive = True):
        """
        :type modelconfig: modelconfig
        :param weights:
        :param bias:
        :param context_embedding:
        :param positive:
        """
        self.probability_fn = None
        if "sigmoid" == modelconfig.probability_function:
            self.probability_fn = sigmoid(weights, bias, context_embedding)
        else :
            self.probability_fn = nn_probability(weights, context_embedding, modelconfig.layer_count)

        self.logits = self.probability_fn.logits
        if positive :
            labels = tf.ones_like(self.logits)
        else :
            labels = tf.zeros_like(self.logits)
        self.xent = tf.nn.sigmoid_cross_entropy_with_logits(labels=labels, logits=self.logits)

class sigmoid :
    def __init__(self, weights, bias, context_embedding):
        self.weights = weights
        self.bias = bias
        self.context_embedding = context_embedding
        self.modified_bias = tf.reduce_sum(self.bias, reduction_indices=[1])
        self.weights_cross_context = tf.multiply(self.context_embedding, self.weights)
        self.weights_cross_context_sum = tf.reduce_sum(self.weights_cross_context, reduction_indices=[2])
        self.logits = self.weights_cross_context_sum + self.modified_bias

class nn_probability :
    def __init__(self, embeddings, context, layer_count):
        self.embeddings = embeddings
        self.context = context

        self.embedding_shape = tf.shape(self.embeddings)
        self.embedding_size = tf.size(self.embeddings)
        self.repeate_count = self.embedding_size / (self.embedding_shape[0] * self.embedding_shape[2])
        self.context_rep = tf.tile(context, [1, self.repeate_count, 1])
        self.context_rep_reshape = tf.reshape(self.context_rep, self.embedding_shape)
        self.layer_1 = tf.concat([self.embeddings, self.context_rep_reshape], 2)
        self.logits = nn("probability_predictor", layer_count, self.layer_1, last_out_layer_count=1, last_no_activation=True)

class AttributeEmbeddings :

    def __init__(self, modelConf, attribute_config):
        """
        :type modelConf: modelconfig
        :type attribute_config: AttributeConfig
        """
        self.attribute_config = attribute_config
        self.attribute_name = attribute_config.name
        self.vocab_size = attribute_config.vocab_size
        self.embedding_size = attribute_config.embedding_size
        self.modelConf = modelConf
        override_embedding = attribute_config.override_embeddings

        if override_embedding.context_dict is None:
            self.context_dict = tf.Variable(tf.random_uniform([self.vocab_size, self.embedding_size], -1.0, 1.0),
                                            dtype=tf.float32)
        else:
            self.context_dict = tf.Variable(override_embedding.context_dict, dtype=tf.float32)
        if modelConf.reuse_context_dict:
            self.softmax_weights = self.context_dict
        else:
            if override_embedding.softmax_weights is None:
                # init_softmax_weights = tf.random_uniform([self.vocab_size, self.embedding_size], -1, 1)
                init_softmax_weights = tf.zeros([self.vocab_size, self.embedding_size])
                self.softmax_weights = tf.Variable(init_softmax_weights,
                                                   name="sm_w_t")
            else:
                self.softmax_weights = tf.Variable(override_embedding.softmax_weights, dtype=tf.float32)
        if override_embedding.softmax_bias is None:
            # init_softmax_bias = tf.random_uniform([self.vocab_size], -1, 1)
            init_softmax_bias = tf.zeros([self.vocab_size])
            self.softmax_bias = tf.Variable(init_softmax_bias, name="sm_b")
        else:
            self.softmax_bias = tf.Variable(override_embedding.softmax_bias, dtype=tf.float32)

PerAttrClickEmb = collections.namedtuple('PerAttrClickEmb', 'pad_handler click_embedding')

def _nn_internal_(layer_count, input_embedding, last_out_layer_count = None, last_no_activation = False) :

    lastdim = input_embedding.get_shape().ndims - 1
    layer_in_count = input_embedding.get_shape()[lastdim].value

    if last_out_layer_count is None:
        last_out_layer_count = layer_in_count

    in_layer = input_embedding
    out_layer = in_layer

    counter = 0
    layer_count = list(layer_count)
    layer_count.append(last_out_layer_count)
    for num in layer_count :
        counter = counter + 1
        if last_no_activation and counter == len(layer_count) :
            activation = None
            bais_init = 0
        else :
            activation = tf.nn.relu
            bais_init = 10
        layer_out_count = num
        stddev = math.sqrt(2.0 / (layer_in_count + layer_out_count))
        kernal_initial = tf.truncated_normal_initializer(0, stddev)
        out_layer = tf.layers.dense(inputs = in_layer,
                                    units = num,
                                    activation=activation,
                                    kernel_initializer =  kernal_initial,
                                    bias_initializer = tf.constant_initializer(bais_init),
                                    name = "layer_" + str(counter))

        in_layer = out_layer
        layer_in_count = layer_out_count

    return out_layer

def nn(namespace, layer_count, embeddings, last_out_layer_count = None, last_no_activation =False) :
    with tf.variable_scope(namespace, reuse=tf.AUTO_REUSE):
        return _nn_internal_(layer_count, embeddings, last_out_layer_count, last_no_activation)

class ContextClickProductHandler() :

    def __init__(self, attributeEmbeddings, input, model_config):
        self.clickAttrEmbs = []
        self.input = input
        self.num_non_pad = 0
        for i in range(len(attributeEmbeddings)):
            attribute_embedding = attributeEmbeddings[i]
            attribute_pad_handler = padding_handler(input[i], attribute_embedding.context_dict, padding_index=CONST.DEFAULT_DICT_KEYS.index(CONST.PAD_TEXT))
            self.num_non_pad = attribute_pad_handler.num_non_pad
            attribute_click_embedding = attribute_pad_handler.tensor_embeddings
            attrEmb = PerAttrClickEmb(attribute_pad_handler, attribute_click_embedding)
            self.clickAttrEmbs.append(attrEmb)
        self.attribute_click_embeddings = [x.click_embedding for x in self.clickAttrEmbs]
        self.click_embedding = tf.concat(self.attribute_click_embeddings, 2)
        if model_config.click_non_linearity:
            self.click_embedding_nn = nn("click_nn", model_config.click_layer_count, self.click_embedding)
        else:
            self.click_embedding_nn = self.click_embedding
        self.embeddings_sum = tf.reduce_sum(self.click_embedding_nn, reduction_indices=[1])
        self.embeddings_mean = self.embeddings_sum / self.num_non_pad


PerAttrScoreEmb = collections.namedtuple('PerAttrScoreEmb', 'pad_handler click_embedding')

class ScoringProductHandler() :

    def __init__(self, attributeEmbeddings, input):
        self.clickAttrEmbs = []
        self.input = input
        self.num_non_pad = 0
        self.per_attr_weights = []
        self.per_attr_bias = []
        for i in range(len(attributeEmbeddings)):
            attribute_embedding = attributeEmbeddings[i]
            attribute_input = input[i]
            self.per_attr_weights.append(tf.nn.embedding_lookup(attribute_embedding.softmax_weights, attribute_input))
            positive_bias_ = tf.nn.embedding_lookup(attribute_embedding.softmax_bias, attribute_input)
            self.per_attr_bias.append(tf.expand_dims(positive_bias_, [1]))

        self.weights = tf.concat(self.per_attr_weights, 2)
        self.bias = tf.concat(self.per_attr_bias, 1)



