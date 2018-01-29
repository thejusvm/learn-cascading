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

def softmax_lookup_method(embeddingsrepo, ids) :
    return tf.nn.embedding_lookup(embeddingsrepo.softmax_weights, ids)

def context_lookup_method(embeddingsrepo, ids) :
    pad_text_index = CONST.DEFAULT_DICT_KEYS.index(CONST.PAD_TEXT)
    return padding_handler(ids, embeddingsrepo.context_dict, padding_index=pad_text_index).tensor_embeddings

class softmax_model(model) :

    def __init__(self, modelConf):
        model.__init__(self, modelConf)
        self.modelConf = modelConf # type: modelconfig

        self.ranking_attributes_embeddingsrepo = [] # type:list of EmbeddingsRepo
        self.regularizer_attributes_embeddingsrepo = []
        self.regularizer_id_embeddings = None

        self.attributes_config = self.modelConf.attributes_config #type:list of EmbeddingsRepo

        id_attribute_config = None
        for attribute_config in self.attributes_config:
            if self.modelConf.attribute_regularizer_id == attribute_config.name:
                id_attribute_config = attribute_config

        if not id_attribute_config:
            for attribute_config in self.attributes_config:
                attribute_config.for_regularization = False

        for attribute_config in self.attributes_config :
            attribute_embeddings = EmbeddingsRepo(modelConf, attribute_config, id_attribute_config)
            if attribute_config.for_ranking:
                self.ranking_attributes_embeddingsrepo.append(attribute_embeddings)
            if self.modelConf.attribute_regularizer_id != attribute_config.name and attribute_config.for_regularization:
                self.regularizer_attributes_embeddingsrepo.append(attribute_embeddings)
            if self.modelConf.attribute_regularizer_id == attribute_config.name:
                self.regularizer_id_embeddingsrepo = attribute_embeddings

        self.enable_regularizer = len(self.regularizer_attributes_embeddingsrepo) != 0 and self.regularizer_id_embeddingsrepo is not None

        if self.enable_regularizer:
            print "attribute regularizer enabled"
        else:
            print "attribute regularizer disabled"

    def feed_input(self, feature_names, inputs):
        ranking_attribute_names = [embeddingsrepo.attribute_name for embeddingsrepo in self.ranking_attributes_embeddingsrepo]
        click_features = fetch_features(ranking_attribute_names, CONST.CLICK_COL_PRERFIX, feature_names, inputs)
        postive_features = fetch_features(ranking_attribute_names, CONST.POSITIVE_COL_PREFIX, feature_names, inputs)
        negative_features = fetch_features(ranking_attribute_names, CONST.NEGATIVE_COL_PREFIX, feature_names, inputs)
        if not self.modelConf.use_context:
            click_features = tf.zeros_like(click_features)
        self.click_embedder = ContextClickProductHandler(self.ranking_attributes_embeddingsrepo, click_features, self.model_config)
        self.positive_embedder = ScoringProductHandler(self.ranking_attributes_embeddingsrepo, postive_features)
        self.negative_embedder = ScoringProductHandler(self.ranking_attributes_embeddingsrepo, negative_features)

        click_pooling_methods = {"mean" : self.click_embedder.embeddings_mean, "sum" : self.click_embedder.embeddings_sum}
        self.click_embeddings = click_pooling_methods[self.model_config.click_pooling]
        self.click_embeddings = tf.expand_dims(self.click_embeddings, 1)
        self.positive_weights = self.positive_embedder.weights
        self.positive_bias = self.positive_embedder.bias
        self.negative_weights = self.negative_embedder.weights
        self.negative_bias = self.negative_embedder.bias

        self.context_embedding = self.click_embeddings
        self.batch_size = tf.shape(self.positive_weights)[0]

        self.positive_handler = to_probability(self.model_config, self.positive_weights, self.positive_bias, self.context_embedding, True)
        self.negative_handler = to_probability(self.model_config, self.negative_weights, self.negative_bias, self.context_embedding, False)

        self.head_tail_id = fetch_features([self.model_config.head_tail_id], CONST.POSITIVE_COL_PREFIX, feature_names, inputs)[0]
        self.head_ids = tf.squeeze(tf.cast(tf.less(self.head_tail_id, self.model_config.head_tail_split), tf.float32))
        self.tail_ids = tf.squeeze(tf.cast(tf.greater(self.head_tail_id, self.model_config.head_tail_split), tf.float32))

        self.sigmoid_loss = (tf.reduce_sum(self.positive_handler.xent) + tf.reduce_sum(self.negative_handler.xent))

        if self.enable_regularizer:
            regularizer_attribute_names = [regularizer_embeddings.attribute_name for regularizer_embeddings in self.regularizer_attributes_embeddingsrepo]
            regularizer_id_name = [self.regularizer_id_embeddingsrepo.attribute_name]
            click_features = fetch_features(regularizer_attribute_names, CONST.CLICK_COL_PRERFIX, feature_names, inputs)
            click_id = fetch_features(regularizer_id_name, CONST.CLICK_COL_PRERFIX, feature_names, inputs)[0]

            postive_features = fetch_features(regularizer_attribute_names, CONST.POSITIVE_COL_PREFIX, feature_names, inputs)
            postive_id = fetch_features(regularizer_id_name, CONST.POSITIVE_COL_PREFIX, feature_names, inputs)[0]

            negative_features = fetch_features(regularizer_attribute_names, CONST.NEGATIVE_COL_PREFIX, feature_names, inputs)
            negative_id = fetch_features(regularizer_id_name, CONST.NEGATIVE_COL_PREFIX, feature_names, inputs)[0]

            self.click_regularization = AttributeRegularization(self.regularizer_id_embeddingsrepo, click_id, self.regularizer_attributes_embeddingsrepo, click_features, context_lookup_method)
            self.click_regularization_loss = self.click_regularization.loss
            self.postive_regularization = AttributeRegularization(self.regularizer_id_embeddingsrepo, postive_id, self.regularizer_attributes_embeddingsrepo, postive_features, softmax_lookup_method)
            self.postive_regularization_loss = self.postive_regularization.loss
            self.negative_regularization = AttributeRegularization(self.regularizer_id_embeddingsrepo, negative_id, self.regularizer_attributes_embeddingsrepo, negative_features, softmax_lookup_method)
            self.negative_regularization_loss = self.negative_regularization.loss

            self.attribute_regularization_loss = self.click_regularization_loss + self.postive_regularization_loss + self.negative_regularization_loss
            attribute_regularizer_weight = self.modelConf.attribute_regularizer_weight
            self.sigmoid_loss = self.sigmoid_loss + attribute_regularizer_weight * self.attribute_regularization_loss

        self.sigmoid_loss = self.sigmoid_loss / tf.cast(self.batch_size, tf.float32)

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

        self.head_count = tf.reduce_sum(self.head_ids)
        self.tail_count = tf.reduce_sum(self.tail_ids)

        self.head_prec_1 = tf.reduce_sum(tf.multiply(self.prec_vector, self.head_ids))
        self.tail_prec_1 = tf.reduce_sum(tf.multiply(self.prec_vector, self.tail_ids))

        self.head_mrr = tf.reduce_sum(tf.multiply(self.reciprocal_rank_per_record, self.head_ids))
        self.tail_mrr = tf.reduce_sum(tf.multiply(self.reciprocal_rank_per_record, self.tail_ids))



    def per_record_test_summaries(self):
        return [["perc-1_head", [self.head_prec_1, self.head_count]],
                ["prec-1_tail", [self.tail_prec_1, self.tail_count]],
                ["mean_reciprocal_rank_head", [self.head_mrr, self.head_count]],
                ["mean_reciprocal_rank_tail", [self.tail_mrr, self.tail_count]]]

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

class EmbeddingsRepo :

    def __init__(self, modelConf, attribute_config, id_attribute_config):
        """
        :type modelConf: modelconfig
        :type attribute_config: AttributeConfig
        :type id_attribute_config: AttributeConfig
        """
        self.attribute_config = attribute_config
        self.attribute_name = attribute_config.name
        self.vocab_size = attribute_config.vocab_size
        self.embedding_size = attribute_config.embedding_size
        self.modelConf = modelConf
        override_embedding = attribute_config.override_embeddings

        if override_embedding.context_dict is None:
            self.context_dict = tf.Variable(tf.random_uniform([self.vocab_size, self.embedding_size], -0.001, 0.001),
                                            dtype=tf.float32, name=self.attribute_name + "_context_w")
        else:
            self.context_dict = tf.Variable(override_embedding.context_dict, dtype=tf.float32, name=self.attribute_name + "_context_w")
        if modelConf.reuse_context_dict:
            self.softmax_weights = self.context_dict
        else:
            if override_embedding.softmax_weights is None:
                # init_softmax_weights = tf.random_uniform([self.vocab_size, self.embedding_size], -1, 1)
                init_softmax_weights = tf.zeros([self.vocab_size, self.embedding_size])
                self.softmax_weights = tf.Variable(init_softmax_weights,
                                                   name=self.attribute_name + "_sm_w_t")
            else:
                self.softmax_weights = tf.Variable(override_embedding.softmax_weights, dtype=tf.float32)
        if override_embedding.softmax_bias is None:
            # init_softmax_bias = tf.random_uniform([self.vocab_size], -1, 1)
            init_softmax_bias = tf.zeros([self.vocab_size])
            self.softmax_bias = tf.Variable(init_softmax_bias, name=self.attribute_name + "_sm_b")
        else:
            self.softmax_bias = tf.Variable(override_embedding.softmax_bias, dtype=tf.float32)

        if attribute_config.for_regularization:
            self.context_to_id = tf.Variable(
                tf.random_uniform([id_attribute_config.embedding_size, self.embedding_size], -0.01, 0.01), dtype=tf.float32)

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
    with tf.variable_scope(namespace):
        try :
            return _nn_internal_(layer_count, embeddings, last_out_layer_count, last_no_activation)
        except ValueError:
            tf.get_variable_scope().reuse_variables()
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

class AttributeRegularization:

    def __init__(self, id_embeddingsrepo, ids, attributes_embeddingsrepo, attributes, lookup_method):
        """
        :type id_attributeembeddings: AttributeEmbeddings
        :type ids:
        :type attributes_attributeembeddings: list of AttributeEmbeddings
        :type attributes:
        """
        self.id_embeddingsrepo, self.ids, self.attributes_embeddingsrepo, self.attributes = \
            id_embeddingsrepo, ids, attributes_embeddingsrepo, attributes

        self.id_embeddings = lookup_method(id_embeddingsrepo, ids)
        self.id_embeddings = tf.transpose(self.id_embeddings)
        self.per_attr_loss = []

        for i in range(len(attributes_embeddingsrepo)):
            attribute_embeddingsrepo = attributes_embeddingsrepo[i]
            context_to_id = attribute_embeddingsrepo.context_to_id
            self.attribute_embeddings = lookup_method(attribute_embeddingsrepo, attributes[i])
            self.attribute_embeddings_T = tf.transpose(self.attribute_embeddings)

            self.attribute_embeddings_adjusted = tf.expand_dims(self.attribute_embeddings_T, axis=0)

            context_to_id = tf.expand_dims(context_to_id, axis=-1)
            context_to_id = tf.expand_dims(context_to_id, axis=-1)

            self.attribute_in_id_space = tf.multiply(context_to_id, self.attribute_embeddings_adjusted)
            self.attribute_in_id_space = tf.reduce_sum(self.attribute_in_id_space, reduction_indices=1)

            self.attr_loss_mult = tf.multiply(self.attribute_in_id_space, self.id_embeddings)
            self.attr_loss = tf.reduce_sum(self.attr_loss_mult, reduction_indices=0)
            self.per_attr_loss.append(self.attr_loss)

        self.logits = tf.add_n(self.per_attr_loss)

        self.xent = tf.nn.sigmoid_cross_entropy_with_logits(labels=tf.ones_like(self.logits), logits=self.logits)

        self.loss = tf.reduce_sum(self.xent)


