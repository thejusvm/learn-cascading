import tensorflow as tf
import mind_palace.product_ranker.constants as CONST
import collections

from mind_palace.commons.padding_handler import padding_handler
from mind_palace.product_ranker.models.modelconfig import modelconfig, EmbeddingDicts, AttributeConfig
from model import model

class softmax_model(model) :

    def __init__(self, modelConf):

        self.modelConf = modelConf # type: modelconfig

        model.__init__(self, modelConf)

        product_embeddings = AttributeEmbeddings(modelConf, self.modelConf.attributes_config[0])

        self.context_embedding = product_embeddings.context_embedding
        self.click_input = product_embeddings.click_input

        self.positive_input = product_embeddings.positive_input
        self.positive_weights = product_embeddings.positive_weights
        self.positive_bias = product_embeddings.positive_bias

        self.negative_input = product_embeddings.negative_input
        self.negative_weights = product_embeddings.negative_weights
        self.negative_bias = product_embeddings.negative_bias

        self.batch_size = tf.shape(self.positive_input)[0]

        self.positive_logits = tf.reduce_sum(tf.multiply(self.context_embedding, self.positive_weights), reduction_indices=[2]) + self.positive_bias
        self.positive_xent = tf.nn.sigmoid_cross_entropy_with_logits(labels=tf.ones_like(self.positive_logits), logits=self.positive_logits)

        self.negative_weights_multiply_context = tf.multiply(self.context_embedding, self.negative_weights)
        self.negative_logits = tf.reduce_sum(self.negative_weights_multiply_context, reduction_indices=[2]) + self.negative_bias
        self.negative_xent = tf.nn.sigmoid_cross_entropy_with_logits(labels=tf.zeros_like(self.negative_logits), logits=self.negative_logits)
        self.nce_loss = (tf.reduce_sum(self.positive_xent) + tf.reduce_sum(self.negative_xent)) / tf.cast(self.batch_size, tf.float32)
        self.train_step = tf.train.AdamOptimizer(1e-3).minimize(self.nce_loss)

        self.max_negative_score = tf.reduce_max(self.negative_logits, reduction_indices = [1])
        self.max_negative_score = tf.reshape(self.max_negative_score, [tf.size(self.max_negative_score)])
        self.positive_score_vector = tf.reshape(self.positive_logits, [tf.size(self.positive_logits)])

        self.prec_vector = tf.cast(tf.greater(self.positive_score_vector, self.max_negative_score), tf.float32)
        self.prec_1 = tf.reduce_mean(self.prec_vector)

        self.positive_probability = tf.sigmoid(self.positive_logits)
        self.positive_mean_probability = tf.reduce_mean(self.positive_probability)

        self.placeholders = [self.positive_input, self.negative_input, self.click_input]

    def test_summaries(self):
        return [["prec-1", self.prec_1], ["probability", self.positive_mean_probability]]

    def score(self, products, click_context):
        product_embeddings = AttributeEmbeddings(self.modelConf, self.modelConf.attributes_config[0], self.override_embedding_dicts)
        positive_weights = tf.nn.embedding_lookup(product_embeddings.softmax_weights, products)
        positive_bias = tf.nn.embedding_lookup(product_embeddings.softmax_bias, products)
        click_padder = padding_handler(click_context, product_embeddings.context_dict)
        click_embeddings_mean = click_padder.tensor_embeddings_mean
        positive_logits = tf.reduce_sum(tf.multiply(click_embeddings_mean, positive_weights), reduction_indices=[2]) + positive_bias
        positive_xent = tf.nn.sigmoid_cross_entropy_with_logits(labels=tf.ones_like(positive_logits), logits=positive_logits)
        return positive_xent

    def place_holders(self):
        return self.placeholders

    def loss(self):
        return self.nce_loss

    def minimize_step(self):
        return self.train_step



class AttributeEmbeddings :

    def __init__(self, modelConf, attribute_config):
        """
        :type modelConf: modelconfig
        :type attribute_config: AttributeConfig
        """

        default_click_index = CONST.DEFAULT_DICT_KEYS.index(CONST.DEFAULT_CLICK_TEXT)
        self.attribute_config = attribute_config
        self.attribute_name = attribute_config.name
        self.vocab_size = attribute_config.vocab_size
        self.embedding_size = attribute_config.embedding_size
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
                self.softmax_weights = tf.Variable(tf.random_uniform([self.vocab_size, self.embedding_size], -1, 1),
                                                   name="sm_w_t")
            else:
                self.softmax_weights = tf.Variable(override_embedding.softmax_weights, dtype=tf.float32)
        if override_embedding.softmax_bias is None:
            self.softmax_bias = tf.Variable(tf.random_uniform([self.vocab_size], -1, 1), name="sm_b")
        else:
            self.softmax_bias = tf.Variable(override_embedding.softmax_bias, dtype=tf.float32)
        self.click_input = tf.placeholder(tf.int32, shape=[None, None])
        self.click_context_samples = self.click_input
        if modelConf.enable_default_click:
            # Adding a dummy click product to each of the list of clicks
            self.num_click_context_samples = tf.shape(self.click_context_samples)[0]
            self._default_click_pad = tf.reshape(
                tf.tile([default_click_index], [self.num_click_context_samples]),
                [self.num_click_context_samples, 1])
            self.click_context_samples_padded = tf.concat([self._default_click_pad, self.click_context_samples], 1)
        else:
            self.click_context_samples_padded = self.click_context_samples
        self.click_padder = padding_handler(self.click_context_samples_padded, self.context_dict)
        if modelConf.use_context is False:
            if default_click_index is not None:
                self.click_embeddings_mean = tf.nn.embedding_lookup(self.context_dict,
                                                                    [default_click_index])
            else:
                self.click_embeddings_mean = None
        else:
            self.click_embeddings_mean = self.click_padder.tensor_embeddings_mean
        self.click_embeddings_mean = tf.expand_dims(self.click_embeddings_mean, 1)

        self.context_embedding = self.click_embeddings_mean


        self.positive_input = tf.placeholder(tf.int32, shape=[None], name="positive_samples")
        self.positive_samples = tf.expand_dims(self.positive_input, [1])
        self.positive_weights = tf.nn.embedding_lookup(self.softmax_weights, self.positive_samples)
        self.positive_bias = tf.nn.embedding_lookup(self.softmax_bias, self.positive_samples)

        self.negative_input = tf.placeholder(tf.int32, shape=[None, None], name="negative_samples")
        self.negative_samples = self.negative_input
        self.negative_weights = tf.nn.embedding_lookup(self.softmax_weights, self.negative_samples)
        self.negative_bias = tf.nn.embedding_lookup(self.softmax_bias, self.negative_samples)




