import tensorflow as tf

from mind_palace.commons.padding_handler import padding_handler
from mind_palace.product_ranker.models.modelconfig import modelconfig, AttributeConfig
from model import model
from mind_palace.product_ranker.commons import generate_feature_names
import mind_palace.product_ranker.constants as CONST

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
        self.placeholders = []
        self.inputs = inputs
        self.per_attribute_positive_weights = []
        self.per_attribute_positive_bias = []
        self.per_attribute_negative_weights = []
        self.per_attribute_negative_bias = []
        self.per_attribute_click_embedding = []
        for i in range(len(self.per_attribute_embeddings)) :
            attribute_embeddings = self.per_attribute_embeddings[i]
            attribute_name = self.attributes_config[i].name
            attribute_feature_names = generate_feature_names([attribute_name], CONST.TRAINING_COL_PREFIXES)
            attribute_feature_indices = [feature_names.index(i) for i in attribute_feature_names]
            inputs_for_attribute = [inputs[i] for i in attribute_feature_indices]
            attribute_embeddings.feed_input(inputs_for_attribute)
            self.per_attribute_positive_weights.append(attribute_embeddings.positive_weights)
            self.per_attribute_positive_bias.append(attribute_embeddings.positive_bias)

            self.per_attribute_negative_weights.append(attribute_embeddings.negative_weights)
            self.per_attribute_negative_bias.append(attribute_embeddings.negative_bias)

            self.per_attribute_click_embedding.append(attribute_embeddings.context_embedding)

            self.placeholders += [attribute_embeddings.positive_input,
                                  attribute_embeddings.negative_input,
                                  attribute_embeddings.click_input]

        self.click_embeddings = tf.concat(self.per_attribute_click_embedding, 2)
        self.positive_weights = tf.concat(self.per_attribute_positive_weights, 2)
        self.positive_bias = tf.concat(self.per_attribute_positive_bias, 1)
        self.negative_weights = tf.concat(self.per_attribute_negative_weights, 2)
        self.negative_bias = tf.concat(self.per_attribute_negative_bias, 1)

        self.context_embedding = self.click_embeddings
        self.batch_size = tf.shape(self.positive_weights)[0]

        self.positive_sigmoid = sigmoid(self.positive_weights, self.positive_bias, self.context_embedding, True)
        self.negative_sigmoid = sigmoid(self.negative_weights, self.negative_bias, self.context_embedding, False)

        self.sigmoid_loss = (tf.reduce_sum(self.positive_sigmoid.xent) + tf.reduce_sum(self.negative_sigmoid.xent)) / tf.cast(self.batch_size, tf.float32)

        # self.train_step = tf.train.AdamOptimizer(self.modelConf.learning_rate).minimize(self.sigmoid_loss)
        self.train_step = tf.train.GradientDescentOptimizer(self.modelConf.learning_rate).minimize(self.sigmoid_loss)


        self.max_negative_score = tf.reduce_max(self.negative_sigmoid.logits, reduction_indices = [1])
        self.max_negative_score = tf.reshape(self.max_negative_score, [tf.size(self.max_negative_score)])

        self.positive_score_vector = tf.reshape(self.positive_sigmoid.logits, [tf.size(self.positive_sigmoid.logits)])
        self.prec_vector = tf.cast(tf.greater(self.positive_score_vector, self.max_negative_score), tf.float32)
        self.prec_1 = tf.reduce_mean(self.prec_vector)

        self.less_than = tf.cast(tf.less(self.positive_sigmoid.logits, self.negative_sigmoid.logits), tf.float32)
        self.rank_per_record = tf.reduce_sum(self.less_than, reduction_indices = [1])
        self.rank_per_record = self.rank_per_record + 1
        self.mean_rank = tf.reduce_mean(self.rank_per_record)

        self.reciprocal_rank_per_record = 1 / tf.cast(self.rank_per_record, tf.float32)
        self.mean_reciprocal_rank = tf.reduce_mean(self.reciprocal_rank_per_record)


        self.positive_probability = tf.sigmoid(self.positive_sigmoid.logits)
        self.positive_mean_probability = tf.reduce_mean(self.positive_probability)


    def test_summaries(self):
        return [["loss", self.sigmoid_loss],
                ["prec-1", self.prec_1],
                ["probability", self.positive_mean_probability],
                ["mean_rank", self.mean_rank],
                ["mean_reciprocal_rank", self.mean_reciprocal_rank]]

    def score(self):
        return [["score", self.positive_score_vector],
                ["bias", self.positive_sigmoid.modified_bias],
                ["wTx", self.positive_sigmoid.weights_cross_context_sum],
                ["xent", self.positive_sigmoid.xent],
                ["probability", self.positive_probability]]

    def place_holders(self):
        return self.placeholders

    def loss(self):
        return self.sigmoid_loss

    def minimize_step(self):
        return self.train_step

class sigmoid :
    def __init__(self, weights, bias, context_embedding, positive = True):
        self.weights = weights
        self.bias = bias
        self.context_embedding = context_embedding
        self.postive = positive
        self.modified_bias = tf.reduce_sum(self.bias, reduction_indices=[1])
        self.weights_cross_context = tf.multiply(self.context_embedding, self.weights)
        self.weights_cross_context_sum = tf.reduce_sum(self.weights_cross_context, reduction_indices=[2])
        self.logits = self.weights_cross_context_sum + self.modified_bias
        if positive :
            labels = tf.ones_like(self.logits)
        else :
            labels = tf.zeros_like(self.logits)
        self.xent = tf.nn.sigmoid_cross_entropy_with_logits(labels=labels, logits=self.logits)

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

    def feed_input(self, inputs):
        if inputs is None:
            self.positive_input = tf.placeholder(tf.int32, shape=[None, 1], name = self.attribute_name + "_positive_input")
            self.negative_input = tf.placeholder(tf.int32, shape=[None, None], name = self.attribute_name + "_negative_input")
            self.click_input = tf.placeholder(tf.int32, shape=[None, None], name = self.attribute_name + "_click_input")
        else :
            self.positive_input, self.negative_input, self.click_input = inputs

        self.click_context_samples = self.click_input
        self.click_context_samples_padded = self.click_context_samples
        self.click_padder = padding_handler(self.click_context_samples_padded, self.context_dict)
        if self.modelConf.use_context is False:
            self.click_embeddings_mean = None
        else:
            self.click_embeddings_mean = self.click_padder.tensor_embeddings_mean
        self.click_embeddings_mean = tf.expand_dims(self.click_embeddings_mean, 1)

        self.context_embedding = self.click_embeddings_mean

        self.positive_samples = self.positive_input
        self.positive_weights = tf.nn.embedding_lookup(self.softmax_weights, self.positive_samples)
        self.positive_bias = tf.nn.embedding_lookup(self.softmax_bias, self.positive_samples)
        self.positive_bias = tf.expand_dims(self.positive_bias, [1])

        self.negative_samples = self.negative_input
        self.negative_weights = tf.nn.embedding_lookup(self.softmax_weights, self.negative_samples)
        self.negative_bias = tf.nn.embedding_lookup(self.softmax_bias, self.negative_samples)
        self.negative_bias = tf.expand_dims(self.negative_bias, [1])




