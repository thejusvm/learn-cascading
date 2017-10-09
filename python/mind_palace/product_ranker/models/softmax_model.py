import tensorflow as tf
from mind_palace.product_ranker.modelconfig import modelconfig
from model import model
from padding_handler import padding_handler

class softmax_model(model) :

    def __init__(self, modelConf, context_dict = None, softmax_weights = None, softmax_bias = None):

        self.modelConf = modelConf # type: modelconfig

        model.__init__(self, modelConf)


        if context_dict is None :
            self.context_dict = tf.Variable(tf.random_uniform([modelConf.vocabulary_size, modelConf.embedding_size], -1.0, 1.0), dtype= tf.float32)
        else:
            self.context_dict = tf.Variable(context_dict, dtype = tf.float32)

        if self.modelConf.reuse_context_dict :
            self.softmax_weights = self.context_dict
        else:
            if softmax_weights is None :
                self.softmax_weights = tf.Variable(tf.random_uniform([self.modelConf.vocabulary_size, self.modelConf.embedding_size], -1, 1), name="sm_w_t")
            else:
                self.softmax_weights = tf.Variable(softmax_weights, dtype = tf.float32)

        if softmax_bias is None :
            self.softmax_bias = tf.Variable(tf.random_uniform([self.modelConf.vocabulary_size], -1, 1), name="sm_b")
        else:
            self.softmax_bias = tf.Variable(softmax_bias, dtype = tf.float32)

        self.click_context_samples = tf.placeholder(tf.int32, shape=[None, None])
        if self.modelConf.default_click_index is not None :
            # Adding a dummy click product to each of the list of clicks
            self.num_click_context_samples = tf.shape(self.click_context_samples)[0]
            self._default_click_pad = tf.reshape(tf.tile([self.modelConf.default_click_index], [self.num_click_context_samples]), [self.num_click_context_samples, 1])
            self.click_context_samples_padded = tf.concat([self._default_click_pad, self.click_context_samples], 1)
        else:
            self.click_context_samples_padded = self.click_context_samples
        self.click_padder = padding_handler(self.click_context_samples_padded, self.context_dict)

        if modelConf.use_context is False :
            if self.modelConf.default_click_index is not None :
                self.click_embeddings_mean = tf.nn.embedding_lookup(self.context_dict, [self.modelConf.default_click_index])
            else :
                self.click_embeddings_mean = None
        else :
            self.click_embeddings_mean = self.click_padder.tensor_embeddings_mean

        self.click_embeddings_mean = tf.expand_dims(self.click_embeddings_mean, 1)

        self.positive_samples_input = tf.placeholder(tf.int32, shape=[None], name="positive_samples")
        self.positive_samples = tf.expand_dims(self.positive_samples_input, [1])
        self.batch_size = tf.shape(self.positive_samples)[0]
        self.positive_weights = tf.nn.embedding_lookup(self.softmax_weights, self.positive_samples)
        self.positive_bias = tf.nn.embedding_lookup(self.softmax_bias, self.positive_samples)
        self.positive_logits = tf.reduce_sum(tf.multiply(self.click_embeddings_mean, self.positive_weights), reduction_indices=[2]) + self.positive_bias
        self.positive_xent = tf.nn.sigmoid_cross_entropy_with_logits(labels=tf.ones_like(self.positive_logits), logits=self.positive_logits)

        self.negative_samples = tf.placeholder(tf.int32, shape=[None, None], name="negative_samples")
        self.negative_weights = tf.nn.embedding_lookup(self.softmax_weights, self.negative_samples)
        self.negative_bias = tf.nn.embedding_lookup(self.softmax_bias, self.negative_samples)
        self.negative_weights_multiply_context = tf.multiply(self.click_embeddings_mean, self.negative_weights)
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

    def test_summaries(self):
        return [["prec-1", self.prec_1], ["probability", self.positive_mean_probability]]

    def score(self, products, click_context):
        positive_weights = tf.nn.embedding_lookup(self.softmax_weights, products)
        positive_bias = tf.nn.embedding_lookup(self.softmax_bias, products)
        click_padder = padding_handler(click_context, self.context_dict)
        click_embeddings_mean = click_padder.tensor_embeddings_mean
        positive_logits = tf.reduce_sum(tf.multiply(click_embeddings_mean, positive_weights), reduction_indices=[2]) + positive_bias
        positive_xent = tf.nn.sigmoid_cross_entropy_with_logits(labels=tf.ones_like(positive_logits), logits=positive_logits)
        return positive_xent

    def place_holders(self):
        return [self.positive_samples_input, self.negative_samples, self.click_context_samples]

    def loss(self):
        return self.nce_loss

    def minimize_step(self):
        return self.train_step



