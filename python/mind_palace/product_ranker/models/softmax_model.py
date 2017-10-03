import tensorflow as tf
from mind_palace.product_ranker.modelconfig import modelconfig
from model import model
from padding_handler import padding_handler

class softmax_model(model) :

    def __init__(self, modelConf, context_dict = None, softmax_weights = None, softmax_bias = None):

        self.modelConf = modelConf # type: modelconfig

        model.__init__(self, modelConf)


        if context_dict is None :
            self.context_dict = tf.Variable(tf.random_uniform([modelConf.vocabulary_size, modelConf.embedding_size], 1.0, 2.0), dtype= tf.float32)
        else:
            self.context_dict = tf.Variable(context_dict, dtype = tf.float32)

        if softmax_weights is None :
            self.softmax_weights = tf.Variable(tf.zeros([self.modelConf.vocabulary_size, self.modelConf.embedding_size]), name="sm_w_t")
        else:
            self.softmax_weights = tf.Variable(softmax_weights, dtype = tf.float32)

        if softmax_bias is None :
            self.softmax_bias = tf.Variable(tf.zeros([self.modelConf.vocabulary_size]), name="sm_b")
        else:
            self.softmax_bias = tf.Variable(softmax_bias, dtype = tf.float32)

        self.click_context_samples = tf.placeholder(tf.int32, shape=[None, None])
        self.click_padder = padding_handler(self.click_context_samples, self.context_dict)
        self.click_embeddings_mean = self.click_padder.click_embeddings_mean

        self.batch_size = tf.shape(self.positive_samples)[0]

        if modelConf.use_context is False :
            self.click_embeddings_mean = None

        self.positive_samples = tf.placeholder(tf.int32, shape=[None, 1], name="positive_samples")
        self.positive_weights = tf.nn.embedding_lookup(self.softmax_weights, self.positive_samples)
        self.positive_bias = tf.nn.embedding_lookup(self.softmax_bias, self.positive_samples)
        self.positive_logits = tf.matmul(self.click_embeddings_mean, self.positive_weights, transpose_b=True) + self.positive_bias
        self.positive_xent = tf.nn.sigmoid_cross_entropy_with_logits(labels=tf.ones_like(self.positive_logits), logits=self.positive_logits)

        self.negative_samples = tf.placeholder(tf.int32, shape=[None, None], name="negative_samples")
        self.negative_weights = tf.nn.embedding_lookup(self.softmax_weights, self.negative_samples)
        self.negative_bias = tf.nn.embedding_lookup(self.softmax_bias, self.negative_samples)
        self.negative_logits = tf.matmul(self.click_embeddings_mean, self.negative_weights, transpose_b=True) + self.negative_bias
        self.negative_xent = tf.nn.sigmoid_cross_entropy_with_logits(labels=tf.zeros_like(self.negative_logits), logits=self.negative_logits)

        self.nce_loss = (tf.reduce_sum(self.positive_xent) + tf.reduce_sum(self.negative_xent)) / self.batch_size

        self.train_step = tf.train.AdamOptimizer(1e-3).minimize(self.nce_loss)


    def click_product_label(self):
        return self.click_context_samples

    def test_summaries(self):
        return []

    def score(self, products, click_context):
        positive_weights = tf.nn.embedding_lookup(self.softmax_weights, products)
        positive_bias = tf.nn.embedding_lookup(self.softmax_bias, products)
        click_padder = padding_handler(click_context, self.context_dict)
        click_embeddings_mean = click_padder.click_embeddings_mean
        positive_logits = tf.matmul(click_embeddings_mean, positive_weights, transpose_b=True) + positive_bias
        positive_xent = tf.nn.sigmoid_cross_entropy_with_logits(labels=tf.ones_like(positive_logits), logits=positive_logits)
        return positive_xent

    def loss(self):
        return self.nce_loss

    def negative_label(self):
        return self.negative_samples

    def poistive_label(self):
        return self.positive_samples

    def minimize_step(self):
        model.minimize_step(self)



