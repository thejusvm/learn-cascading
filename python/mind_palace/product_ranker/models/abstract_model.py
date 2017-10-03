from model import model
import tensorflow as tf

class abstract_model(model) :

    def __init__(self, modelConf, init_embedding_dict = None):
        model.__init__(self, modelConf)

        if init_embedding_dict is None :
            self.embeddings_dict = tf.Variable(tf.random_uniform([modelConf.vocabulary_size, modelConf.embedding_size], 1.0, 2.0), dtype= tf.float32)
        else:
            self.embeddings_dict = tf.Variable(modelConf.init_embedding, dtype = tf.float32)

        self.positive_samples = tf.placeholder(tf.int32, shape=[None, 1], name="positive_samples")
        self.negative_samples = tf.placeholder(tf.int32, shape=[None, None], name="negative_samples")

        self.click_context_samples = tf.placeholder(tf.int32, shape=[None, None])
        self.click_context_mask = tf.greater(self.click_context_samples, 0)
        self.click_context_mask = tf.cast(self.click_context_mask, tf.float32)
        self.click_context_mask = tf.expand_dims(self.click_context_mask, 2)

        self.positive_embeddings = tf.nn.embedding_lookup(self.embeddings_dict, self.positive_samples)
        self.negative_embeddings = tf.nn.embedding_lookup(self.embeddings_dict, self.negative_samples)
        self.click_embeddings_pre_pad = tf.nn.embedding_lookup(self.embeddings_dict, self.click_context_samples)
        self.click_embeddings = tf.multiply(self.click_embeddings_pre_pad, self.click_context_mask)

        self.num_non_pad = tf.reduce_sum(self.click_context_mask, reduction_indices = [1])
        self.num_non_pad_zero_mask = tf.cast(tf.equal(self.num_non_pad, 0), tf.float32)
        self.num_non_pad = self.num_non_pad + self.num_non_pad_zero_mask
        self.click_embeddings_mean = tf.reduce_sum(self.click_embeddings, reduction_indices = [1]) / self.num_non_pad

        if modelConf.use_context is False :
            self.click_embeddings_mean = None

    def poistive_label(self):
        return self.positive_samples

    def negative_label(self):
        return self.negative_samples

    def click_product_label(self):
        return self.click_context_samples

    def embedding_dict(self):
        return self.embeddings_dict
