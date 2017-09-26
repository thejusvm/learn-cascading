import tensorflow as tf
import numpy as np

def _nn_internal_(embeddings, ifreuse, context = None) :

    if context is None :
        input_embedding = embeddings
    else :
        embedding_shape = tf.shape(embeddings)
        embedding_size = tf.size(embeddings)
        repeate_count = embedding_size / (embedding_shape[0] * embedding_shape[2])
        context_rep = tf.tile(context, [1, repeate_count])
        context_rep_reshape = tf.reshape(context_rep, embedding_shape)
        input_embedding = tf.concat([embeddings, context_rep_reshape], 2)

    dense_v1 = tf.layers.dense(inputs=input_embedding,
                               units=256,
                               activation=tf.nn.relu,
                               kernel_initializer =  tf.random_uniform_initializer(0, 1, seed = None),
                               bias_initializer = tf.constant_initializer(100),
                               # kernel_regularizer = tf.contrib.layers.l2_regularizer(scale=0.1),
                               name = "layer1",
                               reuse=ifreuse)
    # dense_v2 = tf.layers.dense(inputs=dense_v1,
    #                            units=256,
    #                            activation=tf.nn.relu,
    #                            kernel_initializer =  tf.random_uniform_initializer(0, 1, seed = None),
    #                            bias_initializer = tf.constant_initializer(100),
    #                            # kernel_regularizer = tf.contrib.layers.l2_regularizer(scale=0.1),
    #                            name = "layer1.5",
    #                            reuse=ifreuse)
    return tf.layers.dense(inputs=dense_v1, units=1, activation=tf.nn.relu, name = "layer2",
                           kernel_initializer =  tf.random_uniform_initializer(0, 1),
                           bias_initializer = tf.constant_initializer(10),
                           # kernel_regularizer = tf.contrib.layers.l2_regularizer(scale=0.1),
                           reuse=ifreuse)

def nn(embeddings, context = None) :
    with tf.variable_scope("discriminator"):
        try :
            return _nn_internal_(embeddings, False, context)
        except ValueError:
            return _nn_internal_(embeddings, True, context)

class model :

    def __init__(self, vocabulary_size, embedding_size,
                 init_embedding = None,
                 num_negative_samples = 20,
                 num_click_context = 32,
                 pad_index = 0,
                 use_context = True) :

        self.vocabulary_size = vocabulary_size
        self.embedding_size = embedding_size
        self.num_negative_samples = num_negative_samples
        self.num_click_context = num_click_context
        self.pad_index = pad_index

        if init_embedding is None :
            self.embeddings_dict = tf.Variable(tf.random_uniform([vocabulary_size, embedding_size], 1.0, 2.0), dtype= tf.float32)
        else:
            self.embeddings_dict = tf.Variable(init_embedding, dtype = tf.float32)

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

        if use_context is False :
            self.click_embeddings_mean = None

        self.positive_score = nn(self.positive_embeddings, self.click_embeddings_mean)
        self.negative_score = nn(self.negative_embeddings, self.click_embeddings_mean)

        self.loss_matrix = tf.maximum(0., 1. - self.positive_score + self.negative_score)
        self.loss = tf.reduce_mean(self.loss_matrix)

        self.accuracy = tf.count_nonzero(self.loss_matrix, reduction_indices=[1])
        self.accuracy = tf.cast(self.accuracy, tf.float32)
        self.accuracy = tf.reduce_mean(self.accuracy)

        self.max_negative_score = tf.reduce_max(self.negative_score, reduction_indices = [1])
        self.max_negative_score = tf.reshape(self.max_negative_score, [tf.size(self.max_negative_score)])
        self.positive_score_vector = tf.reshape(self.positive_score, [tf.size(self.positive_score)])

        self.prec_vector = tf.cast(tf.greater(self.positive_score_vector, self.max_negative_score), tf.float32)
        self.prec_1 = tf.reduce_mean(self.prec_vector)

        self.train_step = tf.train.AdamOptimizer(1e-3).minimize(self.loss)
