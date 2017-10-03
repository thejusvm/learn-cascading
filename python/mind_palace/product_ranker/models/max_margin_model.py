import math
import tensorflow as tf

from mind_palace.product_ranker.models.model import model


def embedding_concat(embeddings, context = None) :
    if context is None :
        return embeddings
    else :
        embedding_shape = tf.shape(embeddings)
        embedding_size = tf.size(embeddings)
        repeate_count = embedding_size / (embedding_shape[0] * embedding_shape[2])
        context_rep = tf.tile(context, [1, repeate_count])
        context_rep_reshape = tf.reshape(context_rep, embedding_shape)
        return tf.concat([embeddings, context_rep_reshape], 2)

def _nn_internal_(modelConf, embeddings, context = None) :

    input_embedding = embedding_concat(embeddings, context)
    lastdim = input_embedding.get_shape().ndims - 1

    layer_in_count = input_embedding.get_shape()[lastdim].value
    in_layer = input_embedding
    out_layer = None

    counter = 0
    for num in modelConf.layer_count :
        counter = counter + 1
        layer_out_count = num
        stddev = math.sqrt(2.0 / (layer_in_count + layer_out_count))
        kernal_initial = tf.truncated_normal_initializer(0, stddev)
        out_layer = tf.layers.dense(inputs = in_layer,
                                   units = num,
                                   activation=tf.nn.relu,
                                   kernel_initializer =  kernal_initial,
                                   bias_initializer = tf.constant_initializer(10),
                                   name = "layer_" + str(counter))

        in_layer = out_layer
        layer_in_count = layer_out_count


    final_layer_initializer= tf.truncated_normal_initializer(0, 1)
    counter = counter + 1
    return tf.layers.dense(inputs = in_layer, units = 1, activation= None, name = "layer_" + str(counter),
                           kernel_initializer =  final_layer_initializer,
                           bias_initializer = tf.constant_initializer(1),
                           # kernel_regularizer = tf.contrib.layers.l2_regularizer(scale=0.1)
                        ), input_embedding

def nn(modelConf, embeddings, context = None) :
    with tf.variable_scope("discriminator"):
        try :
            return _nn_internal_(modelConf, embeddings, context)
        except ValueError:
            tf.get_variable_scope().reuse_variables()
            return _nn_internal_(modelConf, embeddings, context)

class max_margin_model(model) :

    def __init__(self, modelConf) :

        model.__init__(self, modelConf)

        if modelConf.init_embedding_dict is None :
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

        self.positive_score, self.positive_and_context = nn(modelConf, self.positive_embeddings, self.click_embeddings_mean)
        self.negative_score, self.negative_and_context = nn(modelConf, self.negative_embeddings, self.click_embeddings_mean)

        self.loss_matrix = tf.maximum(0., 1. - self.positive_score + self.negative_score)
        self.avg_loss = tf.reduce_mean(self.loss_matrix)

        self.accuracy = tf.count_nonzero(self.loss_matrix, reduction_indices=[1])
        self.accuracy = tf.cast(self.accuracy, tf.float32)
        self.accuracy = tf.reduce_mean(self.accuracy)

        self.max_negative_score = tf.reduce_max(self.negative_score, reduction_indices = [1])
        self.max_negative_score = tf.reshape(self.max_negative_score, [tf.size(self.max_negative_score)])
        self.positive_score_vector = tf.reshape(self.positive_score, [tf.size(self.positive_score)])

        self.prec_vector = tf.cast(tf.greater(self.positive_score_vector, self.max_negative_score), tf.float32)
        self.prec_1 = tf.reduce_mean(self.prec_vector)

        self.train_step = tf.train.AdamOptimizer(1e-3).minimize(self.avg_loss)

    def minimize_step(self):
        return self.train_step

    def loss(self):
        return self.avg_loss

    def poistive_label(self):
        return self.positive_samples

    def negative_label(self):
        return self.negative_samples

    def click_product_label(self):
        return self.click_context_samples

    def embedding_dict(self):
        return self.embeddings_dict

    def test_summaries(self):
        return [["accuracy", self.accuracy],
                ["prec-1", self.prec_1]]

    def score(self, products, click_context):
        i_embedding = tf.nn.embedding_lookup(self.embeddings_dict, products)
        return nn(self.model_config, i_embedding, None)[0]




