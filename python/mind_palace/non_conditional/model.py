import tensorflow as tf

def _nn_internal_(embeddings, ifreuse) :
    dense_v1 = tf.layers.dense(inputs=embeddings,
                               units=100,
                               activation=tf.nn.relu,
                               kernel_initializer =  tf.random_uniform_initializer(1, 3),
                               bias_initializer = tf.random_uniform_initializer(1, 3),
                               kernel_regularizer = tf.contrib.layers.l2_regularizer(scale=0.1),
                               name = "layer1",
                               reuse=ifreuse)
    return tf.layers.dense(inputs=dense_v1, units=1, activation=tf.nn.relu, name = "layer2",
                           kernel_initializer =  tf.random_uniform_initializer(1, 3),
                           bias_initializer = tf.random_uniform_initializer(1, 3),
                           kernel_regularizer = tf.contrib.layers.l2_regularizer(scale=0.1),
                           reuse=ifreuse)

def nn(embeddings) :
    with tf.variable_scope("discriminator"):
        try :
            return _nn_internal_(embeddings, False)
        except ValueError:
            return _nn_internal_(embeddings, True)



vocabulary_size = 10
embedding_size = 10

class model :

    def __init__(self, vocabulary_size, embedding_size) :

        self.vocabulary_size = vocabulary_size
        self.embedding_size = embedding_size

        self.embeddings_dict = tf.Variable(tf.random_uniform([vocabulary_size, embedding_size], 0.0, 1.0))

        self.positive_samples = tf.placeholder(tf.int32, shape=[None, 1], name="positive_samples")
        self.negative_samples = tf.placeholder(tf.int32, shape=[None, None], name="negative_samples")

        click_context = tf.placeholder(tf.int32, shape=[None, None])

        self.positive_embeddings = tf.nn.embedding_lookup(self.embeddings_dict, self.positive_samples)
        self.negative_embeddings = tf.nn.embedding_lookup(self.embeddings_dict, self.negative_samples)

        self.positive_score = nn(self.positive_embeddings)
        self.negative_score = nn(self.negative_embeddings)

        self.loss_matrix = tf.maximum(0., 1. - self.positive_score + self.negative_score)
        self.loss = tf.reduce_sum(self.loss_matrix) #+ tf.reduce_sum(positive_score) + tf.reduce_sum(negative_score)

        self.accuracy = tf.cast(tf.count_nonzero(self.loss_matrix), tf.float32) / tf.cast(tf.size(self.loss_matrix), tf.float32)

        self.max_negative_score = tf.reduce_max(self.negative_score, reduction_indices = [1])
        self.max_negative_score = tf.reshape(self.max_negative_score, [tf.size(self.max_negative_score)])
        self.positive_score_vector = tf.reshape(self.positive_score, [tf.size(self.positive_score)])

        prec_vector = tf.cast(tf.greater(self.positive_score_vector, self.max_negative_score), tf.uint8)
        self.prec_1 = tf.cast(tf.reduce_sum(prec_vector), tf.float32) / tf.cast(tf.size(prec_vector), tf.float32)

        self.train_step = tf.train.AdamOptimizer(1e-3).minimize(self.loss)
