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


def getmodel (vocabulary_size, embedding_size):
    embeddings_dict = tf.Variable(tf.random_uniform([vocabulary_size, embedding_size], 0.0, 1.0))

    positive_samples = tf.placeholder(tf.int32, shape=[None, 1], name="positive_samples")
    negative_samples = tf.placeholder(tf.int32, shape=[None, None], name="negative_samples")

    click_context = tf.placeholder(tf.int32, shape=[None, None])

    positive_embeddings = tf.nn.embedding_lookup(embeddings_dict, positive_samples)
    negative_embeddings = tf.nn.embedding_lookup(embeddings_dict, negative_samples)

    positive_score = nn(positive_embeddings)
    negative_score = nn(negative_embeddings)

    loss_matrix = tf.maximum(0., 1. - positive_score + negative_score)
    loss = tf.reduce_sum(loss_matrix) #+ tf.reduce_sum(positive_score) + tf.reduce_sum(negative_score)
    train_step = tf.train.AdamOptimizer(1e-3).minimize(loss)

    return positive_samples, negative_samples, embeddings_dict, loss, train_step