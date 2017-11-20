from model import model
import mind_palace.product_ranker.constants as CONST
import tensorflow as tf
from mind_palace.product_ranker.models.modelconfig import modelconfig, AttributeConfig

class glove(model):

    def __init__(self, model_config):
        """
        :type model_config: modelconfig
        """
        model.__init__(self, model_config)

        self.model_config = model_config
        attr_config = model_config.attributes_config #type:AttributeConfig

        if attr_config.override_embeddings.context_dict is not None :
            init_focal = attr_config.override_embeddings.context_dict
        else :
            init_focal = tf.random_uniform([attr_config.vocab_size, attr_config.embedding_size], 1.0, -1.0)

        self.input_specific_learning_rate = tf.constant(attr_config.per_attribute_learning_rate)
        self.focal_embeddings = tf.Variable(
            init_focal,
            name="focal_embeddings")
        # init_context = tf.random_uniform([attr_config.vocab_size, attr_config.embedding_size], 1.0, -1.0)
        # init_context = tf.zeros([attr_config.vocab_size, attr_config.embedding_size])
        # self.context_embeddings = tf.Variable(
        #     init_context,
        #     name="context_embeddings")

        # self.focal_biases = tf.Variable(tf.random_uniform([attr_config.vocab_size], 1.0, -1.0),
        #                            name='focal_biases')
        # self.context_biases = tf.Variable(tf.random_uniform([attr_config.vocab_size], 1.0, -1.0),
        #                              name="context_biases")


    def feed_input(self, feature_names, inputs):
        self.__focal_input, self.__context_input, self.__cooccurrence_count = inputs
        self.focal_embedding = tf.nn.embedding_lookup([self.focal_embeddings], self.__focal_input)
        self.context_embedding = tf.nn.embedding_lookup([self.focal_embeddings], self.__context_input)

        # self.focal_bias = tf.nn.embedding_lookup([self.focal_biases], self.__focal_input)
        # self.context_bias = tf.nn.embedding_lookup([self.context_biases], self.__context_input)

        self.embedding_product = tf.reduce_sum(tf.multiply(self.focal_embedding, self.context_embedding), 1)

        self.log_cooccurrences = tf.to_float(self.__cooccurrence_count)

        self.score_val = self.embedding_product #+ self.focal_bias + self.context_bias
        self.single_losses = tf.square(self.score_val - self.log_cooccurrences)
        self.__total_loss = tf.reduce_sum(self.single_losses)
        # self.__combined_embeddings = tf.add(self.focal_embeddings, self.context_embeddings,
        #                                     name="combined_embeddings")

    def place_holders(self):
        """:return the list of placeholders in attach data to"""
        pass

    def loss(self):
        return self.__total_loss

    def assign(self, embeddings, gradient):
        self.gradient = gradient
        self.gradient__1 = self.model_config.learning_rate * gradient.values

        self.indices = gradient.indices
        self.input_specific_learning_rate_for_indices = tf.nn.embedding_lookup(self.input_specific_learning_rate, self.indices)

        self.input_specific_learning_rate_for_indices = tf.expand_dims(self.input_specific_learning_rate_for_indices, 1)
        self.gradient__2 = tf.multiply(self.gradient__1, self.input_specific_learning_rate_for_indices)

        update_embeddings = tf.scatter_sub(embeddings, self.indices, self.gradient__2)

        return update_embeddings

    def minimize_step(self):
        # trainstep = tf.train.AdamOptimizer(self.model_config.learning_rate).minimize(self.__total_loss)
        self.grad_focal_emb = tf.gradients(xs=self.focal_embeddings, ys=self.__total_loss)
        self.new_focal_emb = self.assign(self.focal_embeddings, self.grad_focal_emb[0])

        return self.new_focal_emb


    def score(self):
        return [self.score_val, self.log_cooccurrences]

    def test_summaries(self):
        """ :return an nx2 array with first column as the summary name and second column as the tensor for the summary"""
        pass

