import tensorflow as tf


class padding_handler :

    def __init__(self, click_context_samples, embeddings_dict):
        self._click_context_mask = tf.greater(click_context_samples, 0)
        self._click_context_mask = tf.cast(self._click_context_mask, tf.float32)
        self._click_context_mask = tf.expand_dims(self._click_context_mask, 2)
        self._click_embeddings_pre_pad = tf.nn.embedding_lookup(embeddings_dict, click_context_samples)
        self._num_non_pad = tf.reduce_sum(self._click_context_mask, reduction_indices=[1])
        self._num_non_pad_zero_mask = tf.cast(tf.equal(self._num_non_pad, 0), tf.float32)
        self.num_non_pad = self._num_non_pad + self._num_non_pad_zero_mask
        self.click_embeddings = tf.multiply(self._click_embeddings_pre_pad, self._click_context_mask)
        self.click_embeddings_sum = tf.reduce_sum(self.click_embeddings, reduction_indices=[1])
        self.click_embeddings_mean = self.click_embeddings_sum / self._num_non_pad



