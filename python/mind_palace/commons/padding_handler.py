import tensorflow as tf


class padding_handler :
    """When a tensor is padded with dummy padding (to convert variable length to fixed length tensor),
    the padded string shouldn't contribute to the loss and gradients shouldn't be propagated into the padded string.
    padding_handler achieves this by identifing all the padded tokens and multiplying the embeddings of the pad by 0
    :arg
        padded_tensor : a 2-dim tensor, with padded tokens
        embeddings_dict : embeddings_dict to lookup the embeddings for tokens
        padding_index : index of the padded token
    :var
        tensor_embeddings : embeddings tensor of the padded_tensor (by looking up into the dict)
                            and paddings replaced by 0
        tensor_embeddings_sum : sum of tensor_embeddings along the first dimension
        tensor_embedding_mean : mean of the tensor_embeddings ignoring the padded tokens
        """

    def __init__(self, padded_tensor, embeddings_dict, padding_index = 0):
        self.padded_tensor = padded_tensor
        self._padding_mask = tf.not_equal(self.padded_tensor, padding_index)
        self._padding_mask = tf.cast(self._padding_mask, tf.float32)
        self._padding_mask = tf.expand_dims(self._padding_mask, 2)
        self._click_embeddings_pre_pad = tf.nn.embedding_lookup(embeddings_dict, self.padded_tensor)

        # num_non_pad is needed to compute the mean embedding, padded tokens should not contribute to the denominator of the mean
        self._num_non_pad = tf.reduce_sum(self._padding_mask, reduction_indices=[1])
        # when the entire padded_tensor is made of paddings the num_non_pad will be 0,
        # mean computation breaks in such cases, since num_non_pad is used in the denominator
        # Handling for the case to make num_non_pad 1 when num_non_pad is zero
        self._num_non_pad_zero_mask = tf.cast(tf.equal(self._num_non_pad, 0), tf.float32)
        self._num_non_pad = self._num_non_pad + self._num_non_pad_zero_mask

        self.tensor_embeddings = tf.multiply(self._click_embeddings_pre_pad, self._padding_mask)
        self.tensor_embeddings_sum = tf.reduce_sum(self.tensor_embeddings, reduction_indices=[1])
        self.tensor_embeddings_mean = self.tensor_embeddings_sum / self._num_non_pad
        self.num_non_pad = self._num_non_pad



