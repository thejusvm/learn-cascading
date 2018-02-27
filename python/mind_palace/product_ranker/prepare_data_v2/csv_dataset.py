import glob
import tensorflow as tf

import mind_palace.product_ranker.constants as CONST
from mind_palace.product_ranker.commons import generate_feature_names


def dense_split(tensor, delimiter="\t"):
    tensor = tf.expand_dims(tensor, 0)
    tensor = tf.string_split(tensor, delimiter=delimiter)
    tensor = tf.sparse_tensor_to_dense(tensor, default_value="")
    tensor = tf.squeeze(tensor, axis=0)
    return tensor

def _parse_feature(feature_tensor) :
    feature_tensor = dense_split(feature_tensor, ",")
    feature_tensor = tf.string_to_number(feature_tensor, out_type=tf.int64)
    return feature_tensor

def _parse_function(feature_indices, row):
    row_split = dense_split(row)
    result = [row_split[i] for i in feature_indices]
    return tuple([_parse_feature(x) for x in result])


class CSV_ClickstreamDataset :

    def __init__(self, attributes, ctr_data_path, shuffle=True, batch_size=None, num_threads=6):
        self.ctr_data_path = ctr_data_path
        self.filenames = tf.placeholder(tf.string, shape=[None])
        self.dataset = tf.contrib.data.Dataset.from_tensor_slices(self.filenames)
        self.dataset = self.dataset.flat_map(
            lambda filename: (
                tf.contrib.data.TextLineDataset(filename).skip(1)))
        self.feature_names = generate_feature_names(attributes, CONST.TRAINING_COL_PREFIXES)

        column_names = get_column_names(ctr_data_path)
        feature_indices = [column_names.index(feature_name) for feature_name in self.feature_names]
        self.dataset = self.dataset.map(lambda row: _parse_function(feature_indices, row),
                                        num_threads=num_threads, output_buffer_size = 100 * (batch_size if batch_size is not None else 1))

        if shuffle :
            self.dataset = self.dataset.shuffle(buffer_size=100000)
        if batch_size != None :
            padding_value = CONST.DEFAULT_DICT_KEYS.index(CONST.PAD_TEXT)
            num_features = len(self.feature_names)
            padding_values = tuple([tf.constant(padding_value, dtype=tf.int64) for _ in range(num_features)])
            padded_shapes = tuple([[None] for _ in range(num_features)])
            self.dataset = self.dataset.padded_batch(batch_size, padded_shapes = padded_shapes, padding_values=padding_values)

        self.iterator = self.dataset.make_initializable_iterator()
        self.get_next = self.iterator.get_next()

    def initialize_iterator(self, sess):
        sess.run(self.iterator.initializer, feed_dict={self.filenames : self.ctr_data_path})

def get_column_names(file_list) :
    with open(file_list[0]) as fh:
        column_names = fh.readline().rstrip('\n').split("\t")
    return column_names


if __name__ == '__main__' :

    path = glob.glob("/Users/thejus/workspace/learn-cascading/data/sessions-2017100.split-part/train/part-test")
    sess = tf.Session()
    attributes = ["productId", "brand", "vertical"]
    # attributes = ["productId"]

    column_names = get_column_names(path)
    dataset = CSV_ClickstreamDataset(attributes, column_names, batch_size=None, shuffle=False)
    dataset.initialize_iterator(sess, path)
    get_next = dataset.get_next
    print sess.run(get_next)
    print sess.run(get_next)
