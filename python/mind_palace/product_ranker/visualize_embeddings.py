import cPickle as pickle
import os
import tensorflow as tf
from tensorflow.contrib.tensorboard.plugins import projector

from mind_palace.product_ranker.models.max_margin_model import max_margin_model

# # Use the same LOG_DIR where you stored your checkpoint.
# summary_writer = tf.train.SummaryWriter(LOG_DIR)
#
# # Format: tensorflow/contrib/tensorboard/plugins/projector/projector_config.proto
# config = projector.ProjectorConfig()
#
# # You can add multiple embeddings. Here we add only one.
# embedding = config.embeddings.add()
# embedding.tensor_name = embedding_var.name
# # Link this tensor to its metadata file (e.g. labels).
# embedding.metadata_path = os.path.join(LOG_DIR, 'metadata.tsv')
#
# # Saves a configuration file that TensorBoard will read during startup.
# projector.visualize_embeddings(summary_writer, config)


with open('saved_models/sessionsimple-productdict.pickle', 'rb') as handle:
    productdict = pickle.load(handle)

vocabulary_size = productdict.dictSize()
embedding_size = 50
md = max_margin_model(vocabulary_size, embedding_size)
saver = tf.train.Saver()

with tf.Session() as sess:
    # Restore variables from disk.
    modelVersion = "19-14400"
    saver.restore(sess, "./saved_models/sessionsimple." + modelVersion)
    LOG_DIR = "/tmp/embedding-visualization." + modelVersion
    saver.save(sess, os.path.join(LOG_DIR, "model.ckpt"), 0)
    summary_writer = tf.summary.FileWriter(LOG_DIR)

    # Format: tensorflow/contrib/tensorboard/plugins/projector/projector_config.proto
    config = projector.ProjectorConfig()

    # You can add multiple embeddings. Here we add only one.
    embedding = config.embeddings.add()
    embedding.tensor_name = md.embeddings_dict.name
    # Link this tensor to its metadata file (e.g. labels).
    embedding.metadata_path = os.path.join(LOG_DIR, 'metadata.tsv')

    # Saves a configuration file that TensorBoard will read during startup.
    projector.visualize_embeddings(summary_writer, config)
    print "done"