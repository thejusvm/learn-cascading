import cPickle as pickle
import os
import sys
import tensorflow as tf
from tensorflow.contrib.tensorboard.plugins import projector
from mind_palace.product_ranker.training import trainingcontext as tc
from mind_palace.product_ranker.models import model_factory as mf
from mind_palace.product_ranker.models import softmax_model
from mind_palace.DictIntegerizer import DictIntegerizer



model_path = sys.argv[1]#"saved_models/run.20171103-15-28-55"
dir = tc.getTraningContextDir(model_path)

print "loading training context : " + dir
with open(dir, 'rb') as handle:
    trainCxt = pickle.load(handle) #type:tc.trainingcontext

trainCxt.model_dir = model_path

attribute_dict_path = trainCxt.attributedict_path
print "attribute dict path : " + attribute_dict_path
with open(attribute_dict_path, 'rb') as handle:
    attribute_dict = pickle.load(handle)

md = mf.get_model(trainCxt.model_config) #type:softmax_model
saver = tf.train.Saver()


def generate_metadata(idict, path, field_name):
    """
    :type idict: DictIntegerizer
    :type path: str
    """
    lst = [None] * idict.currentCount
    term_to_int = idict.getDict()
    for term in term_to_int:
        lst[term_to_int[term]] = term.upper()

    with open(path, mode="w+b") as writer :
        # writer.write(field_name + '\n')
        for l in lst :
            writer.write(l + '\n')
        writer.flush()


with tf.Session() as sess:
    # Restore variables from disk.
    nn_dir = tf.train.latest_checkpoint(trainCxt.model_dir)
    saver.restore(sess, nn_dir)
    LOG_DIR = "/tmp/embedding-visualization." + trainCxt.date
    saver.save(sess, os.path.join(LOG_DIR, "model.ckpt"), 0)
    summary_writer = tf.summary.FileWriter(LOG_DIR)

    # Format: tensorflow/contrib/tensorboard/plugins/projector/projector_config.proto
    config = projector.ProjectorConfig()

    # You can add multiple embeddings. Here we add only one.
    embedding = config.embeddings.add()
    per_attribute_embeddings = md.per_attribute_embeddings[0]
    embedding.tensor_name = per_attribute_embeddings.context_dict.name
    # Link this tensor to its metadata file (e.g. labels).
    embedding.metadata_path = os.path.join(LOG_DIR, 'metadata.tsv')
    attribute_name_ = per_attribute_embeddings.attribute_name
    generate_metadata(attribute_dict[attribute_name_], embedding.metadata_path, attribute_name_)

    # Saves a configuration file that TensorBoard will read during startup.
    projector.visualize_embeddings(summary_writer, config)
    print "done"