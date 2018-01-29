import cPickle as pickle
import os
import sys
import tensorflow as tf
import mind_palace.product_ranker.constants as CONST
from tensorflow.contrib.tensorboard.plugins import projector
from mind_palace.product_ranker.training import trainingcontext as tc
from mind_palace.product_ranker.models import model_factory as mf
from mind_palace.product_ranker.models import softmax_model
from mind_palace.DictIntegerizer import DictIntegerizer

os.environ["CUDA_VISIBLE_DEVICES"]="-1"

def generate_metadata(attribute_dict_path, metadata_path, attribute_name):
    with open(metadata_path, mode="w+b") as writer :
        with open(attribute_dict_path, mode="r") as reader:
            # reader.readline()
            writer.write(reader.read())


model_path = sys.argv[1] #"saved_models/run.20171103-15-28-55"
attribute_name = sys.argv[2]
intProductPath = sys.argv[3]

dir = tc.getTraningContextDir(model_path)

print "loading training context : " + dir
with open(dir, 'rb') as handle:
    trainCxt = pickle.load(handle) #type:tc.trainingcontext

trainCxt.model_dir = model_path

md = mf.get_model(trainCxt.model_config) #type: softmax_model
saver = tf.train.Saver()


with tf.Session() as sess:
    # Restore variables from disk.
    nn_dir = tf.train.latest_checkpoint(trainCxt.model_dir)
    saver.restore(sess, nn_dir)
    LOG_DIR = "/tmp/embedding-visualization/vis." + trainCxt.date
    saver.save(sess, os.path.join(LOG_DIR, "model.ckpt"), 0)
    summary_writer = tf.summary.FileWriter(LOG_DIR)

    # Format: tensorflow/contrib/tensorboard/plugins/projector/projector_config.proto
    config = projector.ProjectorConfig()

    # You can add multiple embeddings. Here we add only one.
    embedding = config.embeddings.add()
    attribute_index = [ac.attribute_name for ac in md.ranking_attributes_embeddingsrepo].index(attribute_name)
    per_attribute_embeddings = md.ranking_attributes_embeddingsrepo[attribute_index]
    embedding.tensor_name = per_attribute_embeddings.context_dict.name
    # Link this tensor to its metadata file (e.g. labels).
    embedding.metadata_path = os.path.join(LOG_DIR, 'metadata.tsv')

    attribute_dict_path = intProductPath+"/attribute_dicts/"+attribute_name+".dict"
    generate_metadata(attribute_dict_path, embedding.metadata_path, attribute_name)

    # Saves a configuration file that TensorBoard will read during startup.
    projector.visualize_embeddings(summary_writer, config)
    print "done"