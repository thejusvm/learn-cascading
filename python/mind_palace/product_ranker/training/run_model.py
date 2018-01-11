import cPickle as pickle
import collections
import numpy as np
import tensorflow as tf
import os
import sys
from operator import itemgetter

import mind_palace.product_ranker.constants as CONST
from mind_palace.DictIntegerizer import DictIntegerizer
from mind_palace.product_ranker.models import model_factory as mf
from mind_palace.product_ranker.models.model import model
from mind_palace.product_ranker.models.modelconfig import modelconfig
from mind_palace.product_ranker.prepare_data.product_attributes_dataset import read_integerized_attributes
from mind_palace.product_ranker.training import trainingcontext as tc
from mind_palace.product_ranker.commons import generate_feature_names

def read_attribute_dict(path):

    with open(path, 'rb') as handle:
        attribute = handle.readline()
        attribute.rstrip('\n')
        di = DictIntegerizer(name=attribute)
        while True :
            line = handle.readline()
            line = line.rstrip('\n')
            if not line:
                break
            else:
                di.get(line)
        return di


def read_attribute_dicts(path, attributes):
    attribute_dicts = {}
    for attribute in attributes :
        attribute_path = path + "/" + attribute + ".dict"
        attribute_dicts[attribute] = read_attribute_dict(attribute_path)
    return attribute_dicts

# di = read_attribute_dicts("/Users/thejus/workspace/learn-cascading/data/sessions-2017100.products-int.1/attribute_dicts", ["color"])
# print di["color"].termdict
# sys.exit()

class Scorer :

    def __init__(self, model_path, product_data_path):

        self.model_path = model_path
        dir = tc.getTraningContextDir(model_path)

        print "loading training context : " + dir
        with open(dir, 'rb') as handle:
            self.trainCxt = pickle.load(handle)

        self.trainCxt.model_dir = model_path
        self.model_conf = self.trainCxt.model_config  # type: modelconfig
        self.attributes = map(lambda x: x.name, self.model_conf.attributes_config)

        attribute_dict_path = product_data_path + "/attribute_dicts"
        product_attributes_path = product_data_path + "/integerized_attributes/part-*"

        attribute_dict = read_attribute_dicts(attribute_dict_path, self.attributes)


        self.model_conf.enable_default_click = False

        self.mod = mf.get_model(self.model_conf) #type:model

        index_field = "productId"
        self.pid_dict = attribute_dict[index_field] #type: DictIntegerizer
        self.product_attributes = read_integerized_attributes(self.attributes, product_attributes_path, index_field)
        self.missing_data_index = CONST.DEFAULT_DICT_KEYS.index(CONST.MISSING_DATA_TEXT)
        print "loaded model, ready to run now"

    def lookup_attributes(self, pids) :
        num_attributes = np.shape(self.product_attributes)[1]
        attributes = np.ones(shape=[len(pids), num_attributes], dtype=int) * self.missing_data_index
        for i in range(len(pids)) :
            pid = pids[i]
            product_index = self.pid_dict.only_get(pid, missing_val=self.missing_data_index)
            if product_index == self.missing_data_index :
                attributes[i] = np.ones(num_attributes, dtype=int) * self.missing_data_index
            else :
                attributes[i] = self.product_attributes[product_index]
        return attributes


    def score(self, products_to_rank, clicked_products, nn_version=None):
        with tf.Session() as sess:
            # Restore variables from disk.
            if nn_version is None :
                nn_dir = tf.train.latest_checkpoint(self.trainCxt.model_dir)
            else :
                nn_dir = self.trainCxt.getNnDir(extension=nn_version)

            if not clicked_products :
                if self.model_conf.enable_default_click :
                    clicked_products = [CONST.DEFAULT_CLICK_TEXT]
                else :
                    clicked_products = [CONST.PAD_TEXT]

            ranking_attributes = self.lookup_attributes(products_to_rank)
            clicked_attributes = self.lookup_attributes(clicked_products)

            result = []
            num_attributes = len(self.attributes)
            num_feed = len(self.attributes) * 3
            feed_values = [[] for i in range(num_feed)]
            num_products = len(products_to_rank)
            for i in range(num_products) :
                pid = products_to_rank[i]
                feed_values_row = []
                for j in range(num_attributes) :
                    feed_values_row += [[ranking_attributes[i][j]], [], clicked_attributes[:, j]]
                for j in range(num_feed) :
                    feed_values[j].append(feed_values_row[j])
            feature_names = generate_feature_names(self.attributes, feature_prefixes=CONST.TRAINING_COL_PREFIXES)
            feed_values = [np.array(feed_values[j], dtype=int) for j in range(num_feed)]
            self.mod.feed_input(feature_names, feed_values)
            score = self.mod.score()
            score_names = [x[0] for x in score]
            score_ops = [x[1] for x in score]

            print "restoring tf model from : " + nn_dir
            saver = tf.train.Saver()
            saver.restore(sess, nn_dir)

            pid_score = sess.run(score_ops)
            for i in range(num_products) :
                pid = products_to_rank[i]
                scores = [float(pid_score[j][i]) for j in range(len(score_names))]
                result.append([pid, i] + scores)

            response_keys = ['product_id', 'original_rank'] + score_names
            product_score = sorted(result, key=itemgetter(2), reverse=True)
            product_score = map(lambda x : collections.OrderedDict(zip(response_keys, x)), product_score)
            return product_score

    def print_score(self, products_to_rank, clicked_products):
        ps = self.score(products_to_rank, clicked_products)
        for p in ps :
            print p


if __name__ == '__main__' :
    model_path = "saved_models/run.20180110-20-06-26"
    product_data_path = "/Users/thejus/workspace/learn-cascading/data/sessions-2017100.products-int.1"
    rm = Scorer(model_path, product_data_path)
    products_to_rank = ["saresnf8hcpxctwx","sarephfghwhth2dj","saretpv3rgd7hswt","sarecj42hrbgf4mg","saresn4gwdg4rhr6","sarecgrgvdkyeyt2","sarewngdwhgd2hs8","sarezr5dweha7ryy","saremxhcbmbgbsqg","saredzuf2yjcwjp7"]
    clicked_products = ["sarexdfd7rgnhexg"]
    ps = rm.print_score(products_to_rank, clicked_products)
    print "---------------------------------------------------------"
    # clicked_products = [CONST.DEFAULT_CLICK_TEXT]
    # ps = rm.print_score(products_to_rank, clicked_products)






