import cPickle as pickle
import tensorflow as tf
import mind_palace.product_ranker.constants as CONST
import numpy as np

from mind_palace.product_ranker.models.modelconfig import modelconfig
from mind_palace.product_ranker.models.model import model
from mind_palace.product_ranker.models import model_factory as mf
from mind_palace.product_ranker.training import trainingcontext as tc
from mind_palace.product_ranker.training.trainingcontext import trainingcontext
from ProductAttributesDataset import integerized_attributes
from mind_palace.DictIntegerizer import DictIntegerizer
from operator import itemgetter


class Scorer :

    def __init__(self, model_path, nn_version, attributes_path):

        self.nn_version = nn_version
        dir = tc.getTraningContextDir(model_path)

        print "loading training context : " + dir
        with open(dir, 'rb') as handle:
            self.trainCxt = pickle.load(handle)

        self.trainCxt.model_dir = model_path

        attribute_dict_path = self.trainCxt.attributedict_path
        print "attribute dict path : " + attribute_dict_path
        with open(attribute_dict_path, 'rb') as handle:
            attribute_dict = pickle.load(handle)

        self.model_conf = self.trainCxt.model_config # type: modelconfig
        self.attributes = map(lambda x : x.name, self.model_conf.attributes_config)

        self.model_conf.enable_default_click = False

        self.mod = mf.get_model(self.model_conf) #type:model
        self.saver = tf.train.Saver()

        index_field = "productId"
        self.pid_dict = attribute_dict[index_field] #type: DictIntegerizer
        self.product_attributes = integerized_attributes(self.attributes, attribute_dict, attributes_path, index_field=index_field)
        self.missing_data_index = CONST.DEFAULT_DICT_KEYS.index(CONST.MISSING_DATA_TEXT)
        print "loaded model, ready to run now"

    def lookup_attributes(self, pids) :
        num_attributes = np.shape(self.product_attributes)[1]
        attributes = np.ones(shape=[len(pids), num_attributes], dtype=int) * self.missing_data_index
        for i in range(len(pids)) :
            pid = pids[i]
            product_index = self.pid_dict.only_get(pid)
            if product_index == self.missing_data_index :
                attributes[i] = np.ones(num_attributes, dtype=int) * self.missing_data_index
            else :
                attributes[i] = self.product_attributes[product_index]
        return attributes


    def score(self, products_to_rank, clicked_products):
        with tf.Session() as sess:
            # Restore variables from disk.
            self.saver.restore(sess, self.trainCxt.getNnDir(extension=self.nn_version))

            score = self.mod.score()
            feed_keys = self.mod.place_holders()

            ranking_attributes = self.lookup_attributes(products_to_rank)
            clicked_attributes = self.lookup_attributes(clicked_products)

            result = []
            for i in range(len(products_to_rank)) :
                pid = products_to_rank[i]
                feed_values = []
                for j in range(len(self.attributes)) :
                    feed_values += [[ranking_attributes[i][j]], [[]], [clicked_attributes[:, j]]]
                feed = dict(zip(feed_keys, feed_values))
                pid_score = sess.run(score, feed_dict = feed)
                result.append([pid, i, ranking_attributes[i][0], float(pid_score[0][0]), float(pid_score[1][0][0]), float(pid_score[2][0][0])])

            product_score = sorted(result, key=itemgetter(3), reverse=True)
            return product_score

    def print_score(self, products_to_rank, clicked_products):
        ps = self.score(products_to_rank, clicked_products)
        for p in ps :
            print p


if __name__ == '__main__' :
    model_path = "saved_models/run.20171018-18-53-05"
    attributes_path = "/home/thejus/workspace/learn-cascading/data/product-attributes.MOB/part-*"
    nn_version = 'counter-7000'
    rm = Scorer(model_path, nn_version, attributes_path)
    products_to_rank = ["MOBEQ98MNXHY4RU9", "MOBES9G5SJHYT9QX", "MOBEQ98TABTWXGTD", "MOBEWN63JHHEXPTD", "MOBEXNP9FJ9K5K53", "MOBEX9WXUSZVYHET", "MOBET6RH4XSXKM7D", "MOBEQ98TWG8X4HH3", "MOBECCA5FHQD43KA", "MOBEWN63NBDSMVPG", "MOBEU9WRGVXDPBSF", "MOBEU9WRZFFUYAXJ", "MOBEU9WRZHRVWXTK", "MOBEMK62PN2HU7EE", "MOBEX9WXZCZHWXUZ", "MOBEWXHUSBXVJ7NZ", "MOBET6RHXVZBJFNT", "MOBESDYMGHC37GCS", "MOBEN2YYKU9386TQ", "MOBEN2YYQH8PSYXG", "MOBECCA5Y5HBYR3Q", "MOBECCA5SMRSKCNY", "MOBEG4XWMBDGZVEX", "MOBEG4XWDK4WBGNU", "MOBEV7YDBCAFG3ZH", "MOBEN2XYK8WFEGM8", "MOBEJFHUFVAJ45YA", "MOBEJFHUGPWTZFQJ", "MOBEV7YD3CFBTENW", "MOBEVKFTCFFU2FE7", "MOBETM9FZWW5UEZG", "MOBEUF42PGDRYCQA", "MOBEUF424KXTP9CT", "MOBEUF42VHXZSQV7", "MOBEQ98T82CYVHGZ", "MOBETM93F7DGJNN5", "MOBETMH3ZYNDPVVC", "MOBEU35JUQMQQHWK", "MOBEU35JAZKVWRPV", "MOBESDYCQD3FJCFW", "MOBEZEMYH7FQBGBQ", "MOBEZENFZBPW8UMF", "MOBEKGT2HGDGADFW", "MOBEMK62JSRHU85T", "MOBEZPVEGADXHMHT", "MOBEZPVENHEVMQDZ", "MOBEQRYTXZXC8FZZ", "MOBETM93HMBGUQKH", "MOBEXHHKDHSA9UZC", "MOBECCA5BJUVUGNP", "MOBEHZTGXSGG2GRX", "MOBEK4ABQFH3SSP7", "MOBE9TGVE7ZBRAEN"]
    clicked_products = ["MOBETMH3ZYNDPVVC"]
    ps = rm.print_score(products_to_rank, clicked_products)
    print "---------------------------------------------------------"
    clicked_products = [CONST.DEFAULT_CLICK_TEXT]
    ps = rm.print_score(products_to_rank, clicked_products)






