import tensorflow as tf
import glob
import time

train_context_pickle = '/train_context.pickle'

def getTraningContextDir(model_dir) :
    return model_dir + train_context_pickle

class trainingcontext :

    def __init__(self):
        self.input_path = ""
        # self.data_path = ""
        self.product_attributes_path = ""
        self.batch_size = 500
        self.num_epochs = 25
        # self.min_click_context = 0 # Minimum number of context click to consider it for training
        self.summary_dir = "/tmp/sessionsimple"
        self.model_dir = "saved_models/"
        self.test_summary_publish_iters = 5000
        self.save_model_on_epoch = False
        self.save_model_num_iter =  5000
        self.save_model = True
        self.timestamp = time.localtime()
        self.date = time.strftime('%Y%m%d-%H-%M-%S', self.timestamp)
        self.publish_summary = True
        # self.num_negative_samples = 20
        self.num_click_context = 32
        self.model_config = None
        self.test_size = 0.03
        self.restore_model_path = ""
        self.train_counter = 0
        self.latency_track_iters = 10000
        self.batch_size = 500


        #train_v2 only args (file train_v2.py)
        self.attributedict_path = ""
        self.train_path = ""
        self.columns_in_data = ""
        self.test_path = ""

        self.input_type = "csv" #csv/tfr

        self.attribute_summary_path = ""

    def getTrainCxtDir(self) :
        return self.model_dir + train_context_pickle

    # def getProductDictDir(self) :
    #     return self.model_dir + '/productdict.pickle'

    def getNnDir(self, extension = None) :
        if extension is None :
            nndir = self.model_dir + "/nn"
            return nndir
        else :
            return self.model_dir + "/nn." + extension




