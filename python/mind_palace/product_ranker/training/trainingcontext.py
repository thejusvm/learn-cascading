import tensorflow as tf
import glob

train_context_pickle = '/train_context.pickle'

def getTraningContextDir(model_dir) :
    return model_dir + train_context_pickle

class trainingcontext :

    def __init__(self):
        self.data_path = None
        self.product_attributes_path = None
        self.batch_size = 500
        self.num_epochs = 20
        self.min_click_context = 0 # Minimum number of context click to consider it for training
        self.summary_dir = "/tmp/sessionsimple"
        self.model_dir = "saved_models/"
        self.test_summary_publish_iters = 100
        self.save_model_on_epoch = False
        self.save_model_num_iter =  None
        self.save_model = False
        self.date = None
        self.timestamp = None
        self.publish_summary = True
        self.num_negative_samples = 20
        self.num_click_context = 32
        self.model_config = None
        self.test_size = 0.2
        self.negative_samples_source = 'random' #possible values : random/productAttributes
        self.restore_model_dir = None
        self.train_counter = 0
        self.latency_track_iters = 10000
        self.batch_size = 500


        #train_v2 only args (file train_v2.py)
        self.attributedict_path = None
        self.train_path = None
        self.test_path = None
        self.max_test_size = 10000000

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




