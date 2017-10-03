
train_context_pickle = '/train_context.pickle'

def getTraningContextDir(model_dir) :
    return model_dir + train_context_pickle


class trainingcontext :

    def __init__(self):
        self.data_path = "/home/thejus/workspace/learn-cascading/data/sessionExplode-201708.MOB"
        self.batch_size = 500
        self.num_epochs = 20
        self.min_click_context = 0
        self.test_size = 0.2
        self.pad_text = "<pad>"
        self.init_pad_to_zeros = True
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

    def getTrainCxtDir(self) :
        return self.model_dir + train_context_pickle

    def getProductDictDir(self) :
        return self.model_dir + '/productdict.pickle'

    def getNnDir(self, extension = None) :
        if extension is None :
            return self.model_dir + '/nn'
        else :
            return self.model_dir + "/nn." + extension




