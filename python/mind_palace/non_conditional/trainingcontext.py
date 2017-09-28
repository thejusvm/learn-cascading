
class trainingcontext :

    def __init__(self):
        self.data_path = "/home/thejus/workspace/learn-cascading/data/sessionExplode-201708.MOB"
        self.batch_size = 500
        self.num_epochs = 20
        self.min_click_context = 0
        self.test_size = 0.2
        self.embedding_size = 10
        self.pad_text = "<pad>"
        self.use_context = True
        self.init_pad_to_zeros = True
        self.summary_dir = "/tmp/sessionsimple"
        self.model_dir = "saved_models/"
        self.test_summary_publish_iters = 100
        self.save_model_on_epoch = False
        self.save_model_num_iter =  None
        self.save_model = False



