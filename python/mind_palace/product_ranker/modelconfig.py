
class modelconfig :

    def __init__(self, model_name, vocab_size, embedding_size):
        self.model_name = model_name
        self.vocabulary_size = vocab_size
        self.embedding_size = embedding_size
        self.init_embedding_dict = None

        self.pad_index = 0
        self.use_context = True
        self.layer_count = [256]