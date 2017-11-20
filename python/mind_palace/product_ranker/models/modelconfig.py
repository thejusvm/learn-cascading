import collections

EmbeddingDicts = collections.namedtuple('EmbeddingDicts', 'softmax_weights softmax_bias context_dict')
empty_embeddingdict = EmbeddingDicts(softmax_weights=None, softmax_bias=None, context_dict=None)

class modelconfig :

    def __init__(self, model_name):
        self.model_name = model_name

        self.attributes_config = None
        self.enable_default_click = False
        self.pad_index = 0
        self.use_context = True
        self.click_non_linearity = False
        self.click_layer_count = []
        self.reuse_context_dict = False
        self.learning_rate = 1e-3

        self.probability_function = "sigmoid" #values: sigmoid/nn
        self.layer_count = []

class AttributeConfig :

    def __init__(self, name, embedding_size, vocab_size = 0, override_embeddings = empty_embeddingdict, per_attribute_learning_rate = None):
        self.name = name
        self.embedding_size = embedding_size
        self.vocab_size = vocab_size
        self.override_embeddings = override_embeddings
        self.per_attribute_learning_rate = per_attribute_learning_rate

    def __str__(self):
        return self.name + ":" + str(self.embedding_size)

    def __repr__(self):
        return self.__str__()

def parse_attribute_config(conf_str):
    confs = conf_str.split(':')
    return AttributeConfig(confs[0], int(confs[1]))
