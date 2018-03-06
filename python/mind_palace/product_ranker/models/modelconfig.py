import collections

EmbeddingDicts = collections.namedtuple('EmbeddingDicts', 'softmax_weights softmax_bias context_dict')
empty_embeddingdict = EmbeddingDicts(softmax_weights=None, softmax_bias=None, context_dict=None)

class modelconfig :

    def __init__(self, model_name):
        self.model_name = model_name
        self.attributes_config = None
        self.attribute_regularizer_id = None
        self.attribute_regularizer_weight = 1
        self.enable_default_click = False
        self.pad_index = 0
        self.use_context = True
        self.click_non_linearity = False
        self.click_layer_count = []
        self.reuse_context_dict = False
        self.learning_rate = 1e-3

        self.head_tail_id = "productId"
        self.head_tail_split = 10000

        self.click_pooling = "mean" #values : mean/sum

        self.probability_function = "sigmoid" #values: sigmoid/nn
        self.layer_count = []

        self.positive_col_prefix = "positive"
        self.negative_col_prefix = "negative_with_random"
        self.click_col_prefix = "clicked_short"
        self.test_negative_col_prefix = "negative"


class AttributeConfig :

    def __init__(self, name, embedding_size, vocab_size=0,
                 for_ranking=True,
                 for_regularization=False,
                 override_embeddings=empty_embeddingdict,
                 per_attribute_learning_rate=None):
        self.name = name
        self.embedding_size = embedding_size
        self.vocab_size = vocab_size
        self.for_ranking = for_ranking
        self.for_regularization = for_regularization
        self.override_embeddings = override_embeddings
        self.per_attribute_learning_rate = per_attribute_learning_rate

    def __str__(self):
        return self.name + ":" + str(self.embedding_size) + ":ranking-" + str(self.for_ranking) + ":regularization-" + str(self.for_regularization)

    def __repr__(self):
        return self.__str__()

def parse_attribute_config(conf_str):
    confs = conf_str.split(':')
    return AttributeConfig(confs[0], int(confs[1]))
