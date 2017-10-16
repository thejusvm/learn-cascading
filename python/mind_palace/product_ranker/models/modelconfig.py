import collections

EmbeddingDicts = collections.namedtuple('EmbeddingDicts', 'softmax_weights softmax_bias context_dict')
empty_embeddingdict = EmbeddingDicts(softmax_weights=None, softmax_bias=None, context_dict=None)

class modelconfig :

    def __init__(self, model_name):
        self.model_name = model_name

        self.attributes_config = None
        self.enable_default_click = True
        self.pad_index = 0
        self.use_context = True
        self.layer_count = [256]
        self.reuse_context_dict = False

class AttributeConfig :

    def __init__(self, name, embedding_size, vocab_size = 0, override_embeddings = empty_embeddingdict):
        self.name = name
        self.embedding_size = embedding_size
        self.vocab_size = vocab_size
        self.override_embeddings = override_embeddings