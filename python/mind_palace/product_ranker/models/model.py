from mind_palace.product_ranker.models.modelconfig import modelconfig


class model :

    def __init__(self, model_config) :
        self.model_config = model_config # type: modelconfig

    def feed_input(self, feature_names, inputs):
        pass

    def place_holders(self):
        """:return the list of placeholders in attach data to"""
        pass

    def loss(self):
        """ :return a tensor representing the loss of the fitting problem """
        pass

    def minimize_step(self):
        """ :return a tensor representing the gradient descent train step, which needs to be iterated on """
        pass

    def score(self):
        """ :return score for the product under the click_context"""
        pass

    def test_summaries(self):
        """ :return an nx2 array with first column as the summary name and second column as the tensor for the summary"""
        pass
