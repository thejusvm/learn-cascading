from mind_palace.product_ranker.modelconfig import modelconfig


class model :

    def __init__(self, model_config) :
        self.model_config = model_config # type: modelconfig

    def poistive_label(self):
        """ :return the placeholder to attach the positive label (into feed dict)"""
        pass

    def negative_label(self):
        """ :return the placeholder to attach the negative labels (into feed dict)"""
        pass

    def click_product_label(self):
        """ :return the placeholder to attach the click product label (into feed dict)"""
        pass

    def loss(self):
        """ :return a tensor representing the loss of the fitting problem """
        pass

    def minimize_step(self):
        """ :return a tensor representing the gradient descent train step, which needs to be iterated on """
        pass

    def score(self, products, click_context):
        """ :return score for the product under the click_context"""
        pass

    def test_summaries(self):
        """ :return an nx2 array with first column as the summary name and second column as the tensor for the summary"""
        pass
