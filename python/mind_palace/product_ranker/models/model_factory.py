import importlib

def get_model(modelconf) :
    class_name = modelconf.model_name
    class_ = getattr(importlib.import_module("mind_palace.product_ranker.models." + class_name), class_name)
    print "Using Model : " + str(class_)
    return class_(modelconf)



