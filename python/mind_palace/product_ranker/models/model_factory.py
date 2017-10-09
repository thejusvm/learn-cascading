
import importlib

def get_model(modelconf) :
    class_name = modelconf.model_name
    class_ = getattr(importlib.import_module("mind_palace.product_ranker.models." + class_name), class_name)
    print class_
    # module = __import__(." + class_name)
    # class_ = getattr(module, class_name)
    return class_(modelconf)



