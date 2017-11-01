import cPickle as pickle


def logBreak() :
    print "------------------------------------------"


def read_object(attribute_dict_path):
    with open(attribute_dict_path, 'rb') as handle:
        attribute_dict = pickle.load(handle)
    return attribute_dict

def write_object(attribute_dict_path, dicts):
    with open(attribute_dict_path, 'w+b') as handle:
        pickle.dump(dicts, handle, protocol=pickle.HIGHEST_PROTOCOL)